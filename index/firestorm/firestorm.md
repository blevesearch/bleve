# Firestorm

A new indexing scheme for Bleve.

## Background

### Goals

- Avoid a single writer that must pause writing to perform computation
    - either by allowing multiple writers, if computation cannot be avoided
    - or by having a single writer which can insert rows uninterrupted
- Avoid the need for a back index
    - the back index is expensive from a space perspective
    - by not writing it out, we should be able to obtain a higher indexing throughput
    - consulting the backindex is one of the read/think/update cycles mentioned above

### Considerations
- The cost for not maintaining a back index is paid in two places
    - Searches may need to read more rows, because old/deleted rows may still exist
    - These rows can be excluded, so correctness is not affected, but they will be slower
    - Old/Deleted rows need to be cleaned up at some point
        - This could either be through an explicit cleanup thread, the job of which is to constantly walk the kvstore looking for rows to delete
        - Or, it could be integrated with a KV stores natural merge/compaction process (aka RocksDB)

### Semantics

It is helpful to review the desired semantics between the Index/Delete operations and Term Searches.

#### Index(doc_id, doc)

- Empty Index
- Term Search for "cat" = empty result set

The Index operation should update the index such that after the operation returns, a matching search would return the document.

- Index("a", "small cat")
- Term Search for "cat" = {"a"}

Calling the Index operation again for the same doc_id should update the index such that after the operation returns, only searches matching the newest version return the document.

- Index("a", "big dog")
- Term Search for "cat" = empty result set
- Term Search for "dog" = {"a"}

NOTE:

- At no point during the second index operation would concurrent searches for "cat" and "dog" both return 0 results.
- At no point during the second index operation would concurrent searches for "cat" and "dog" both return 1 result.

#### Delete(doc_id)

- Index("a", "small cat")
- Term Search for "cat" = {"a"}
- Delete("a")
- Term Search for "cat" = empty result set

Once the Delete operation returns, the document should no longer be returned by any search.

## Details

### Terminology

Document ID (`doc_id`)
:The user specified identifier (utf8 string).  This never changes for a document.

Document Number (`doc_number`)
:The Bleve internal identifier (uint64).  These numbers are generated from an atomic counter.

DocIdNumber
: Concatenation of `<doc_id> 0xff <doc_number>`

### Theory of Operation

By including a new unique identifier as a part of every row generated, the index operation no longer concerns itself with updating existing values or deleting previous values.

Removal of old rows is handled indepenently by separate threads.

Ensuring of correct semantics with respect to added/updated/deleted documents is maintained through synchronized in-memory data structures, to compensate for the decoupling of these other operations.

The Dictionary becomes a best effort data element.  In kill-9 scenarios it could become incorrect, but it is believed that this will generally only affect scoring not correctness, and we can pursue read-repair operations.

### Index State

The following pseudo-structure will be used to explain changes to the internal state.  Keep in mind the datatypes shown represent the logical structure required for correct behavior.  The actual implementation may be different to achieve performance goals.

    indexState {
        docCount uint64
        fieldCache map[string]uint16
        nextDocNumber uint64
        docIdNumberMutex sync.RWMutex // for protecting fields below
        maxReadDocNumber uint64
        inFlightDocIds map[string]uint64
        deletedDocIdNumbers [][]byte
    }

### Operation

#### Creating New Index

- New KV Batch
- SET VersionRow{version=X}
- SET FieldRow{field_id=0 field_name="_id"}
- Execute Batch
- Index State intialized to:

        {
            docCount = 0
            fieldCache = {
                "_id": 0
            }
            nextDocNumber = 1
            maxReadDocNumber = 0
            inFlightDocIds = {}
            deletedDocIdNumbers = {}
        }

- Garbage Collector Thread is started
- Old Doc Number Lookup Thread is started
- Index marked open

#### Opening an Existing Index

- GET VersionRow, assert current version or exit
- ITERATE all FieldRows{}
- ITERATE all TermFrequencyRow{ where field_id = 0 }
  - Identify consecutive rows with same doc_id but different doc_number
  - Lower document numbers are added to the deletedDocIdNumbers list
  - Count all non-duplicate rows, seed the docCount
  - Observe highest document number seen, seed nextDocNumber

- Index State intialized to:

        {
            docCount = <as counted above>
            fieldCache = {
                "_id": 0
                <as scanned above>
            }
            nextDocNumber = <as scanned above> + 1
            maxReadDocNumber = <same as nextDocNumber>
            inFlightDocIds = {}
            deletedDocIdNumbers = {<as scanned above>}
        }

- Garbage Collector Thread is started
- Old Doc Number Lookup Thread is started
- Index marked open

#### Garbage Collector Thread

The role of the Garbage Collector thread is to clean up rows referring to document numbers that are no longer relevant (document was deleted or updated).

Currently, only two types of rows include document numbers:
- Term Frequency Rows
- Stored Rows

The current thought is that the garbage collector thread will use a single iterator to iterate the following key spaces:

- TermFrequencyRow { where field_id > 0}
- StoredRow {all}

For any row refering to a document number on the deletedDocNumbers list, that key will be DELETED.

The garbage collector will track loop iterations or start key for each deletedDocNumber so that it knows when it has walked a full circle for a given doc number.  At point the following happen in order:

- docNumber is removed from the deletecDocNumbers list
- DELETE is issued on TermFreqRow{ field_id=0, term=doc_id, doc_id=doc_id_number }

The last thing we do is delete the TermFreqRow for field 0.  If anything crashes at any point prior to this, we will again read this record on our next warmup and that doc_id_number will again go through the garbage collection process.

#### Old Doc Number Lookup Thread

The role of the Old Doc Number Lookup thread is to asynchronously lookup old document numbers in use for a give document id.

Waits in a select loop reading from a channel.  Through this channel it is notified of a doc_id where work is to be done.  When a doc_id comes in, the following is performed:

- Acquire indexState.docIdNumberMutex for reading:
- Read maxReadDocNumber
- Find doc_id/doc_number k/v pair in the inFlightDocIds map
- Release indexState.docIdNumberMutex
- Start Iterator at TermFrequency{ field_id=0 term=doc_id}
- Iterator until term != doc_id

All doc_numbers found that are less than maxReadDocNumber and != doc_number in the inFlightDocIds map are now scheduled for deletion.

- Acquire indexState.docIdNumberMutex for writing:
- add doc numbers to deletedDocIdNumbers
- check if doc_number in inFlightDocIds is still the same
  - if so delete it
  - if not, it was updated again, so we must leave it
- Release indexState.docIdNumberMutex

Notify Garbage Collector Thread directly of new doc_numbers.

#### Term Dictionary Updater Thread

The role of the Term Dictionary Updater thread is to asynchronously perform best-effort updates to the Term Dictionary.  Note the contents of the Term Dictionary only affect scoring, and not correctness of query results.

NOTE: one case where correctness could be affected is if the dictionary is completely missing a term which has non-zero usage.  Since the garbage collector thread is continually looking at these rows, its help could be enlisted to detect/repair this situation.

It is notified via a channel of increased term usage (by index ops) and of decresed term usage (by garbage collector cleaing up old usage)

#### Indexing a Document

- Perform all analysis on the document.
- new_doc_number = indexState.nextDocNumber++
- Create New Batch
- Batch will contain SET operations for:
    - any new Fields
    - Term Frequency Rows for indexed fields terms
    - Stored Rows for stored fields
- Execute Batch
- Acquire indexState.docIdNumberMutex for writing:
- set maxReadDocNumber new_doc_number
- set inFlightDocIds{ docId = new_doc_number }
- Release indexState.docIdNumberMutex
- Notify Term Frequency Updater thread of increased term usage.
- Notify Old Doc Number Lookup Thread of doc_id.

The key property is that a search matching the updated document *SHOULD* return the document once this method returns.  If the document was an update, it should return the previous document until this method returns.  There should be no period of time where neither document matches.

#### Deleting a Document

- Acquire indexState.docIdNumberMutex for writing:
- set inFlightDocIds{ docId = 0 } // 0 is a doc number we never use, indicates pending deltion of docId
- Release indexState.docIdNumberMutex
- Notify Old Doc Number Lookup Thread of doc_id.

#### Batch Operations

Batch operations look largely just like the indexing/deleting operations.  Two other optimizations come into play.

- More SET operations in the underlying batch
- Larger aggregated updates can be passed to the Term Frequency Updater Thread

#### Term Field Iteration

- Acquire indexState.docIdNumberMutex for reading:
- Get copy of: (it is assumed some COW data structure is used, or MVCC is accomodated in some way by the impl)
    - maxReadDocNumber
    - inFlightDocIds
    - deletedDocIdNumbers
- Release indexState.docIdNumberMutex

Term Field Iteration is used by the basic term search.  It produces the set of documents (and related info like term vectors) which used the specified term in the specified field.

Iterator starts at key:

```'t' <field id uint16> <term utf8> 0xff```

Iterator ends when the term does not match.

- Any row with doc_number > maxReadDocNumber MUST be ignored.
- Any row with doc_id_number on the deletedDocIdNumber list MUST be ignored.
- Any row with the same doc_id as an entry in the inFlightDocIds map, MUST have the same number.

Any row satisfying the above conditions is a candidate document.

### Row Encoding

All keys are manually encoded to ensure a precise row ordering.

Internal Row values are opaque byte arrays.

All other values are encoded using protobuf for a balance of efficiency and flexibility.  Dictionary and TermFrequency rows are the most likely to take advantage of this flexibility, but other rows are read/written infrequently enough that the flexibility outweighs any overhead.

#### Version

There is a single version row which records which version of the firestorm indexing scheme is in use.

| Key     | Value      |
|---------|------------|
|```'v'```|```<VersionValue protobuf>```|

    message VersionValue {
        required uint64 version = 1;
    }

#### Field

Field rows map field names to numeric values

| Key     | Value      |
|---------|------------|
|```'f' <field id uint16>```|```<FieldValue protobuf>```|

    message FieldValue {
        required string name = 1;
    }

#### Dictionary

Dictionary rows record which terms are used in a particular field.  The value can be used to store additional information about the term usage.  The value will be encoded using protobuf so that future versions can add data to this structure.

| Key     | Value      |
|---------|------------|
|```'d' <field id uint16> <term utf8>```|```<DictionaryValue protobuf>```|

    message DictionaryValue {
        optional uint64 count = 1; // number of documents using this term in this field
    }

#### Term Frequency

Term Freqquency rows record which documents use a term in a particular field.  The value must record how often the term occurs.  It may optionally include other details such as a normalization value (precomputed scoring adjustment for the length of the field) and term vectors (where the term occurred within the field).  The value will be encoded using protobuf so that future versions can add data to this structure.

| Key     | Value      |
|---------|------------|
|```'t' <field id uint16> <term utf8> 0xff <doc_id utf8 > 0xff <doc number uint64>```|```<TermFreqValue protobuf>```|


    message TermVectorEntry {
        optional uint32 field = 1; // field optional if redundant, required for composite fields
        optional uint64 pos = 2; // positional offset within the field
        optional uint64 start = 3; // start byte offset
        optional uint64 end = 4; // end byte offset
        repeated uint64 arrayPositions = 5; // array positions
    }

    message TermFrequencyValue {
        required uint64 freq = 1; // frequency of the term occurance within this field
        optional float norm = 2; // normalization factor
        repeated TermVectorEntry vectors = 3; // term vectors
    }

#### Stored

Stored rows record the original values used to produce the index.  At the row encoding level this is an opaque sequence of bytes.

| Key                       | Value                   |
|---------------------------|-------------------------|
|```'s' <doc id utf8> 0xff <doc number uint64> <field id uint16>```|```<StoredValue protobuf>```|

    message StoredValue {
        optional bytes raw = 1; // raw bytes
    }

NOTE: we currently encode stored values as raw bytes, however we have other proposals in flight to do something better than this.  By using protobuf here as well, we can support existing functionality through the raw field, but allow for more strongly typed information in the future.

#### Internal

Internal rows are a reserved keyspace which the layer above can use for anything it wants.

| Key                       | Value                   |
|---------------------------|-------------------------|
|```'i' <application key []byte>```|```<application value []byte>```|

### FAQ

1.  How do you ensure correct semantics while updating a document in the index?

Let us consider 5 possible states:

  a.  Document X#1 is in the index, maxReadDocNumber=1, inFlightDocIds{}, deletedDocIdNumbers{}

  b.  Document X#1 and X#2 are in the index, maxReadDocNumber=1, inFlightDocIds{}, deletedDocIdNumbers{}

  c.  Document X#1 and X#2 are in the index, maxReadDocNumber=2, inFlightDocIds{X:2}, deletedDocIdNumbers{}

  d.  Document X#1 and X#2 are in the index, maxReadDocNumber=2, inFlightDocIds{}, deletedDocIdNumbers{X#1}

  e.  Document X#2 is in the index, maxReadDocNumber=2, inFlightDocIds{}, deletedDocIdNumbers{}

In state a, we have a steady state where one document has been indexed with id X.

In state b, we have executed the batch that writes the new rows corresponding to the new version of X, but we have not yet updated our in memory compensation data structures.  This is OK, because maxReadDocNumber is still 1, all readers will ignore the new rows we just wrote.  This is also OK because we are still inside the Index() method, so there is not yet any expectation to see the udpated document.

In state c, we have updated both the maxReadDocNumber to 2 and added X:2 to the inFlightDocIds map.  This means that searchers could find rows corresponding to X#1 and X#2.  However, they are forced to disregard any row for X where the document number is not 2.

In state d, we have completed the lookup for the old document numbers of X, and found 1.  Now deletedDocIdNumbers contains X#1.  Now readers that encounter this doc_id_number will ignore it.

In state e, the garbage collector has removed all record of X#1.

The Index method returns after it has transitioned to state c, which maintains the semantics we desire.

2\.  Wait, what happens if I kill -9 the process, won't you forget about the deleted documents?

No, our proposal is for a warmup process to walk a subset of the keyspace (TermFreq{ where field_id=0 }).  This warmup process will identify all not-yet cleaned up document numbers, and seed the deletedDocIdNumbers state as well as the Garbage Collector Thread.

3\.  Wait, but what will happen to the inFlightDocIds in a kill -9 scenario?

It turns out they actually don't matter.  That list was just an optimization to get us through the window of time while we hadn't yet looked up the old document numbers for a given document id.  But, during the warmup phase we still identify all those keys and they go directly onto deletedDocIdNumbers list.