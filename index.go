//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bleve

import (
	"context"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/size"
)

// A Batch groups together multiple Index and Delete
// operations you would like performed at the same
// time.  The Batch structure is NOT thread-safe.
// You should only perform operations on a batch
// from a single thread at a time.  Once batch
// execution has started, you may not modify it.
type Batch struct {
	index    Index
	internal *index.Batch

	lastDocSize uint64
	totalSize   uint64
}

// Index adds the specified index operation to the
// batch.  NOTE: the bleve Index is not updated
// until the batch is executed.
func (b *Batch) Index(id string, data interface{}) error {
	if id == "" {
		return ErrorEmptyID
	}
	doc := document.NewDocument(id)
	err := b.index.Mapping().MapDocument(doc, data)
	if err != nil {
		return err
	}
	b.internal.Update(doc)

	b.lastDocSize = uint64(doc.Size() +
		len(id) + size.SizeOfString) // overhead from internal
	b.totalSize += b.lastDocSize

	return nil
}

func (b *Batch) LastDocSize() uint64 {
	return b.lastDocSize
}

func (b *Batch) TotalDocsSize() uint64 {
	return b.totalSize
}

// IndexAdvanced adds the specified index operation to the
// batch which skips the mapping.  NOTE: the bleve Index is not updated
// until the batch is executed.
func (b *Batch) IndexAdvanced(doc *document.Document) (err error) {
	if doc.ID == "" {
		return ErrorEmptyID
	}
	b.internal.Update(doc)
	return nil
}

// Delete adds the specified delete operation to the
// batch.  NOTE: the bleve Index is not updated until
// the batch is executed.
func (b *Batch) Delete(id string) {
	if id != "" {
		b.internal.Delete(id)
	}
}

// SetInternal adds the specified set internal
// operation to the batch. NOTE: the bleve Index is
// not updated until the batch is executed.
func (b *Batch) SetInternal(key, val []byte) {
	b.internal.SetInternal(key, val)
}

// DeleteInternal adds the specified delete internal
// operation to the batch. NOTE: the bleve Index is
// not updated until the batch is executed.
func (b *Batch) DeleteInternal(key []byte) {
	b.internal.DeleteInternal(key)
}

// Size returns the total number of operations inside the batch
// including normal index operations and internal operations.
func (b *Batch) Size() int {
	return len(b.internal.IndexOps) + len(b.internal.InternalOps)
}

// String prints a user friendly string representation of what
// is inside this batch.
func (b *Batch) String() string {
	return b.internal.String()
}

// Reset returns a Batch to the empty state so that it can
// be re-used in the future.
func (b *Batch) Reset() {
	b.internal.Reset()
	b.lastDocSize = 0
	b.totalSize = 0
}

func (b *Batch) Merge(o *Batch) {
	if o != nil && o.internal != nil {
		b.internal.Merge(o.internal)
		if o.LastDocSize() > 0 {
			b.lastDocSize = o.LastDocSize()
		}
		b.totalSize = uint64(b.internal.TotalDocSize())
	}
}

func (b *Batch) SetPersistedCallback(f index.BatchCallback) {
	b.internal.SetPersistedCallback(f)
}

func (b *Batch) PersistedCallback() index.BatchCallback {
	return b.internal.PersistedCallback()
}

// An Index implements all the indexing and searching
// capabilities of bleve.  An Index can be created
// using the New() and Open() methods.
//
// Index() takes an input value, deduces a DocumentMapping for its type,
// assigns string paths to its fields or values then applies field mappings on
// them.
//
// The DocumentMapping used to index a value is deduced by the following rules:
// 1) If value implements mapping.bleveClassifier interface, resolve the mapping
//    from BleveType().
// 2) If value implements mapping.Classifier interface, resolve the mapping
//    from Type().
// 3) If value has a string field or value at IndexMapping.TypeField.
// (defaulting to "_type"), use it to resolve the mapping. Fields addressing
// is described below.
// 4) If IndexMapping.DefaultType is registered, return it.
// 5) Return IndexMapping.DefaultMapping.
//
// Each field or nested field of the value is identified by a string path, then
// mapped to one or several FieldMappings which extract the result for analysis.
//
// Struct values fields are identified by their "json:" tag, or by their name.
// Nested fields are identified by prefixing with their parent identifier,
// separated by a dot.
//
// Map values entries are identified by their string key. Entries not indexed
// by strings are ignored. Entry values are identified recursively like struct
// fields.
//
// Slice and array values are identified by their field name. Their elements
// are processed sequentially with the same FieldMapping.
//
// String, float64 and time.Time values are identified by their field name.
// Other types are ignored.
//
// Each value identifier is decomposed in its parts and recursively address
// SubDocumentMappings in the tree starting at the root DocumentMapping.  If a
// mapping is found, all its FieldMappings are applied to the value. If no
// mapping is found and the root DocumentMapping is dynamic, default mappings
// are used based on value type and IndexMapping default configurations.
//
// Finally, mapped values are analyzed, indexed or stored. See
// FieldMapping.Analyzer to know how an analyzer is resolved for a given field.
//
// Examples:
//
//  type Date struct {
//    Day string `json:"day"`
//    Month string
//    Year string
//  }
//
//  type Person struct {
//    FirstName string `json:"first_name"`
//    LastName string
//    BirthDate Date `json:"birth_date"`
//  }
//
// A Person value FirstName is mapped by the SubDocumentMapping at
// "first_name". Its LastName is mapped by the one at "LastName". The day of
// BirthDate is mapped to the SubDocumentMapping "day" of the root
// SubDocumentMapping "birth_date". It will appear as the "birth_date.day"
// field in the index. The month is mapped to "birth_date.Month".
type Index interface {
	// Index analyzes, indexes or stores mapped data fields. Supplied
	// identifier is bound to analyzed data and will be retrieved by search
	// requests. See Index interface documentation for details about mapping
	// rules.
	Index(id string, data interface{}) error
	Delete(id string) error

	NewBatch() *Batch
	Batch(b *Batch) error

	// Document returns specified document or nil if the document is not
	// indexed or stored.
	Document(id string) (*document.Document, error)
	// DocCount returns the number of documents in the index.
	DocCount() (uint64, error)

	Search(req *SearchRequest) (*SearchResult, error)
	SearchInContext(ctx context.Context, req *SearchRequest) (*SearchResult, error)

	Fields() ([]string, error)

	FieldDict(field string) (index.FieldDict, error)
	FieldDictRange(field string, startTerm []byte, endTerm []byte) (index.FieldDict, error)
	FieldDictPrefix(field string, termPrefix []byte) (index.FieldDict, error)

	Close() error

	Mapping() mapping.IndexMapping

	Stats() *IndexStat
	StatsMap() map[string]interface{}

	GetInternal(key []byte) ([]byte, error)
	SetInternal(key, val []byte) error
	DeleteInternal(key []byte) error

	// Name returns the name of the index (by default this is the path)
	Name() string
	// SetName lets you assign your own logical name to this index
	SetName(string)

	// Advanced returns the indexer and data store, exposing lower level
	// methods to enumerate records and access data.
	Advanced() (index.Index, store.KVStore, error)
}

// New index at the specified path, must not exist.
// The provided mapping will be used for all
// Index/Search operations.
func New(path string, mapping mapping.IndexMapping) (Index, error) {
	return newIndexUsing(path, mapping, Config.DefaultIndexType, Config.DefaultKVStore, nil)
}

// NewMemOnly creates a memory-only index.
// The contents of the index is NOT persisted,
// and will be lost once closed.
// The provided mapping will be used for all
// Index/Search operations.
func NewMemOnly(mapping mapping.IndexMapping) (Index, error) {
	return newIndexUsing("", mapping, Config.DefaultIndexType, Config.DefaultMemKVStore, nil)
}

// NewUsing creates index at the specified path,
// which must not already exist.
// The provided mapping will be used for all
// Index/Search operations.
// The specified index type will be used.
// The specified kvstore implementation will be used
// and the provided kvconfig will be passed to its
// constructor. Note that currently the values of kvconfig must
// be able to be marshaled and unmarshaled using the encoding/json library (used
// when reading/writing the index metadata file).
func NewUsing(path string, mapping mapping.IndexMapping, indexType string, kvstore string, kvconfig map[string]interface{}) (Index, error) {
	return newIndexUsing(path, mapping, indexType, kvstore, kvconfig)
}

// Open index at the specified path, must exist.
// The mapping used when it was created will be used for all Index/Search operations.
func Open(path string) (Index, error) {
	return openIndexUsing(path, nil)
}

// OpenUsing opens index at the specified path, must exist.
// The mapping used when it was created will be used for all Index/Search operations.
// The provided runtimeConfig can override settings
// persisted when the kvstore was created.
func OpenUsing(path string, runtimeConfig map[string]interface{}) (Index, error) {
	return openIndexUsing(path, runtimeConfig)
}
