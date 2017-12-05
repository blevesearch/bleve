# bolt segment format

## top level key space (all sub-buckets, as bolt has no root bucket)

We have chosen to letter these starting with 'a' and in the code refer to them with more meaningful names.  The reason is that we intend to write them in order, and this lets us rearrange them more easily later.

- 'a' field storage
- 'b' term dictionaries
- 'c' postings list
- 'd' postings details
- 'e' stored fields
- 'x' configuration

## variable length integers that sort correctly (insert order same as numeric)

We use numbers as keys in several places.  We want those keys to be small, so we prefer to use a variable length key to minimize space, but we also want to insert these in order, so the encoding has to sort correctly.

We have chosen to the the scheme found in [CockroachDB](https://github.com/cockroachdb/cockroach/blob/2dd65dde5d90c157f4b93f92502ca1063b904e1d/pkg/util/encoding/encoding.go).

In short, the first byte indicates how many bytes will follow, with a few other nice properties.
- values 0-127 are not used in the first byte (this means we can still use any ASCII values we want and avoid collision)
- very small values are packed directly into this first byte
For the full details see the link above.

## field storage bucket

Contains one row for each field, the key is the integer field ID, and the value is the string name associated with the field.

There is one additional row with key 'l'.  The value is a binary serialization of a [roaring bitmap](https://github.com/RoaringBitmap/roaring), with bits set for each field id which also index location details with each posting.

## term dictionary bucket

Contains one row for each field, the key is the integer field ID, and the value is a binary serialization of the [Vellum](https://github.com/couchbaselabs/vellum) FST.  The Vellum FST maps from term (utf-8 string) to a posting ID (uint64).

## postings list bucket

Contains one row for each postings list, the key is the integer posting ID, the value is a binary serialization of a [roaring bitmap](https://github.com/RoaringBitmap/roaring).  The roaring bitmap has bits set for each doc number that used this term in this field.

## posting details bucket

Contains one sub-bucket for each postings list, the name of the sub-bucket is the posting ID.

### individual posting detail sub-bucket

Contains one sub-bucket for each chunk.  A chunk contains details for sub-section of the docNum key space.  By default, the chunk size is 1024, so all posting details for the first 1024 docs are in chunk zero, then the next 1024 in chunk one, and so on.

The purpose of the chunking is so that when trying to Seek/Advance through a large number of hits to something much further ahead, we have to keep seeking through the roaring bitmap, but we can jump to the nearest chunk for details, and only seek within the details of the current chunk.

#### chunk posting detail sub-bucket

Contains two key/value pairs:

Key 'a' contains a [govarint](https://github.com/Smerity/govarint) compressed slice of uint64 values.  For each hit in the postings list, there are two values on this list, the first is the term frequency (uint64) and the second is the norm factor (float32).

Key 'b' contains a [govarint](https://github.com/Smerity/govarint) compressed slice of uint64 values.  For each location (there will be one location for each 'frequency' in the list above) there will be a variable number of uint64 values as follows:

- field ID (uint16)
- pos (uint64)
- start (uint64)
- end (uint64)
- number of array position entries that follow (uint64)
- variable number of array positions (each uint64)

## stored field values sub-bucket

Contains one sub-bucket for each doc number (uint64).

## stored field doc specific sub-bucket

Contains two key/value pairs:

Key 'a' contains a [govarint](https://github.com/Smerity/govarint) compressed slice of uint64 values.  For each stored field there are a variable number of uint64 values as follows:

- field ID (uint16)
- value type (byte) (string/number/date/geo/etc)
- start offset (in the uncompressed slice of data)
- length (in the uncompressed slice of data)
- number of array position entries that follow (uint64)
- variable number of array positions (each uint64)

Key 'b' contains a [snappy]() compressed sequence of bytes.  The input to the snappy compression was a slice of bytes containing the field values, in the same order the metadata slice was created.

## configuration sub-bucket

Currently contains two key/value pairs:

Key 'c' contains a BigEndian encoded uint32 chunk size.  This chunk size must be used when computing doc number to chunk conversions in this segment.

Key 'v' contains a version number, currently 0.

## Example

The following is a dump of the boltdb bucket/key/value space for a segment which contains two documents:

```
{
  "_id": "a",
  "name": "wow",
  "desc": "some thing",
  "tag": ["cold", "dark"]
}

{
  "_id": "b",
  "name": "who",
  "desc": "some thing",
  "tag": ["cold", "dark"]
}
```

```
[61]  ('a' - field storage)
  6c ('l' - roaring bitmap of field IDs which have index location data)
    3a 30 00 00 01 00 00 00 00 00 03 00 10 00 00 00 01 00 02 00 03 00 04 00
  88 (field ID 0)
    5f 69 64 (utf-8 string '_id')
  89 (field ID 1)
    5f 61 6c 6c (utf-8 string '_all')
  8a (field ID 2)
    6e 61 6d 65 (utf-8 string 'name')
  8b (field ID 3)
    64 65 73 63 (utf-8 string 'desc')
  8c (field ID 4)
    74 61 67 (utf-8 string 'tag')
[62] ('b' - term dictionary)
  88 (field ID 0)
    01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 0b 05 00 00 62 61 11 02 02 00 00 00 00 00 00 00 17 00 00 00 00 00 00 00 (vellum FST data)
  89
    01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 10 92 cf c4 00 10 a7 c7 c5 00 10 82 d0 c4 00 10 97 cb c8 ce 00 10 84 00 10 8c 00 0d 01 04 6f 68 11 02 00 02 01 04 03 01 0f 15 1a 1f 77 74 73 64 63 11 05 06 00 00 00 00 00 00 00 43 00 00 00 00 00 00 00
  8a
    01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 10 84 00 10 8c 00 06 01 04 6f 68 11 02 06 01 11 8c 02 00 00 00 00 00 00 00 21 00 00 00 00 00 00 00
  8b
    01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 10 82 d0 c4 00 10 97 cb c8 ce 08 07 01 07 74 73 11 02 02 00 00 00 00 00 00 00 22 00 00 00 00 00 00 00
  8c
    01 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 10 92 cf c4 00 10 a7 c7 c5 0a 09 01 06 64 63 11 02 02 00 00 00 00 00 00 00 21 00 00 00 00 00 00 00
[63] ('c' - postings lists)
  88 (field ID 0)
    3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 00 00 (roaring bitmap data)
  89
    3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 01 00
  8a
    3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 01 00
  8b
    3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 01 00
  8c
    3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 01 00
  8d
    3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 00 00
  8e
    3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 00 00
  8f
    3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 01 00
  90
    3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 01 00
  91
    3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 01 00
  92
    3a 30 00 00 01 00 00 00 00 00 01 00 10 00 00 00 00 00 01 00
  93
    3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 01 00
  94
    3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 01 00
  95
    3a 30 00 00 01 00 00 00 00 00 00 00 10 00 00 00 01 00
[64] ('d' - postings details)
  [88] (posting ID 0)
    [88] (chunk ID 0)
      61 ('a' term freq/norm data)
        01 ae f2 93 f7 03
      62 ('b' term location data)
        02 01 00 03 00
  [89] (posting ID 1)
    [88] (chunk ID 0)
      61 ('a' term freq/norm data)
        01 ae f2 93 f7 03
      62 ('b' term location data)
        03 01 00 04 00
    [89] (chunk ID 1)
      61 ('a' term freq/norm data)
        01 ae f2 93 f7 03
      62 ('b' term location data)
        03 01 00 04 00
  [8a]
    [88]
      61
        01 ae f2 93 f7 03
      62
        03 02 05 0a 00
    [89]
      61
        01 ae f2 93 f7 03
      62
        03 02 05 0a 00
  [8b]
    [88]
      61
        01 ae f2 93 f7 03
      62
        04 01 00 04 01 00
    [89]
      61
        01 ae f2 93 f7 03
      62
        04 01 00 04 01 00
  [8c]
    [88]
      61
        01 ae f2 93 f7 03
      62
        04 01 00 04 01 01
    [89]
      61
        01 ae f2 93 f7 03
      62
        04 01 00 04 01 01
  [8d]
    [88]
      61
        01 80 80 80 fc 03
      62

  [8e]
    [88]
      61
        01 80 80 80 fc 03
      62
        02 01 00 03 00
  [8f]
    [88]
      61
        01 f3 89 d4 f9 03
      62
        03 01 00 04 00
    [89]
      61
        01 f3 89 d4 f9 03
      62
        03 01 00 04 00
  [90]
    [88]
      61
        01 f3 89 d4 f9 03
      62
        03 02 05 0a 00
    [89]
      61
        01 f3 89 d4 f9 03
      62
        03 02 05 0a 00
  [91]
    [88]
      61
        01 f3 89 d4 f9 03
      62
        04 01 00 04 01 00
    [89]
      61
        01 f3 89 d4 f9 03
      62
        04 01 00 04 01 00
  [92]
    [88]
      61
        01 f3 89 d4 f9 03
      62
        04 01 00 04 01 01
    [89]
      61
        01 f3 89 d4 f9 03
      62
        04 01 00 04 01 01
  [93]
    [89]
      61
        01 80 80 80 fc 03
      62

  [94]
    [89]
      61
        01 80 80 80 fc 03
      62
        02 01 00 03 00
  [95]
    [89]
      61
        01 ae f2 93 f7 03
      62
        02 01 00 03 00
[65] ('e' - stored fields)
  [88] (doc num 0)
    61 ('a' - stored field meta slice)
      00 74 00 01 00 02 74 01 03 00 03 74 04 0a 00 04 74 0e 04 01 00 04 74 12 04 01 01
    62 ('b' - snappy compressed value bytes)
      16 54 61 77 6f 77 73 6f 6d 65 20 74 68 69 6e 67 63 6f 6c 64 64 61 72 6b
  [89]
    61
      00 74 00 01 00 02 74 01 03 00 03 74 04 0a 00 04 74 0e 04 01 00 04 74 12 04 01 01
    62
      16 54 62 77 68 6f 73 6f 6d 65 20 74 68 69 6e 67 63 6f 6c 64 64 61 72 6b
[78] ('x' - configuration)
  63 ('c' - chunk size)
    00 00 00 01 (big endian 1)
  76 ('v' - version)
    00 (single byte 0)
```
