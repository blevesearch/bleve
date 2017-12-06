//  Copyright (c) 2017 Couchbase, Inc.
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

package bolt

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/RoaringBitmap/roaring"
	"github.com/Smerity/govarint"
	"github.com/blevesearch/bleve/index/scorch/segment"
	"github.com/blevesearch/bleve/index/scorch/segment/mem"
	"github.com/boltdb/bolt"
	"github.com/couchbaselabs/vellum"
	"github.com/golang/snappy"
)

var fieldsBucket = []byte{'a'}
var dictBucket = []byte{'b'}
var postingsBucket = []byte{'c'}
var postingDetailsBucket = []byte{'d'}
var storedBucket = []byte{'e'}
var configBucket = []byte{'x'}

var indexLocsKey = []byte{'l'}

var freqNormKey = []byte{'a'}
var locKey = []byte{'b'}

var metaKey = []byte{'a'}
var dataKey = []byte{'b'}

var chunkKey = []byte{'c'}
var versionKey = []byte{'v'}

var version = 0

func PersistSegment(memSegment *mem.Segment, path string, chunkFactor uint32) (err error) {
	db, err := bolt.Open(path, 0777, nil)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := db.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()

	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}()

	err = persistFields(memSegment, tx)
	if err != nil {
		return err
	}

	err = persistDictionary(memSegment, tx)
	if err != nil {
		return err
	}

	err = persistPostings(memSegment, tx)
	if err != nil {
		return err
	}

	err = persistPostingsDetails(memSegment, tx, chunkFactor)
	if err != nil {
		return err
	}

	err = persistStored(memSegment, tx)
	if err != nil {
		return err
	}

	err = persistConfig(tx, chunkFactor)
	if err != nil {
		return err
	}

	return nil
}

// persistFields puts the fields as separate k/v pairs in the fields bucket
// makes very little attempt to squeeze a lot of perf because it is expected
// this is usually somewhat small, and when re-opened it will be read once and
// kept on the heap, and not read out of the file subsequently
func persistFields(memSegment *mem.Segment, tx *bolt.Tx) error {
	bucket, err := tx.CreateBucket(fieldsBucket)
	if err != nil {
		return err
	}
	bucket.FillPercent = 1.0

	// build/persist a bitset corresponding to the field locs array
	indexLocs := roaring.NewBitmap()
	for i, indexLoc := range memSegment.FieldsLoc {
		if indexLoc {
			indexLocs.AddInt(i)
		}
	}
	var indexLocsBuffer bytes.Buffer
	_, err = indexLocs.WriteTo(&indexLocsBuffer)
	if err != nil {
		return err
	}
	err = bucket.Put(indexLocsKey, indexLocsBuffer.Bytes())
	if err != nil {
		return err
	}

	// we use special varint which is still guaranteed to sort correctly
	fieldBuf := make([]byte, 0, segment.MaxVarintSize)
	for fieldID, fieldName := range memSegment.FieldsInv {
		if fieldID != 0 {
			// reset buffer if necessary
			fieldBuf = fieldBuf[:0]
		}
		fieldBuf = segment.EncodeUvarintAscending(fieldBuf, uint64(fieldID))
		err = bucket.Put(fieldBuf, []byte(fieldName))
		if err != nil {
			return err
		}
	}
	return nil
}

func persistDictionary(memSegment *mem.Segment, tx *bolt.Tx) error {
	bucket, err := tx.CreateBucket(dictBucket)
	if err != nil {
		return err
	}
	bucket.FillPercent = 1.0

	// TODO consider whether or not there is benefit to building the vellums
	// concurrently.  While we have to insert them into the bolt in order,
	// the (presumably) heavier lifting involved in building the FST could
	// be done concurrently.

	fieldBuf := make([]byte, 0, segment.MaxVarintSize)
	for fieldID, fieldTerms := range memSegment.DictKeys {
		if fieldID != 0 {
			// reset buffers if necessary
			fieldBuf = fieldBuf[:0]
		}
		// start a new vellum for this field
		var buffer bytes.Buffer
		builder, err := vellum.New(&buffer, nil)
		if err != nil {
			return err
		}

		dict := memSegment.Dicts[fieldID]
		// now walk the dictionary in order of fieldTerms (already sorted)
		for i := range fieldTerms {
			err = builder.Insert([]byte(fieldTerms[i]), dict[fieldTerms[i]]-1)
			if err != nil {
				return err
			}
		}
		err = builder.Close()
		if err != nil {
			return err
		}

		// put this FST into bolt
		// we use special varint which is still guaranteed to sort correctly
		fieldBuf = segment.EncodeUvarintAscending(fieldBuf, uint64(fieldID))
		err = bucket.Put(fieldBuf, buffer.Bytes())
		if err != nil {
			return err
		}
	}

	return nil
}

func persistPostings(memSegment *mem.Segment, tx *bolt.Tx) error {
	bucket, err := tx.CreateBucket(postingsBucket)
	if err != nil {
		return err
	}
	bucket.FillPercent = 1.0

	postingIDBuf := make([]byte, 0, segment.MaxVarintSize)
	for postingID := range memSegment.Postings {
		if postingID != 0 {
			// reset buffers if necessary
			postingIDBuf = postingIDBuf[:0]
		}
		postingIDBuf = segment.EncodeUvarintAscending(postingIDBuf, uint64(postingID))
		var postingsBuf bytes.Buffer
		_, err := memSegment.Postings[postingID].WriteTo(&postingsBuf)
		if err != nil {
			return err
		}
		err = bucket.Put(postingIDBuf, postingsBuf.Bytes())
		if err != nil {
			return err
		}
	}

	return nil
}

func persistPostingsDetails(memSegment *mem.Segment, tx *bolt.Tx,
	chunkFactor uint32) error {
	bucket, err := tx.CreateBucket(postingDetailsBucket)
	if err != nil {
		return err
	}
	bucket.FillPercent = 1.0

	postingIDBuf := make([]byte, 0, segment.MaxVarintSize)
	for postingID := range memSegment.Postings {
		if postingID != 0 {
			// reset buffers if necessary
			postingIDBuf = postingIDBuf[:0]
		}
		postingIDBuf = segment.EncodeUvarintAscending(postingIDBuf, uint64(postingID))

		// make bucket for posting details
		postingBucket, err := bucket.CreateBucket(postingIDBuf)
		if err != nil {
			return err
		}
		postingBucket.FillPercent = 1.0

		err = persistPostingDetails(memSegment, postingBucket, postingID, chunkFactor)
		if err != nil {
			return err
		}
	}

	return nil
}

func persistPostingDetails(memSegment *mem.Segment, postingBucket *bolt.Bucket,
	postingID int, chunkFactor uint32) error {
	// walk the postings list
	var err error
	var chunkBucket *bolt.Bucket
	var currChunk uint32
	chunkIDBuf := make([]byte, 0, segment.MaxVarintSize)
	postingsListItr := memSegment.Postings[postingID].Iterator()
	var encoder *govarint.Base128Encoder
	var locEncoder *govarint.Base128Encoder

	encodingBuf := &bytes.Buffer{}
	locEncodingBuf := &bytes.Buffer{}

	var offset int
	var locOffset int
	for postingsListItr.HasNext() {
		docNum := postingsListItr.Next()
		chunk := docNum / chunkFactor

		// create new chunk bucket if necessary
		if chunkBucket == nil || currChunk != chunk {

			// close out last chunk
			if chunkBucket != nil {

				// fix me write freq/norms
				encoder.Close()
				err = chunkBucket.Put(freqNormKey, encodingBuf.Bytes())
				if err != nil {
					return err
				}
				locEncoder.Close()
				err = chunkBucket.Put(locKey, locEncodingBuf.Bytes())
				if err != nil {
					return err
				}

				// reset for next
				chunkIDBuf = chunkIDBuf[:0]
				encodingBuf = &bytes.Buffer{}
				locEncodingBuf = &bytes.Buffer{}
			}

			// prepare next chunk
			chunkIDBuf = segment.EncodeUvarintAscending(chunkIDBuf, uint64(chunk))
			chunkBucket, err = postingBucket.CreateBucket(chunkIDBuf)
			if err != nil {
				return err
			}
			chunkBucket.FillPercent = 1.0
			currChunk = chunk

			encoder = govarint.NewU64Base128Encoder(encodingBuf)
			locEncoder = govarint.NewU64Base128Encoder(locEncodingBuf)
		}

		// put freq
		_, err = encoder.PutU64(memSegment.Freqs[postingID][offset])
		if err != nil {
			return err
		}

		// put norm
		norm := memSegment.Norms[postingID][offset]
		normBits := math.Float32bits(norm)
		_, err = encoder.PutU32(normBits)
		if err != nil {
			return err
		}

		// put locations

		for i := 0; i < int(memSegment.Freqs[postingID][offset]); i++ {

			if len(memSegment.Locfields[postingID]) > 0 {
				// put field
				_, err = locEncoder.PutU64(uint64(memSegment.Locfields[postingID][locOffset]))
				if err != nil {
					return err
				}

				// put pos
				_, err = locEncoder.PutU64(memSegment.Locpos[postingID][locOffset])
				if err != nil {
					return err
				}

				// put start
				_, err = locEncoder.PutU64(memSegment.Locstarts[postingID][locOffset])
				if err != nil {
					return err
				}

				// put end
				_, err = locEncoder.PutU64(memSegment.Locends[postingID][locOffset])
				if err != nil {
					return err
				}

				// put array positions
				num := len(memSegment.Locarraypos[postingID][locOffset])

				// put the number of array positions to follow
				_, err = locEncoder.PutU64(uint64(num))
				if err != nil {
					return err
				}

				// put each array position
				for j := 0; j < num; j++ {
					_, err = locEncoder.PutU64(memSegment.Locarraypos[postingID][locOffset][j])
					if err != nil {
						return err
					}
				}
			}

			locOffset++
		}

		offset++
	}

	// close out last chunk

	if chunkBucket != nil {
		// fix me write freq/norms
		encoder.Close()
		err = chunkBucket.Put(freqNormKey, encodingBuf.Bytes())
		if err != nil {
			return err
		}
		locEncoder.Close()
		err = chunkBucket.Put(locKey, locEncodingBuf.Bytes())
		if err != nil {
			return err
		}
	}

	return nil
}

func persistStored(memSegment *mem.Segment, tx *bolt.Tx) error {
	bucket, err := tx.CreateBucket(storedBucket)
	if err != nil {
		return err
	}
	bucket.FillPercent = 1.0

	var curr int
	// we use special varint which is still guaranteed to sort correctly
	docNumBuf := make([]byte, 0, segment.MaxVarintSize)
	for docNum, storedValues := range memSegment.Stored {
		var metaBuf bytes.Buffer
		var data, compressed []byte
		if docNum != 0 {
			// reset buffer if necessary
			docNumBuf = docNumBuf[:0]
			curr = 0
		}
		// create doc sub-bucket
		docNumBuf = segment.EncodeUvarintAscending(docNumBuf, uint64(docNum))
		docBucket, err := bucket.CreateBucket(docNumBuf)
		if err != nil {
			return err
		}
		docBucket.FillPercent = 1.0

		metaEncoder := govarint.NewU64Base128Encoder(&metaBuf)

		// encode fields in order
		for fieldID := range memSegment.FieldsInv {
			if storedFieldValues, ok := storedValues[uint16(fieldID)]; ok {
				// has stored values for this field
				num := len(storedFieldValues)

				// process each value
				for i := 0; i < num; i++ {
					// encode field
					_, err2 := metaEncoder.PutU64(uint64(fieldID))
					if err2 != nil {
						return err2
					}
					// encode type
					_, err2 = metaEncoder.PutU64(uint64(memSegment.StoredTypes[docNum][uint16(fieldID)][i]))
					if err2 != nil {
						return err2
					}
					// encode start offset
					_, err2 = metaEncoder.PutU64(uint64(curr))
					if err2 != nil {
						return err2
					}
					// end len
					_, err2 = metaEncoder.PutU64(uint64(len(storedFieldValues[i])))
					if err2 != nil {
						return err2
					}
					// encode number of array pos
					_, err2 = metaEncoder.PutU64(uint64(len(memSegment.StoredPos[docNum][uint16(fieldID)][i])))
					if err2 != nil {
						return err2
					}
					// encode all array positions
					for j := 0; j < len(memSegment.StoredPos[docNum][uint16(fieldID)][i]); j++ {
						_, err2 = metaEncoder.PutU64(memSegment.StoredPos[docNum][uint16(fieldID)][i][j])
						if err2 != nil {
							return err2
						}
					}
					// append data
					data = append(data, storedFieldValues[i]...)
					// update curr
					curr += len(storedFieldValues[i])
				}
			}
		}
		metaEncoder.Close()

		err = docBucket.Put(metaKey, metaBuf.Bytes())
		if err != nil {
			return err
		}

		// compress data
		compressed = snappy.Encode(compressed, data)

		err = docBucket.Put(dataKey, compressed)
		if err != nil {
			return err
		}

	}

	return nil
}

func persistConfig(tx *bolt.Tx, chunkFactor uint32) error {
	bucket, err := tx.CreateBucket(configBucket)
	if err != nil {
		return err
	}

	chunkVal := make([]byte, 4)
	binary.BigEndian.PutUint32(chunkVal, chunkFactor)
	err = bucket.Put(chunkKey, chunkVal)
	if err != nil {
		return err
	}

	err = bucket.Put(versionKey, []byte{byte(version)})
	if err != nil {
		return err
	}

	return nil
}
