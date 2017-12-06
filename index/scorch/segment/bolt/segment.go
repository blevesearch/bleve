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
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/Smerity/govarint"
	"github.com/blevesearch/bleve/index/scorch/segment"
	"github.com/boltdb/bolt"
	"github.com/couchbaselabs/vellum"
	"github.com/golang/snappy"
)

var readOnlyOptions = &bolt.Options{
	ReadOnly: true,
}

// _id field is always guaranteed to have fieldID of 0
const idFieldID uint16 = 0

// Open returns a boltdb impl of a segment
func Open(path string) (segment.Segment, error) {

	db, err := bolt.Open(path, 0600, readOnlyOptions)
	if err != nil {
		return nil, err
	}

	tx, err := db.Begin(false)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	rv := &Segment{
		db:        db,
		tx:        tx,
		fieldsMap: make(map[string]uint16),
	}

	err = rv.loadConfig()
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	err = rv.loadFields()
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	return rv, nil
}

// Segment implements a boltdb based implementation of a segment
type Segment struct {
	version     uint8
	chunkFactor uint32
	db          *bolt.DB
	tx          *bolt.Tx

	fieldsMap map[string]uint16
	fieldsInv []string
	fieldsLoc []bool
}

func (s *Segment) loadConfig() (err error) {
	bucket := s.tx.Bucket(configBucket)
	if bucket == nil {
		return fmt.Errorf("config bucket missing")
	}

	ver := bucket.Get(versionKey)
	if ver == nil {
		return fmt.Errorf("version key missing")
	}
	s.version = ver[0]

	chunk := bucket.Get(chunkKey)
	if chunk == nil {
		return fmt.Errorf("chunk key is missing")
	}
	s.chunkFactor = binary.BigEndian.Uint32(chunk)

	return nil
}

// loadFields reads the fields info from the segment so that we never have to go
// back to disk to access this (small and used frequently)
func (s *Segment) loadFields() (err error) {

	bucket := s.tx.Bucket(fieldsBucket)
	if bucket == nil {
		return fmt.Errorf("fields bucket missing")
	}

	indexLocs := roaring.NewBitmap()
	err = bucket.ForEach(func(k []byte, v []byte) error {

		// process index locations bitset
		if k[0] == indexLocsKey[0] {
			_, err2 := indexLocs.FromBuffer(v)
			if err2 != nil {
				return fmt.Errorf("error loading indexLocs: %v", err2)
			}
		} else {

			_, fieldID, err2 := segment.DecodeUvarintAscending(k)
			if err2 != nil {
				return err2
			}
			// we store fieldID+1 in so we can discern the zero value
			s.fieldsMap[string(v)] = uint16(fieldID + 1)
		}
		return nil
	})
	if err != nil {
		return err
	}

	// now setup the inverse (should have same size as map and be keyed 0-(len-1))
	s.fieldsInv = make([]string, len(s.fieldsMap))
	for k, v := range s.fieldsMap {
		s.fieldsInv[int(v)-1] = k
	}
	s.fieldsLoc = make([]bool, len(s.fieldsInv))
	for i := range s.fieldsInv {
		if indexLocs.ContainsInt(i) {
			s.fieldsLoc[i] = true
		}
	}

	return nil
}

// Fields returns the field names used in this segment
func (s *Segment) Fields() []string {
	return s.fieldsInv
}

// Count returns the number of documents in this segment
// (this has no notion of deleted docs)
func (s *Segment) Count() uint64 {
	return uint64(s.tx.Bucket(storedBucket).Stats().BucketN - 1)
}

// Dictionary returns the term dictionary for the specified field
func (s *Segment) Dictionary(field string) (segment.TermDictionary, error) {
	dict, err := s.dictionary(field)
	if err == nil && dict == nil {
		return &segment.EmptyDictionary{}, nil
	}
	return dict, err
}

func (s *Segment) dictionary(field string) (*Dictionary, error) {

	rv := &Dictionary{
		segment: s,
		field:   field,
	}

	rv.fieldID = s.fieldsMap[field]
	if rv.fieldID > 0 {
		rv.fieldID = rv.fieldID - 1
		fieldIDKey := segment.EncodeUvarintAscending(nil, uint64(rv.fieldID))
		bucket := s.tx.Bucket(dictBucket)
		if bucket == nil {
			return nil, fmt.Errorf("dictionary bucket missing")
		}
		fstBytes := bucket.Get(fieldIDKey)
		if fstBytes == nil {
			return nil, fmt.Errorf("dictionary field %s bytes nil", field)
		}
		if fstBytes != nil {
			fst, err := vellum.Load(fstBytes)
			if err != nil {
				return nil, fmt.Errorf("dictionary field %s vellum err: %v", field, err)
			}
			if err == nil {
				rv.fst = fst
			}
		}

	} else {
		return nil, nil
	}

	return rv, nil
}

// VisitDocument invokes the DocFieldValueVistor for each stored field
// for the specified doc number
func (s *Segment) VisitDocument(num uint64, visitor segment.DocumentFieldValueVisitor) error {
	storedBuucket := s.tx.Bucket(storedBucket)
	if storedBuucket == nil {
		return fmt.Errorf("stored bucket missing")
	}
	docNumKey := segment.EncodeUvarintAscending(nil, num)
	docBucket := storedBuucket.Bucket(docNumKey)
	if docBucket == nil {
		return fmt.Errorf("segment has no doc number %d", num)
	}
	metaBytes := docBucket.Get(metaKey)
	if metaBytes == nil {
		return fmt.Errorf("stored meta bytes for doc number %d is nil", num)
	}
	dataBytes := docBucket.Get(dataKey)
	if dataBytes == nil {
		return fmt.Errorf("stored data bytes for doc number %d is nil", num)
	}
	uncompressed, err := snappy.Decode(nil, dataBytes)
	if err != nil {
		return err
	}

	reader := bytes.NewReader(metaBytes)
	decoder := govarint.NewU64Base128Decoder(reader)

	keepGoing := true
	for keepGoing {
		field, err := decoder.GetU64()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		typ, err := decoder.GetU64()
		if err != nil {
			return err
		}
		offset, err := decoder.GetU64()
		if err != nil {
			return err
		}
		l, err := decoder.GetU64()
		if err != nil {
			return err
		}
		numap, err := decoder.GetU64()
		if err != nil {
			return err
		}
		var arrayPos []uint64
		if numap > 0 {
			arrayPos = make([]uint64, numap)
			for i := 0; i < int(numap); i++ {
				ap, err := decoder.GetU64()
				if err != nil {
					return err
				}
				arrayPos[i] = ap
			}
		}

		value := uncompressed[offset : offset+l]
		keepGoing = visitor(s.fieldsInv[field], byte(typ), value, arrayPos)
	}

	return nil
}

// DocNumbers returns a bitset corresponding to the doc numbers of all the
// provided _id strings
func (s *Segment) DocNumbers(ids []string) (*roaring.Bitmap, error) {
	rv := roaring.New()

	if len(s.fieldsMap) > 0 {
		idDict, err := s.dictionary("_id")
		if err != nil {
			return nil, err
		}

		for _, id := range ids {
			postings, err := idDict.postingsList(id, nil)
			if err != nil {
				return nil, err
			}
			if postings.postings != nil {
				rv.Or(postings.postings)
			}
		}
	}

	return rv, nil
}

// Close releases all resources associated with this segment
func (s *Segment) Close() error {
	err := s.tx.Rollback()
	if err != nil {
		_ = s.db.Close()
		return err
	}
	return s.db.Close()
}

func (s *Segment) Path() string {
	return s.db.Path()
}
