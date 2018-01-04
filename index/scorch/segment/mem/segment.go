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

package mem

import (
	"fmt"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/index/scorch/segment"
)

// _id field is always guaranteed to have fieldID of 0
const idFieldID uint16 = 0

// KNOWN ISSUES
// - LIMITATION - we decided whether or not to store term vectors for a field
//                at the segment level, based on the first definition of a
//                field we see.  in normal bleve usage this is fine, all
//                instances of a field definition will be the same.  however,
//                advanced users may violate this and provide unique field
//                definitions with each document.  this segment does not
//                support this usage.

// TODO
// - need better testing of multiple docs, iterating freqs, locations and
//   and verifying the correct results are returned

// Segment is an in memory implementation of scorch.Segment
type Segment struct {

	// FieldsMap name -> id+1
	FieldsMap map[string]uint16
	// fields id -> name
	FieldsInv []string

	// term dictionary
	//  field id -> term -> posting id + 1
	Dicts []map[string]uint64

	// term dictionary keys
	//  field id -> []dictionary keys
	DictKeys [][]string

	// Postings list
	//  Postings list id -> Postings bitmap
	Postings []*roaring.Bitmap

	// Postings List has locations
	PostingsLocs []*roaring.Bitmap

	// term frequencies
	//  postings list id -> Freqs (one for each hit in bitmap)
	Freqs [][]uint64

	// field Norms
	//  postings list id -> Norms (one for each hit in bitmap)
	Norms [][]float32

	// field/start/end/pos/locarraypos
	//  postings list id -> start/end/pos/locarraypos (one for each freq)
	Locfields   [][]uint16
	Locstarts   [][]uint64
	Locends     [][]uint64
	Locpos      [][]uint64
	Locarraypos [][][]uint64

	// Stored field values
	//  docNum -> field id -> slice of values (each value []byte)
	Stored []map[uint16][][]byte

	// stored field types
	//  docNum -> field id -> slice of types (each type byte)
	StoredTypes []map[uint16][]byte

	// stored field array positions
	//  docNum -> field id -> slice of array positions (each is []uint64)
	StoredPos []map[uint16][][]uint64

	// for storing the docValue persisted fields
	DocValueFields map[uint16]bool

	// footprint of the segment, updated when analyzed document mutations
	// are added into the segment
	sizeInBytes uint64
}

// New builds a new empty Segment
func New() *Segment {
	return &Segment{
		FieldsMap:      map[string]uint16{},
		DocValueFields: map[uint16]bool{},
	}
}

func (s *Segment) updateSizeInBytes() {
	var sizeInBytes uint64

	for k, _ := range s.FieldsMap {
		sizeInBytes += uint64(len(k)*2 /* FieldsMap + FieldsInv */ +
			2 /* size of uint16 */)
	}

	for _, entry := range s.Dicts {
		for k, _ := range entry {
			sizeInBytes += uint64(len(k)*2 /* Dicts + DictKeys */ +
				8 /* size of uint64 */)
		}
	}

	for i := 0; i < len(s.Postings); i++ {
		sizeInBytes += s.Postings[i].GetSizeInBytes() + s.PostingsLocs[i].GetSizeInBytes()
	}

	for i := 0; i < len(s.Freqs); i++ {
		sizeInBytes += uint64(len(s.Freqs[i])*8 /* size of uint64 */ +
			len(s.Norms[i])*4 /* size of float32 */)
	}

	for i := 0; i < len(s.Locfields); i++ {
		sizeInBytes += uint64(len(s.Locfields[i])*2 /* size of uint16 */ +
			len(s.Locstarts[i])*8 /* size of uint64 */ +
			len(s.Locends[i])*8 /* size of uint64 */ +
			len(s.Locpos[i])*8 /* size of uint64 */)

		for j := 0; j < len(s.Locarraypos[i]); j++ {
			sizeInBytes += uint64(len(s.Locarraypos[i][j]) * 8 /* size of uint64 */)
		}
	}

	for i := 0; i < len(s.Stored); i++ {
		for _, v := range s.Stored[i] {
			sizeInBytes += uint64(2 /* size of uint16 */)
			for _, arr := range v {
				sizeInBytes += uint64(len(arr))
			}
		}

		for _, v := range s.StoredTypes[i] {
			sizeInBytes += uint64(2 /* size of uint16 */ + len(v))
		}

		for _, v := range s.StoredPos[i] {
			sizeInBytes += uint64(2 /* size of uint16 */)
			for _, arr := range v {
				sizeInBytes += uint64(len(arr) * 8 /* size of uint64 */)
			}
		}
	}

	sizeInBytes += uint64(8 /* size of sizeInBytes -> uint64*/)

	s.sizeInBytes = sizeInBytes
}

func (s *Segment) SizeInBytes() uint64 {
	return s.sizeInBytes
}

func (s *Segment) AddRef() {
}

func (s *Segment) DecRef() error {
	return nil
}

// Fields returns the field names used in this segment
func (s *Segment) Fields() []string {
	return s.FieldsInv
}

// VisitDocument invokes the DocFieldValueVistor for each stored field
// for the specified doc number
func (s *Segment) VisitDocument(num uint64, visitor segment.DocumentFieldValueVisitor) error {
	// ensure document number exists
	if int(num) > len(s.Stored)-1 {
		return nil
	}
	docFields := s.Stored[int(num)]
	for field, values := range docFields {
		for i, value := range values {
			keepGoing := visitor(s.FieldsInv[field], s.StoredTypes[int(num)][field][i], value, s.StoredPos[int(num)][field][i])
			if !keepGoing {
				return nil
			}
		}
	}
	return nil
}

func (s *Segment) getField(name string) (int, error) {
	fieldID, ok := s.FieldsMap[name]
	if !ok {
		return 0, fmt.Errorf("no field named %s", name)
	}
	return int(fieldID - 1), nil
}

// Dictionary returns the term dictionary for the specified field
func (s *Segment) Dictionary(field string) (segment.TermDictionary, error) {
	fieldID, err := s.getField(field)
	if err != nil {
		// no such field, return empty dictionary
		return &segment.EmptyDictionary{}, nil
	}
	return &Dictionary{
		segment: s,
		field:   field,
		fieldID: uint16(fieldID),
	}, nil
}

// Count returns the number of documents in this segment
// (this has no notion of deleted docs)
func (s *Segment) Count() uint64 {
	return uint64(len(s.Stored))
}

// DocNumbers returns a bitset corresponding to the doc numbers of all the
// provided _id strings
func (s *Segment) DocNumbers(ids []string) (*roaring.Bitmap, error) {
	rv := roaring.New()

	// guard against empty segment
	if len(s.FieldsMap) > 0 {
		idDictionary := s.Dicts[idFieldID]

		for _, id := range ids {
			postingID := idDictionary[id]
			if postingID > 0 {
				rv.Or(s.Postings[postingID-1])
			}
		}
	}
	return rv, nil
}

// Close releases all resources associated with this segment
func (s *Segment) Close() error {
	return nil
}
