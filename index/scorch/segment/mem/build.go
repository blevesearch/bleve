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
	"math"
	"sort"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
)

// NewFromAnalyzedDocs places the analyzed document mutations into this segment
func NewFromAnalyzedDocs(results []*index.AnalysisResult) *Segment {
	s := New()

	// ensure that _id field get fieldID 0
	s.getOrDefineField("_id")

	// walk each doc
	for _, result := range results {
		s.processDocument(result)
	}

	// go back and sort the dictKeys
	for _, dict := range s.DictKeys {
		sort.Strings(dict)
	}

	// compute memory usage of segment
	s.updateSizeInBytes()

	// professional debugging
	//
	// log.Printf("fields: %v\n", s.FieldsMap)
	// log.Printf("fieldsInv: %v\n", s.FieldsInv)
	// log.Printf("fieldsLoc: %v\n", s.FieldsLoc)
	// log.Printf("dicts: %v\n", s.Dicts)
	// log.Printf("dict keys: %v\n", s.DictKeys)
	// for i, posting := range s.Postings {
	// 	log.Printf("posting %d: %v\n", i, posting)
	// }
	// for i, freq := range s.Freqs {
	// 	log.Printf("freq %d: %v\n", i, freq)
	// }
	// for i, norm := range s.Norms {
	// 	log.Printf("norm %d: %v\n", i, norm)
	// }
	// for i, field := range s.Locfields {
	// 	log.Printf("field %d: %v\n", i, field)
	// }
	// for i, start := range s.Locstarts {
	// 	log.Printf("start %d: %v\n", i, start)
	// }
	// for i, end := range s.Locends {
	// 	log.Printf("end %d: %v\n", i, end)
	// }
	// for i, pos := range s.Locpos {
	// 	log.Printf("pos %d: %v\n", i, pos)
	// }
	// for i, apos := range s.Locarraypos {
	// 	log.Printf("apos %d: %v\n", i, apos)
	// }
	// log.Printf("stored: %v\n", s.Stored)
	// log.Printf("stored types: %v\n", s.StoredTypes)
	// log.Printf("stored pos: %v\n", s.StoredPos)

	return s
}

func (s *Segment) processDocument(result *index.AnalysisResult) {
	// used to collate information across fields
	docMap := map[uint16]analysis.TokenFrequencies{}
	fieldLens := map[uint16]int{}
	docNum := uint64(s.addDocument())

	processField := func(field uint16, name string, l int, tf analysis.TokenFrequencies) {
		fieldLens[field] += l
		if existingFreqs, ok := docMap[field]; ok {
			existingFreqs.MergeAll(name, tf)
		} else {
			docMap[field] = tf
		}
	}

	storeField := func(docNum uint64, field uint16, typ byte, val []byte, pos []uint64) {
		s.Stored[docNum][field] = append(s.Stored[docNum][field], val)
		s.StoredTypes[docNum][field] = append(s.StoredTypes[docNum][field], typ)
		s.StoredPos[docNum][field] = append(s.StoredPos[docNum][field], pos)
	}

	// walk each composite field
	for _, field := range result.Document.CompositeFields {
		fieldID := uint16(s.getOrDefineField(field.Name()))
		l, tf := field.Analyze()
		processField(fieldID, field.Name(), l, tf)
	}

	// walk each field
	for i, field := range result.Document.Fields {
		fieldID := uint16(s.getOrDefineField(field.Name()))
		l := result.Length[i]
		tf := result.Analyzed[i]
		processField(fieldID, field.Name(), l, tf)
		if field.Options().IsStored() {
			storeField(docNum, fieldID, encodeFieldType(field), field.Value(), field.ArrayPositions())
		}

		if field.Options().IncludeDocValues() {
			s.DocValueFields[fieldID] = true
		}
	}

	// now that its been rolled up into docMap, walk that
	for fieldID, tokenFrequencies := range docMap {
		for term, tokenFreq := range tokenFrequencies {
			fieldTermPostings := s.Dicts[fieldID][term]

			// FIXME this if/else block has duplicate code that has resulted in
			// bugs fixed/missed more than once, need to refactor
			if fieldTermPostings == 0 {
				// need to build new posting
				bs := roaring.New()
				bs.AddInt(int(docNum))

				newPostingID := uint64(len(s.Postings) + 1)
				// add this new bitset to the postings slice
				s.Postings = append(s.Postings, bs)

				locationBS := roaring.New()
				s.PostingsLocs = append(s.PostingsLocs, locationBS)
				// add this to the details slice
				s.Freqs = append(s.Freqs, []uint64{uint64(tokenFreq.Frequency())})
				s.Norms = append(s.Norms, []float32{float32(1.0 / math.Sqrt(float64(fieldLens[fieldID])))})
				// add to locations
				var locfields []uint16
				var locstarts []uint64
				var locends []uint64
				var locpos []uint64
				var locarraypos [][]uint64
				if len(tokenFreq.Locations) > 0 {
					locationBS.AddInt(int(docNum))
				}
				for _, loc := range tokenFreq.Locations {
					var locf = fieldID
					if loc.Field != "" {
						locf = uint16(s.getOrDefineField(loc.Field))
					}
					locfields = append(locfields, locf)
					locstarts = append(locstarts, uint64(loc.Start))
					locends = append(locends, uint64(loc.End))
					locpos = append(locpos, uint64(loc.Position))
					if len(loc.ArrayPositions) > 0 {
						locarraypos = append(locarraypos, loc.ArrayPositions)
					} else {
						locarraypos = append(locarraypos, nil)
					}
				}
				s.Locfields = append(s.Locfields, locfields)
				s.Locstarts = append(s.Locstarts, locstarts)
				s.Locends = append(s.Locends, locends)
				s.Locpos = append(s.Locpos, locpos)
				s.Locarraypos = append(s.Locarraypos, locarraypos)
				// record it
				s.Dicts[fieldID][term] = newPostingID
				// this term was new for this field, add it to dictKeys
				s.DictKeys[fieldID] = append(s.DictKeys[fieldID], term)
			} else {
				// posting already started for this field/term
				// the actual offset is - 1, because 0 is zero value
				bs := s.Postings[fieldTermPostings-1]
				bs.AddInt(int(docNum))
				locationBS := s.PostingsLocs[fieldTermPostings-1]
				s.Freqs[fieldTermPostings-1] = append(s.Freqs[fieldTermPostings-1], uint64(tokenFreq.Frequency()))
				s.Norms[fieldTermPostings-1] = append(s.Norms[fieldTermPostings-1], float32(1.0/math.Sqrt(float64(fieldLens[fieldID]))))
				if len(tokenFreq.Locations) > 0 {
					locationBS.AddInt(int(docNum))
				}
				for _, loc := range tokenFreq.Locations {
					var locf = fieldID
					if loc.Field != "" {
						locf = uint16(s.getOrDefineField(loc.Field))
					}
					s.Locfields[fieldTermPostings-1] = append(s.Locfields[fieldTermPostings-1], locf)
					s.Locstarts[fieldTermPostings-1] = append(s.Locstarts[fieldTermPostings-1], uint64(loc.Start))
					s.Locends[fieldTermPostings-1] = append(s.Locends[fieldTermPostings-1], uint64(loc.End))
					s.Locpos[fieldTermPostings-1] = append(s.Locpos[fieldTermPostings-1], uint64(loc.Position))
					if len(loc.ArrayPositions) > 0 {
						s.Locarraypos[fieldTermPostings-1] = append(s.Locarraypos[fieldTermPostings-1], loc.ArrayPositions)
					} else {
						s.Locarraypos[fieldTermPostings-1] = append(s.Locarraypos[fieldTermPostings-1], nil)
					}
				}
			}
		}
	}
}

func (s *Segment) getOrDefineField(name string) int {
	fieldID, ok := s.FieldsMap[name]
	if !ok {
		fieldID = uint16(len(s.FieldsInv) + 1)
		s.FieldsMap[name] = fieldID
		s.FieldsInv = append(s.FieldsInv, name)
		s.Dicts = append(s.Dicts, make(map[string]uint64))
		s.DictKeys = append(s.DictKeys, make([]string, 0))
	}
	return int(fieldID - 1)
}

func (s *Segment) addDocument() int {
	docNum := len(s.Stored)
	s.Stored = append(s.Stored, map[uint16][][]byte{})
	s.StoredTypes = append(s.StoredTypes, map[uint16][]byte{})
	s.StoredPos = append(s.StoredPos, map[uint16][][]uint64{})
	return docNum
}

func encodeFieldType(f document.Field) byte {
	fieldType := byte('x')
	switch f.(type) {
	case *document.TextField:
		fieldType = 't'
	case *document.NumericField:
		fieldType = 'n'
	case *document.DateTimeField:
		fieldType = 'd'
	case *document.BooleanField:
		fieldType = 'b'
	case *document.GeoPointField:
		fieldType = 'g'
	case *document.CompositeField:
		fieldType = 'c'
	}
	return fieldType
}
