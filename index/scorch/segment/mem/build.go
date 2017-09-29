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
	s.getOrDefineField("_id", false)

	// walk each doc
	for _, result := range results {
		s.processDocument(result)
	}

	// go back and sort the dictKeys
	for _, dict := range s.dictKeys {
		sort.Strings(dict)
	}

	// professional debugging
	//
	// log.Printf("fields: %v\n", s.fields)
	// log.Printf("fieldsInv: %v\n", s.fieldsInv)
	// log.Printf("fieldsLoc: %v\n", s.fieldsLoc)
	// log.Printf("dicts: %v\n", s.dicts)
	// log.Printf("dict keys: %v\n", s.dictKeys)
	// for i, posting := range s.postings {
	// 	log.Printf("posting %d: %v\n", i, posting)
	// }
	// for i, freq := range s.freqs {
	// 	log.Printf("freq %d: %v\n", i, freq)
	// }
	// for i, norm := range s.norms {
	// 	log.Printf("norm %d: %v\n", i, norm)
	// }
	// for i, field := range s.locfields {
	// 	log.Printf("field %d: %v\n", i, field)
	// }
	// for i, start := range s.locstarts {
	// 	log.Printf("start %d: %v\n", i, start)
	// }
	// for i, end := range s.locends {
	// 	log.Printf("end %d: %v\n", i, end)
	// }
	// for i, pos := range s.locpos {
	// 	log.Printf("pos %d: %v\n", i, pos)
	// }
	// for i, apos := range s.locarraypos {
	// 	log.Printf("apos %d: %v\n", i, apos)
	// }
	// log.Printf("stored: %v\n", s.stored)
	// log.Printf("stored types: %v\n", s.storedTypes)
	// log.Printf("stored pos: %v\n", s.storedPos)

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
		s.stored[docNum][field] = append(s.stored[docNum][field], val)
		s.storedTypes[docNum][field] = append(s.storedTypes[docNum][field], typ)
		s.storedPos[docNum][field] = append(s.storedPos[docNum][field], pos)
	}

	// walk each composite field
	for _, field := range result.Document.CompositeFields {
		fieldID := uint16(s.getOrDefineField(field.Name(), false))
		l, tf := field.Analyze()
		processField(fieldID, field.Name(), l, tf)
	}

	// walk each field
	for i, field := range result.Document.Fields {
		fieldID := uint16(s.getOrDefineField(field.Name(), field.Options().IncludeTermVectors()))
		l := result.Length[i]
		tf := result.Analyzed[i]
		processField(fieldID, field.Name(), l, tf)
		if field.Options().IsStored() {
			storeField(docNum, fieldID, encodeFieldType(field), field.Value(), field.ArrayPositions())
		}
	}

	// now that its been rolled up into docMap, walk that
	for fieldID, tokenFrequencies := range docMap {
		for term, tokenFreq := range tokenFrequencies {
			fieldTermPostings := s.dicts[fieldID][term]

			// FIXME this if/else block has duplicate code that has resulted in
			// bugs fixed/missed more than once, need to refactor
			if fieldTermPostings == 0 {
				// need to build new posting
				bs := roaring.New()
				bs.AddInt(int(docNum))

				newPostingID := uint64(len(s.postings) + 1)
				// add this new bitset to the postings slice
				s.postings = append(s.postings, bs)
				// add this to the details slice
				s.freqs = append(s.freqs, []uint64{uint64(tokenFreq.Frequency())})
				s.norms = append(s.norms, []float32{float32(1.0 / math.Sqrt(float64(fieldLens[fieldID])))})
				// add to locations
				var locfields []uint16
				var locstarts []uint64
				var locends []uint64
				var locpos []uint64
				var locarraypos [][]uint64
				for _, loc := range tokenFreq.Locations {
					var locf = fieldID
					if loc.Field != "" {
						locf = uint16(s.getOrDefineField(loc.Field, false))
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
				s.locfields = append(s.locfields, locfields)
				s.locstarts = append(s.locstarts, locstarts)
				s.locends = append(s.locends, locends)
				s.locpos = append(s.locpos, locpos)
				s.locarraypos = append(s.locarraypos, locarraypos)
				// record it
				s.dicts[fieldID][term] = newPostingID
				// this term was new for this field, add it to dictKeys
				s.dictKeys[fieldID] = append(s.dictKeys[fieldID], term)
			} else {
				// posting already started for this field/term
				// the actual offset is - 1, because 0 is zero value
				bs := s.postings[fieldTermPostings-1]
				bs.AddInt(int(docNum))
				s.freqs[fieldTermPostings-1] = append(s.freqs[fieldTermPostings-1], uint64(tokenFreq.Frequency()))
				s.norms[fieldTermPostings-1] = append(s.norms[fieldTermPostings-1], float32(1.0/math.Sqrt(float64(fieldLens[fieldID]))))
				for _, loc := range tokenFreq.Locations {
					var locf = fieldID
					if loc.Field != "" {
						locf = uint16(s.getOrDefineField(loc.Field, false))
					}
					s.locfields[fieldTermPostings-1] = append(s.locfields[fieldTermPostings-1], locf)
					s.locstarts[fieldTermPostings-1] = append(s.locstarts[fieldTermPostings-1], uint64(loc.Start))
					s.locends[fieldTermPostings-1] = append(s.locends[fieldTermPostings-1], uint64(loc.End))
					s.locpos[fieldTermPostings-1] = append(s.locpos[fieldTermPostings-1], uint64(loc.Position))
					if len(loc.ArrayPositions) > 0 {
						s.locarraypos[fieldTermPostings-1] = append(s.locarraypos[fieldTermPostings-1], loc.ArrayPositions)
					} else {
						s.locarraypos[fieldTermPostings-1] = append(s.locarraypos[fieldTermPostings-1], nil)
					}
				}
			}
		}
	}
}

func (s *Segment) getOrDefineField(name string, hasLoc bool) int {
	fieldID, ok := s.fields[name]
	if !ok {
		fieldID = uint16(len(s.fieldsInv) + 1)
		s.fields[name] = fieldID
		s.fieldsInv = append(s.fieldsInv, name)
		s.fieldsLoc = append(s.fieldsLoc, hasLoc)
		s.dicts = append(s.dicts, make(map[string]uint64))
		s.dictKeys = append(s.dictKeys, make([]string, 0))
	}
	return int(fieldID - 1)
}

func (s *Segment) addDocument() int {
	docNum := len(s.stored)
	s.stored = append(s.stored, map[uint16][][]byte{})
	s.storedTypes = append(s.storedTypes, map[uint16][]byte{})
	s.storedPos = append(s.storedPos, map[uint16][][]uint64{})
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
