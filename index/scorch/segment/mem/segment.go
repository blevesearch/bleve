package mem

import (
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
// - need tests for term dictionary iteration

// Segment is an in memory implementation of scorch.Segment
type Segment struct {

	// FieldsMap name -> id+1
	FieldsMap map[string]uint16
	// fields id -> name
	FieldsInv []string
	// field id -> has location info
	FieldsLoc []bool

	// term dictionary
	//  field id -> term -> posting id + 1
	Dicts []map[string]uint64

	// term dictionary keys
	//  field id -> []dictionary keys
	DictKeys [][]string

	// Postings list
	//  Postings list id -> Postings bitmap
	Postings []*roaring.Bitmap

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
}

// New builds a new empty Segment
func New() *Segment {
	return &Segment{
		FieldsMap: map[string]uint16{},
	}
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

// Dictionary returns the term dictionary for the specified field
func (s *Segment) Dictionary(field string) segment.TermDictionary {
	return &Dictionary{
		segment: s,
		field:   field,
		fieldID: uint16(s.getOrDefineField(field, false)),
	}
}

// Count returns the number of documents in this segment
// (this has no notion of deleted docs)
func (s *Segment) Count() uint64 {
	return uint64(len(s.Stored))
}

// DocNumbers returns a bitset corresponding to the doc numbers of all the
// provided _id strings
func (s *Segment) DocNumbers(ids []string) *roaring.Bitmap {

	idDictionary := s.Dicts[idFieldID]
	rv := roaring.New()
	for _, id := range ids {
		postingID := idDictionary[id]
		if postingID > 0 {
			rv.Or(s.Postings[postingID-1])
		}
	}
	return rv
}
