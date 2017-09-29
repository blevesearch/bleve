package mem

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/index/scorch/segment"
)

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

	// fields name -> id+1
	fields map[string]uint16
	// fields id -> name
	fieldsInv []string
	// field id -> has location info
	fieldsLoc []bool

	// term dictionary
	//  field id -> term -> posting id + 1
	dicts []map[string]uint64

	// term dictionary keys
	//  field id -> []dictionary keys
	dictKeys [][]string

	// postings list
	//  postings list id -> postings bitmap
	postings []*roaring.Bitmap

	// term frequencies
	//  postings list id -> freqs (one for each hit in bitmap)
	freqs [][]uint64

	// field norms
	//  postings list id -> norms (one for each hit in bitmap)
	norms [][]float32

	// field/start/end/pos/locarraypos
	//  postings list id -> start/end/pos/locarraypos (one for each freq)
	locfields   [][]uint16
	locstarts   [][]uint64
	locends     [][]uint64
	locpos      [][]uint64
	locarraypos [][][]uint64

	// stored field values
	//  docNum -> field id -> slice of values (each value []byte)
	stored []map[uint16][][]byte

	// stored field types
	//  docNum -> field id -> slice of types (each type byte)
	storedTypes []map[uint16][]byte

	// stored field array positions
	//  docNum -> field id -> slice of array positions (each is []uint64)
	storedPos []map[uint16][][]uint64
}

// New builds a new empty Segment
func New() *Segment {
	return &Segment{
		fields: map[string]uint16{},
	}
}

// Fields returns the field names used in this segment
func (s *Segment) Fields() []string {
	return s.fieldsInv
}

// VisitDocument invokes the DocFieldValueVistor for each stored field
// for the specified doc number
func (s *Segment) VisitDocument(num uint64, visitor segment.DocumentFieldValueVisitor) error {
	// ensure document number exists
	if int(num) > len(s.stored)-1 {
		return nil
	}
	docFields := s.stored[int(num)]
	for field, values := range docFields {
		for i, value := range values {
			keepGoing := visitor(s.fieldsInv[field], s.storedTypes[int(num)][field][i], value, s.storedPos[int(num)][field][i])
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
	return uint64(len(s.stored))
}

// DocNumbers returns a bitset corresponding to the doc numbers of all the
// provided _id strings
func (s *Segment) DocNumbers(ids []string) *roaring.Bitmap {

	idDictionary := s.dicts[s.getOrDefineField("_id", false)]
	rv := roaring.New()
	for _, id := range ids {
		postingID := idDictionary[id]
		if postingID > 0 {
			rv.Or(s.postings[postingID-1])
		}
	}
	return rv
}
