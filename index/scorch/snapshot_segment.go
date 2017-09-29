package scorch

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/index/scorch/segment"
)

type SegmentDictionarySnapshot struct {
	s *SegmentSnapshot
	d segment.TermDictionary
}

func (s *SegmentDictionarySnapshot) PostingsList(term string, except *roaring.Bitmap) segment.PostingsList {
	return s.d.PostingsList(term, s.s.deleted)
}

func (s *SegmentDictionarySnapshot) Iterator() segment.DictionaryIterator {
	return s.d.Iterator()
}

func (s *SegmentDictionarySnapshot) PrefixIterator(prefix string) segment.DictionaryIterator {
	return s.d.PrefixIterator(prefix)
}

func (s *SegmentDictionarySnapshot) RangeIterator(start, end string) segment.DictionaryIterator {
	return s.d.RangeIterator(start, end)
}

type SegmentSnapshot struct {
	id      uint64
	segment segment.Segment
	deleted *roaring.Bitmap
}

func (s *SegmentSnapshot) VisitDocument(num uint64, visitor segment.DocumentFieldValueVisitor) error {
	return s.segment.VisitDocument(num, visitor)
}

func (s *SegmentSnapshot) Count() uint64 {
	rv := s.segment.Count()
	if s.deleted != nil {
		rv -= s.deleted.GetCardinality()
	}
	return rv
}

func (s *SegmentSnapshot) Dictionary(field string) segment.TermDictionary {
	return &SegmentDictionarySnapshot{
		s: s,
		d: s.segment.Dictionary(field),
	}
}

func (s *SegmentSnapshot) DocNumbers(docIDs []string) *roaring.Bitmap {
	rv := s.segment.DocNumbers(docIDs)
	if s.deleted != nil {
		rv.AndNot(s.deleted)
	}
	return rv
}

func (s *SegmentSnapshot) Fields() []string {
	return s.segment.Fields()
}
