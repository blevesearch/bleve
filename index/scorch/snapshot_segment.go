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

package scorch

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/scorch/segment"
)

type SegmentDictionarySnapshot struct {
	s *SegmentSnapshot
	d segment.TermDictionary
}

func (s *SegmentDictionarySnapshot) PostingsList(term string, except *roaring.Bitmap) (segment.PostingsList, error) {
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

	notify []chan error
}

func (s *SegmentSnapshot) Id() uint64 {
	return s.id
}

func (s *SegmentSnapshot) FullSize() int64 {
	return int64(s.segment.Count())
}

func (s SegmentSnapshot) LiveSize() int64 {
	return int64(s.Count())
}

func (s *SegmentSnapshot) Close() error {
	return s.segment.Close()
}

func (s *SegmentSnapshot) VisitDocument(num uint64, visitor segment.DocumentFieldValueVisitor) error {
	return s.segment.VisitDocument(num, visitor)
}

func (s *SegmentSnapshot) DocumentVisitFieldTerms(num uint64, fields []string,
	visitor index.DocumentFieldTermVisitor) error {
	collection := make(map[string][][]byte)
	// collect field indexed values
	for _, field := range fields {
		dict, err := s.Dictionary(field)
		if err != nil {
			return err
		}
		dictItr := dict.Iterator()
		var next *index.DictEntry
		next, err = dictItr.Next()
		for next != nil && err == nil {
			postings, err2 := dict.PostingsList(next.Term, nil)
			if err2 != nil {
				return err2
			}
			postingsItr := postings.Iterator()
			nextPosting, err2 := postingsItr.Next()
			for err2 == nil && nextPosting != nil && nextPosting.Number() <= num {
				if nextPosting.Number() == num {
					// got what we're looking for
					collection[field] = append(collection[field], []byte(next.Term))
				}
				nextPosting, err = postingsItr.Next()
			}
			if err2 != nil {
				return err
			}
			next, err = dictItr.Next()
		}
		if err != nil {
			return err
		}
	}
	// invoke callback
	for field, values := range collection {
		for _, value := range values {
			visitor(field, value)
		}
	}
	return nil
}

func (s *SegmentSnapshot) Count() uint64 {

	rv := s.segment.Count()
	if s.deleted != nil {
		rv -= s.deleted.GetCardinality()
	}
	return rv
}

func (s *SegmentSnapshot) Dictionary(field string) (segment.TermDictionary, error) {
	d, err := s.segment.Dictionary(field)
	if err != nil {
		return nil, err
	}
	return &SegmentDictionarySnapshot{
		s: s,
		d: d,
	}, nil
}

func (s *SegmentSnapshot) DocNumbers(docIDs []string) (*roaring.Bitmap, error) {
	rv, err := s.segment.DocNumbers(docIDs)
	if err != nil {
		return nil, err
	}
	if s.deleted != nil {
		rv.AndNot(s.deleted)
	}
	return rv, nil
}

// DocNumbersLive returns bitsit containing doc numbers for all live docs
func (s *SegmentSnapshot) DocNumbersLive() *roaring.Bitmap {
	rv := roaring.NewBitmap()
	rv.AddRange(0, s.segment.Count())
	if s.deleted != nil {
		rv.AndNot(s.deleted)
	}
	return rv
}

func (s *SegmentSnapshot) Fields() []string {
	return s.segment.Fields()
}
