//  Copyright (c) 2026 Couchbase, Inc.
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

package searcher

import (
	"context"
	"math"
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
)

var reflectStaticSizePerSegmentBooleanSearcher int

func init() {
	var s PerSegmentBooleanSearcher
	reflectStaticSizePerSegmentBooleanSearcher = int(reflect.TypeOf(s).Size())
}

// PerSegmentBooleanSearcher is the boolean combination of per segment searchers:
// a must clause, a should clause and a clause to exclude, any of them optional
// but for one of the first two. It is BooleanSearcher's semantics, see
// booleanCursor.
//
// It has the generic path only, NextBlock: like tantivy, which only prunes plain
// unions and intersections of terms, it doesn't prune once there are clauses
// with different roles.
type PerSegmentBooleanSearcher struct {
	perSegBase
	must, should, mustNot perSegChild
	// shouldRequired: the should clause has a minimum above 0, so the docs have
	// to be in it too. It is read from the clause as it was given, as a clause
	// of one that is unwrapped has lost its minimum.
	shouldRequired bool
}

var _ search.PerSegmentSearcher = (*PerSegmentBooleanSearcher)(nil)
var _ search.OptimizedPerSegmentSearcher = (*PerSegmentBooleanSearcher)(nil)
var _ perSegChild = (*PerSegmentBooleanSearcher)(nil)

// NewPerSegmentBooleanSearcher builds the boolean combination of the searchers,
// nil for the clauses there aren't, or returns nil if one of them is not a per
// segment searcher, in which case the regular NewBooleanSearcher is what has to
// be used (and the searchers have to be closed by whoever has them).
func NewPerSegmentBooleanSearcher(must, should, mustNot search.Searcher,
	options search.SearcherOptions) *PerSegmentBooleanSearcher {
	rv := &PerSegmentBooleanSearcher{
		perSegBase: perSegBase{scored: options.Score != "none"},
	}
	for _, c := range []struct {
		in  search.Searcher
		out *perSegChild
	}{{must, &rv.must}, {should, &rv.should}, {mustNot, &rv.mustNot}} {
		if c.in == nil {
			continue
		}
		child, ok := c.in.(perSegChild)
		if !ok {
			return nil
		}
		if c.in == should {
			rv.shouldRequired = should.Min() > 0
		}
		child = unwrapSingle(child)
		*c.out = child
		rv.children = append(rv.children, child)
	}
	if rv.must == nil && rv.should == nil {
		return nil // nothing to drive it
	}
	rv.computeQueryNorm()
	return rv
}

// computeQueryNorm is BooleanSearcher's: the clauses that score tell the query
// norm, the excluding one is left out.
func (s *PerSegmentBooleanSearcher) computeQueryNorm() {
	s.SetQueryNorm(1.0 / math.Sqrt(s.Weight()))
}

func (s *PerSegmentBooleanSearcher) Weight() float64 {
	var rv float64
	if s.must != nil {
		rv += s.must.Weight()
	}
	if s.should != nil {
		rv += s.should.Weight()
	}
	return rv
}

func (s *PerSegmentBooleanSearcher) SetQueryNorm(qnorm float64) {
	if s.must != nil {
		s.must.SetQueryNorm(qnorm)
	}
	if s.should != nil {
		s.should.SetQueryNorm(qnorm)
	}
}

func (s *PerSegmentBooleanSearcher) Size() int {
	sizeInBytes := reflectStaticSizePerSegmentBooleanSearcher + size.SizeOfPtr
	for _, c := range s.children {
		sizeInBytes += c.Size()
	}
	return sizeInBytes
}

// Count is a worst case, as BooleanSearcher's is.
func (s *PerSegmentBooleanSearcher) Count() uint64 {
	var sum uint64
	if s.must != nil {
		sum += s.must.Count()
	}
	if s.should != nil {
		sum += s.should.Count()
	}
	return sum
}

func (s *PerSegmentBooleanSearcher) Min() int { return 0 }

func (s *PerSegmentBooleanSearcher) numSegments() int { return s.segments() }

// segCursor implements perSegChild.
func (s *PerSegmentBooleanSearcher) segCursor(seg int, scored bool) (docCursor, bool) {
	var must, should, mustNot docCursor
	if s.must != nil {
		c, ok := s.must.segCursor(seg, scored)
		if !ok {
			return nil, false // nothing is a match without the required clause
		}
		must = c
	}
	if s.should != nil {
		c, ok := s.should.segCursor(seg, scored)
		if ok {
			should = c
		} else if s.must == nil || s.shouldRequired {
			return nil, false // nothing drives it, or it's required: and it isn't here
		}
	}
	if s.mustNot != nil {
		// the scores of what is excluded don't matter
		if c, ok := s.mustNot.segCursor(seg, false); ok {
			mustNot = c
		}
	}
	optional := s.should == nil || !s.shouldRequired
	return newBooleanCursor(must, should, mustNot, optional), true
}

// CanCollectOptimized implements search.OptimizedPerSegmentSearcher. With scores
// every match is visited, like tantivy does; without, it's enough to stop at the
// first matches.
func (s *PerSegmentBooleanSearcher) CanCollectOptimized() bool { return !s.scored }

// CollectOptimized implements search.OptimizedPerSegmentSearcher.
func (s *PerSegmentBooleanSearcher) CollectOptimized(ctx context.Context, sink search.PerSegmentSink) error {
	return s.collectUnscoredGeneric(ctx, sink, func(seg int) docCursor {
		if c, ok := s.segCursor(seg, false); ok {
			return c
		}
		return nil
	})
}

// NextBlock implements search.PerSegmentSearcher.
func (s *PerSegmentBooleanSearcher) NextBlock(blk *search.PerSegmentScoredBlock) (int, error) {
	return s.nextBlock(blk, func(seg int) docCursor {
		if c, ok := s.segCursor(seg, s.scored); ok {
			return c
		}
		return nil
	})
}
