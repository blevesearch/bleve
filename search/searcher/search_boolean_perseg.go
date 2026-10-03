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
	"fmt"
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
// It has the generic path only, NextMatch: like tantivy, which only prunes plain
// unions and intersections of terms, it doesn't prune once there are clauses
// with different roles. The clause that drives it (the required one, or without
// it the optional one) is read through, and the other clauses are sought about
// as often as it has matches, which is what each is told when it is asked for
// its cursor (segCursorSeeked) so that a clause of terms is read a window of docs
// at a time only when that pays.
type PerSegmentBooleanSearcher struct {
	perSegBase
	must, should, mustNot perSegChild
	// shouldRequired: the should clause has a minimum above 0, so the docs have
	// to be in it too. It is read from the clause as it was given, as a clause
	// of one that is unwrapped has lost its minimum.
	shouldRequired bool

	// what each clause was wrapped in before it was unwrapped, outermost first
	mustWraps, shouldWraps, mustNotWraps []wrapKind
}

var _ search.PerSegmentSearcher = (*PerSegmentBooleanSearcher)(nil)
var _ search.OptimizedPerSegmentSearcher = (*PerSegmentBooleanSearcher)(nil)
var _ perSegChild = (*PerSegmentBooleanSearcher)(nil)

// NewPerSegmentBooleanSearcher builds the boolean combination of the searchers,
// nil for the clauses there aren't, which have to be searchers of this package: it
// returns an error if one is not, or if there is neither a must nor a should clause
// to drive it. The searchers are closed by whoever has them if it does; if not,
// they are the boolean's, and closed with it.
func NewPerSegmentBooleanSearcher(must, should, mustNot search.PerSegmentSearcher,
	options search.SearcherOptions) (*PerSegmentBooleanSearcher, error) {
	rv := &PerSegmentBooleanSearcher{
		perSegBase: perSegBase{scored: options.Score != "none"},
	}
	for _, c := range []struct {
		in    search.PerSegmentSearcher
		out   *perSegChild
		wraps *[]wrapKind
	}{{must, &rv.must, &rv.mustWraps}, {should, &rv.should, &rv.shouldWraps}, {mustNot, &rv.mustNot, &rv.mustNotWraps}} {
		if c.in == nil {
			continue
		}
		child, ok := c.in.(perSegChild)
		if !ok {
			return nil, fmt.Errorf("searcher: %T can't be a clause of a per segment boolean", c.in)
		}
		if c.in == should {
			rv.shouldRequired = should.Min() > 0
		}
		child, *c.wraps = unwrapSingleKinds(child)
		*c.out = child
		rv.children = append(rv.children, child)
	}
	if rv.must == nil && rv.should == nil {
		return nil, fmt.Errorf("searcher: a per segment boolean needs a must or a should clause")
	}
	rv.computeQueryNorm()
	return rv, nil
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
	return s.segCursorSeeked(seg, scored, 0)
}

// segCost implements perSegChild: the matches of the clause that drives it.
func (s *PerSegmentBooleanSearcher) segCost(seg int) uint64 {
	if s.must != nil {
		return s.must.segCost(seg)
	}
	return s.should.segCost(seg)
}

// segCursorSeeked implements perSegChild. The clause that drives the cursor -- the
// required one, or without it the optional one -- is read through, whatever the
// consumer does; the other clauses are sought as many times as it has matches.
func (s *PerSegmentBooleanSearcher) segCursorSeeked(seg int, scored bool, seeks uint64) (docCursor, bool) {
	driven := s.segCost(seg)
	var must, should, mustNot docCursor
	if s.must != nil {
		c, ok := s.must.segCursorSeeked(seg, scored, 0)
		if !ok {
			return nil, false // nothing is a match without the required clause
		}
		must = c
	}
	if s.should != nil {
		hint := driven
		if s.must == nil {
			hint = 0 // it is the one that is read through
		}
		c, ok := s.should.segCursorSeeked(seg, scored, hint)
		if ok {
			should = c
		} else if s.must == nil || s.shouldRequired {
			return nil, false // nothing drives it, or it's required: and it isn't here
		}
	}
	if s.mustNot != nil {
		// the scores of what is excluded don't matter
		if c, ok := s.mustNot.segCursorSeeked(seg, false, driven); ok {
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

// NextMatch implements search.PerSegmentSearcher.
func (s *PerSegmentBooleanSearcher) NextMatch() (search.PerSegmentMatch, bool, error) {
	return s.nextMatch(func(seg int) docCursor {
		if c, ok := s.segCursor(seg, s.scored); ok {
			return c
		}
		return nil
	})
}
