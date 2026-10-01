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

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// perSegChild is a clause of a composite per segment searcher: a term, or a
// composite itself.
type perSegChild interface {
	search.Searcher

	// segCursor is the cursor over the matches of the clause in segment seg,
	// and false if there are none there. A cursor can only be had once.
	segCursor(seg int, scored bool) (docCursor, bool)

	// numSegments is how many segments the clause has readers for: all of
	// the index's, or none if it matches nothing anywhere.
	numSegments() int
}

// numSegments implements perSegChild.
func (s *PerSegmentTermSearcher) numSegments() int { return len(s.readers) }

// segCursor implements perSegChild.
func (s *PerSegmentTermSearcher) segCursor(seg int, scored bool) (docCursor, bool) {
	if seg >= len(s.readers) || s.readers[seg] == nil {
		return nil, false
	}
	return newTermCursor(0, s.readers[seg], s.scorer, scored), true
}

// CountPerSegmentSearchers says how many of the searchers are per segment
// searchers that a composite can be made of.
func CountPerSegmentSearchers(qsearchers []search.Searcher) int {
	n := 0
	for _, q := range qsearchers {
		if _, ok := q.(perSegChild); ok {
			n++
		}
	}
	return n
}

// perSegBase is what the composite per segment searchers (conjunction and
// disjunction) have in common.
type perSegBase struct {
	children []perSegChild
	scored   bool

	// the generic iteration (NextBlock): the segment it is in, and the cursor
	// over its matches
	nextSeg int
	cursor  docCursor
}

// segments is how many segments the index has. A clause that matches in none
// of them has no readers, so it's the highest count among the clauses that
// is the answer.
func (b *perSegBase) segments() int {
	n := 0
	for _, c := range b.children {
		n = max(n, c.numSegments())
	}
	return n
}

// computeQueryNorm does what the regular conjunction and disjunction
// searchers do: the clauses are told the norm of the whole query.
func (b *perSegBase) computeQueryNorm() {
	var sumOfSquaredWeights float64
	for _, c := range b.children {
		sumOfSquaredWeights += c.Weight()
	}
	b.SetQueryNorm(1.0 / math.Sqrt(sumOfSquaredWeights))
}

func (b *perSegBase) Weight() float64 {
	var rv float64
	for _, c := range b.children {
		rv += c.Weight()
	}
	return rv
}

func (b *perSegBase) SetQueryNorm(qnorm float64) {
	for _, c := range b.children {
		c.SetQueryNorm(qnorm)
	}
}

func (b *perSegBase) Close() error {
	var rv error
	for _, c := range b.children {
		if err := c.Close(); err != nil && rv == nil {
			rv = err
		}
	}
	return rv
}

func (b *perSegBase) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	return nil, ErrPerSegmentSearcherIterated
}

func (b *perSegBase) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	return nil, ErrPerSegmentSearcherIterated
}

func (b *perSegBase) DocumentMatchPoolSize() int { return 0 }

// nextBlock is the generic iteration: the matches of each segment, one segment
// after the other, a block of them at a time. makeCursor builds the cursor
// over the matches of a segment, nil if there are none.
func (b *perSegBase) nextBlock(blk *search.PerSegmentScoredBlock,
	makeCursor func(seg int) docCursor) (int, error) {
	for {
		if b.cursor == nil {
			if b.nextSeg >= b.segments() {
				return 0, nil
			}
			b.cursor = makeCursor(b.nextSeg)
			b.nextSeg++
			if b.cursor == nil {
				continue
			}
		}

		c := b.cursor
		n := 0
		var max float32
		for n < search.PerSegmentBlockLen && c.Doc() != noMoreDocs {
			blk.Docs[n] = c.Doc()
			var score float32
			if b.scored {
				score = c.Score()
			}
			blk.Scores[n] = score
			if score > max {
				max = score
			}
			n++
			c.Advance()
		}
		if err := c.Err(); err != nil {
			return 0, err
		}
		if n > 0 {
			blk.Seg = b.nextSeg - 1
			blk.Offset = c.Offset()
			blk.MaxScore = max
			return n, nil
		}
		b.cursor = nil
	}
}

// drainCursor offers every match of a segment to the sink: what's done when
// there is nothing to prune with. Every match is visited and counted.
func drainCursor(ctx context.Context, sink search.PerSegmentSink, seg int, c docCursor, scored bool) error {
	h := sink.Heap(seg)
	offset := c.Offset()
	var visited uint64
	thr, full := h.Threshold()
	var maxScore float32
	for c.Doc() != noMoreDocs {
		if visited%checkDoneEvery == 0 {
			select {
			case <-ctx.Done():
				search.RecordSearchCost(ctx, search.AbortM, 0)
				return ctx.Err()
			default:
			}
		}
		var score float32
		if scored {
			score = c.Score()
			if score > maxScore {
				maxScore = score
			}
		}
		if !full || score > thr {
			h.Offer(search.PerSegmentHit{Score: score, Doc: offset + uint64(c.Doc()), Ord: uint32(visited), Seg: uint32(seg)})
			thr, full = h.Threshold()
		}
		visited++
		c.Advance()
	}
	sink.AddTotal(seg, visited)
	sink.ObserveMaxScore(maxScore)
	return c.Err()
}

// thresholdOf is the score that a hit has to beat to make the heap: -Inf as long
// as the heap isn't full.
func thresholdOf(sink search.PerSegmentSink) float32 {
	if thr, full := sink.Threshold(); full {
		return thr
	}
	return float32(math.Inf(-1))
}
