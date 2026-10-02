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

// perSegBase is what the composite per segment searchers (conjunction and
// disjunction) have in common.
type perSegBase struct {
	children []perSegChild
	scored   bool

	// wraps[i] is what children[i] was wrapped in before it was unwrapped
	// (nothing, mostly), outermost first
	wraps [][]wrapKind

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
		if f, ok := c.(blockFiller); ok {
			// a cursor that can fill a block by itself does it without a call for
			// each match
			n := f.fillBlock(blk)
			if err := c.Err(); err != nil {
				return 0, err
			}
			if n > 0 {
				blk.Seg = b.nextSeg - 1
				return n, nil
			}
			b.cursor = nil
			continue
		}
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

// blockFiller is a docCursor that can put its next matches in a block itself.
type blockFiller interface {
	// fillBlock puts the next matches, up to a block, in blk -- Docs, Scores,
	// Offset and MaxScore -- and returns how many there are, 0 once it is done.
	fillBlock(blk *search.PerSegmentScoredBlock) int
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

// unwrapSingle returns what a one clause disjunction or conjunction is: its
// clause. A one clause conjunction sums the one score, a one clause disjunction
// adds the one score and multiplies by a coord of 1 of 1, and both weigh what the
// clause weighs, so a query that is one of them is the clause, to the bit. The
// clause being a term matters: the algorithms that prune only work on terms, and a
// match of a single token, which is a query of one clause, is a term.
//
// A disjunction that wants more than one of its one clause matches nothing, so
// that is not unwrapped.
func unwrapSingle(c perSegChild) perSegChild {
	c, _ = unwrapSingleKinds(c)
	return c
}

// wrapKind is what a clause that was unwrapped was wrapped in.
type wrapKind uint8

const (
	wrapConjunction wrapKind = iota + 1
	wrapDisjunction
)

// unwrapSingleKinds is unwrapSingle that also says what was taken off, outermost
// first: scoring doesn't need the wrappers, but an explanation shows them, as the
// regular searchers' do.
func unwrapSingleKinds(c perSegChild) (perSegChild, []wrapKind) {
	var kinds []wrapKind
	for {
		switch w := c.(type) {
		case *PerSegmentDisjunctionSearcher:
			if len(w.children) == 1 && w.min <= 1 {
				// what its clause was wrapped in is under it
				kinds = append(append(kinds, wrapDisjunction), w.wrapsOf(0)...)
				c = w.children[0]
				continue
			}
		case *PerSegmentConjunctionSearcher:
			if len(w.children) == 1 {
				kinds = append(append(kinds, wrapConjunction), w.wrapsOf(0)...)
				c = w.children[0]
				continue
			}
		}
		return c, kinds
	}
}

// collectUnscoredGeneric collects a search without scores from cursors, one
// segment after the other. What is wanted is the first matches in doc order and,
// like the regular path does with them, it stops once it has them: the total is
// then a lower bound, as counting the rest takes walking all of it. A search for
// no hits at all is a count, and counts. makeCursor builds the cursor over the
// matches of a segment, nil if there are none.
func (b *perSegBase) collectUnscoredGeneric(ctx context.Context, sink search.PerSegmentSink,
	makeCursor func(seg int) docCursor) error {
	k := sink.Limit()
	for seg := 0; seg < b.segments(); seg++ {
		select {
		case <-ctx.Done():
			search.RecordSearchCost(ctx, search.AbortM, 0)
			return ctx.Err()
		default:
		}

		if k > 0 && heapIsFull(sink) {
			// the segments that follow have matches, or may have, that aren't counted
			sink.MarkPruned()
			return nil
		}
		c := makeCursor(seg)
		if c == nil {
			continue
		}
		if k == 0 {
			if err := drainCursor(ctx, sink, seg, c, false); err != nil {
				return err
			}
			continue
		}

		h := sink.Heap(seg)
		offset := c.Offset()
		var total uint64
		for c.Doc() != noMoreDocs && h.Len() < k {
			if total%checkDoneEvery == 0 {
				select {
				case <-ctx.Done():
					search.RecordSearchCost(ctx, search.AbortM, 0)
					return ctx.Err()
				default:
				}
			}
			h.Offer(search.PerSegmentHit{Doc: offset + uint64(c.Doc()), Ord: uint32(total), Seg: uint32(seg)})
			total++
			c.Advance()
		}
		if c.Doc() != noMoreDocs {
			sink.MarkPruned() // there is more, uncounted
		}
		sink.AddTotal(seg, total)
		if err := c.Err(); err != nil {
			return err
		}
	}
	return nil
}
