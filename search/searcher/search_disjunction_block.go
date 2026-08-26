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
	"math/bits"

	"github.com/blevesearch/bleve/v2/search"
)

// A K-way disjunction merged through a heap costs O(log K) per posting, and
// measurement put that at ~13ns more per posting for every doubling of K — 38
// ns/posting at K=1 rising to 141 ns/posting at K=256. Since prefix, wildcard,
// fuzzy and numeric-range queries all become large disjunctions once their terms
// are enumerated, that growth term is most of their cost.
//
// This is the accumulator alternative: sweep the doc space in windows, add each
// clause's contribution into a dense array indexed by offset within the window,
// then walk the window once. O(P + D) with no heap and no dependence on K.
//
// The window is advanced to the lowest pending document rather than marched
// through the whole doc space, and only the touched span of each window is
// swept, so sparse clauses do not pay for empty regions.

// bulkChild buffers one child searcher's block output so the accumulator can
// consume it document by document.
type bulkChild struct {
	bs   search.BulkSearcher
	blk  *search.DocScoreBlock
	n    int
	pos  int
	done bool
}

func (c *bulkChild) fill() error {
	if c.done {
		return nil
	}
	n, err := c.bs.ScoreBlock(c.blk)
	if err != nil {
		return err
	}
	c.n, c.pos = n, 0
	if n == 0 {
		c.done = true
	}
	return nil
}

// peek returns the child's current document without consuming it.
func (c *bulkChild) peek() (uint64, float64, bool, error) {
	for c.pos >= c.n {
		if c.done {
			return 0, 0, false, nil
		}
		if err := c.fill(); err != nil {
			return 0, 0, false, err
		}
	}
	return c.blk.IDs[c.pos], c.blk.Scores[c.pos], true, nil
}

func (c *bulkChild) advance() { c.pos++ }

// blockDisjunction accumulates child contributions over a sliding window.
type blockDisjunction struct {
	children []*bulkChild
	acc      []float64
	cnt      []int32
	// matched has one bit per window slot, set when a clause contributes to it.
	// The emit sweep walks these words and pops set bits, so it costs O(matches)
	// rather than O(span between the lowest and highest match) — a window with
	// three documents 200 slots apart visits three slots, not 200. Lucene's
	// BooleanScorer (FixedBitSet matching + Long.numberOfTrailingZeros) and
	// tantivy's BufferedUnionScorer (TinySet bitsets + pop_lowest) both do this.
	matched []uint64
	min     int
	total   int
}

func newBlockDisjunction(searchers []search.Searcher, min, total int) *blockDisjunction {
	bd := &blockDisjunction{
		acc:     make([]float64, search.BlockSize),
		cnt:     make([]int32, search.BlockSize),
		matched: make([]uint64, (search.BlockSize+63)/64),
		min:     min,
		total:   total,
	}
	if bd.min < 1 {
		bd.min = 1
	}
	for _, s := range searchers {
		bs, ok := s.(search.BulkSearcher)
		if !ok {
			return nil
		}
		bd.children = append(bd.children, &bulkChild{bs: bs, blk: search.NewDocScoreBlock()})
	}
	return bd
}

// canBlockDisjunct reports whether every clause can produce blocks.
func canBlockDisjunct(searchers []search.Searcher) bool {
	if len(searchers) == 0 {
		return false
	}
	for _, s := range searchers {
		bs, ok := s.(search.BulkSearcher)
		if !ok || !bs.CanScoreBlock() {
			return false
		}
	}
	return true
}

func (bd *blockDisjunction) scoreBlock(out *search.DocScoreBlock) (int, error) {
	const w = search.BlockSize

	for {
		// jump the window to the lowest pending document so empty stretches of
		// the doc space cost nothing
		var base uint64
		found := false
		for _, c := range bd.children {
			d, _, ok, err := c.peek()
			if err != nil {
				return 0, err
			}
			if ok && (!found || d < base) {
				base, found = d, true
			}
		}
		if !found {
			return 0, nil
		}
		base &^= uint64(w - 1)
		end := base + w

		for _, c := range bd.children {
			for {
				d, sc, ok, err := c.peek()
				if err != nil {
					return 0, err
				}
				if !ok || d >= end {
					break
				}
				off := int(d - base)
				bd.acc[off] += sc
				bd.cnt[off]++
				bd.matched[off>>6] |= 1 << uint(off&63)
				c.advance()
			}
		}

		// Emit by draining the matched bitset. Words ascend and
		// TrailingZeros64 pops the lowest set bit first, so documents still come
		// out in ascending order; empty runs of up to 64 slots are skipped by a
		// single zero-word test.
		n := 0
		for wi := range bd.matched {
			word := bd.matched[wi]
			if word == 0 {
				continue
			}
			for word != 0 {
				tz := bits.TrailingZeros64(word)
				off := wi<<6 | tz
				c := bd.cnt[off]
				if int(c) >= bd.min {
					out.IDs[n] = base + uint64(off)
					// same coord factor the scalar scorer applies
					out.Scores[n] = bd.acc[off] * float64(c) / float64(bd.total)
					n++
				}
				bd.acc[off] = 0
				bd.cnt[off] = 0
				word &^= 1 << uint(tz)
			}
			bd.matched[wi] = 0
		}
		if n > 0 {
			return n, nil
		}
	}
}

// ---------------------------------------------------------------- slice searcher

func (s *DisjunctionSliceSearcher) CanScoreBlock() bool {
	return !s.retrieveScoreBreakdown && canBlockDisjunct(s.searchers)
}

func (s *DisjunctionSliceSearcher) ScoreBlock(out *search.DocScoreBlock) (int, error) {
	if s.blockDisj == nil {
		s.blockDisj = newBlockDisjunction(s.searchers, s.min, s.numSearchers)
		if s.blockDisj == nil {
			return 0, nil
		}
	}
	return s.blockDisj.scoreBlock(out)
}

// ---------------------------------------------------------------- heap searcher

func (s *DisjunctionHeapSearcher) CanScoreBlock() bool {
	return !s.retrieveScoreBreakdown && canBlockDisjunct(s.searchers)
}

func (s *DisjunctionHeapSearcher) ScoreBlock(out *search.DocScoreBlock) (int, error) {
	if s.blockDisj == nil {
		s.blockDisj = newBlockDisjunction(s.searchers, s.min, s.numSearchers)
		if s.blockDisj == nil {
			return 0, nil
		}
	}
	return s.blockDisj.scoreBlock(out)
}

var (
	_ search.BulkSearcher = (*DisjunctionSliceSearcher)(nil)
	_ search.BulkSearcher = (*DisjunctionHeapSearcher)(nil)
)
