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

// accWindow is how many documents the accumulator sweeps at a time.
//
// Deliberately independent of search.BlockSize. Each window costs one pass over
// every clause to pick the base plus another to drain it, so a many-clause
// disjunction pays O(clauses) per window whether or not those clauses have
// anything in range: at 256 an 894-term wildcard over 200k documents spends
// ~1.4M peeks on that bookkeeping against ~950k that do real work. Widening the
// window divides the bookkeeping by the same factor.
//
// It cannot simply be search.BlockSize raised, because that also sizes the
// per-clause DocScoreBlock buffers - 894 clauses x 1024 x 4 arrays x 8 bytes
// would be 29MB. Lucene's BooleanScorer and tantivy's BufferedUnionScorer both
// keep one bucket array per scorer rather than per clause, which is what this
// split reproduces.
//
// 1024 rather than the 4096 those two use. A balanced sweep (5 ascending + 5
// descending repeats over 256..16384) put the workload total at 143.5ms at 256,
// 136.1 at 1024 and 133.5 at 16384, so 1024 takes three quarters of the win for
// a quarter of the footprint: 8KB acc + 4KB cnt + 128B matched, against 48.5KB
// at 4096.
//
// Past 4096 the curve also turns over on every accumulator shape except the
// 894-term wildcard - at 16384, numrange +8%, or-5-mid +5%, match-4 +3% - as
// acc/cnt stop sitting comfortably alongside the clause buffers. Only wildcard
// keeps improving, and it is 42% of the workload total, so optimising the total
// alone picks a window that is worse for everything else.
//
// Must be a power of two: the base is aligned with base &^= accWindow-1.
const accWindow = 1024

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

	// Resumable sweep state. A window is wider than the caller's output block,
	// so a dense window can produce more documents than fit. Rather than
	// shrinking the window we hand back a full block and resume the sweep on
	// the next call: base is the window in progress, sweepWord the next word of
	// matched to drain, and sweeping says whether a window is still open.
	base      uint64
	sweepWord int
	sweeping  bool
}

func newBlockDisjunction(searchers []search.Searcher, min, total int) *blockDisjunction {
	bd := &blockDisjunction{
		acc:     make([]float64, accWindow),
		cnt:     make([]int32, accWindow),
		matched: make([]uint64, accWindow/64),
		min:     min,
		total:   total,
	}
	if bd.min < 1 {
		bd.min = 1
	}
	for _, s := range searchers {
		bs, ok := s.(search.BulkSearcher)
		if !ok {
			// Callers reach this only by skipping CanScoreBlock, which has
			// already vetted every clause. Hand back what we took anyway
			// rather than leave the pool to drain on a path whose safety
			// depends on that discipline never lapsing.
			bd.release()
			return nil
		}
		bd.children = append(bd.children, &bulkChild{bs: bs, blk: search.GetDocScoreBlock()})
	}
	return bd
}

// release returns every child's block to the pool. Called from the owning
// searcher's Close; nothing may use the blockDisjunction afterwards, and the
// nil-ing makes a violation a panic rather than two queries quietly sharing a
// block.
func (bd *blockDisjunction) release() {
	if bd == nil {
		return
	}
	for _, c := range bd.children {
		search.PutDocScoreBlock(c.blk)
		c.blk = nil
	}
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
	capOut := len(out.IDs)
	if capOut == 0 {
		return 0, nil
	}

	for {
		if !bd.sweeping {
			// jump the window to the lowest pending document so empty stretches
			// of the doc space cost nothing
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
			base &^= uint64(accWindow - 1)
			end := base + accWindow

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

			bd.base = base
			bd.sweepWord = 0
			bd.sweeping = true
		}

		// Emit by draining the matched bitset. Words ascend and
		// TrailingZeros64 pops the lowest set bit first, so documents still come
		// out in ascending order; empty runs of up to 64 slots are skipped by a
		// single zero-word test.
		n := 0
		for bd.sweepWord < len(bd.matched) {
			word := bd.matched[bd.sweepWord]
			for word != 0 {
				if n == capOut {
					// output full mid-window: stash the bits still to drain and
					// resume here next call, leaving bd.sweeping set
					bd.matched[bd.sweepWord] = word
					return n, nil
				}
				tz := bits.TrailingZeros64(word)
				off := bd.sweepWord<<6 | tz
				c := bd.cnt[off]
				if int(c) >= bd.min {
					out.IDs[n] = bd.base + uint64(off)
					// same coord factor the scalar scorer applies
					out.Scores[n] = bd.acc[off] * float64(c) / float64(bd.total)
					n++
				}
				bd.acc[off] = 0
				bd.cnt[off] = 0
				word &^= 1 << uint(tz)
			}
			bd.matched[bd.sweepWord] = 0
			bd.sweepWord++
		}
		bd.sweeping = false

		if n > 0 {
			return n, nil
		}
		// the whole window fell below min: pick the next one
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
