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

// bufferedIntersectionCursor is the matches of an AND of terms that have about as
// many postings as each other, found a window of docs at a time: each term puts
// its postings that are in the window in a bitmap, and its scores in the slots of
// their docs; the bitmaps are ANDed, and the matches are the bits that are left,
// taken lowest first. Where the terms are alike, that is less work than moving
// the cursor of each from match to match, which is what intersectionCursor does,
// and what is best when one term has far fewer postings than the others.
//
// The terms are visited in the order of the query, so the score of a doc is summed
// in that order, and is intersectionCursor's, to the bit.
//
// Like bufferedUnionCursor, it falls back to the cursor it stands for (the
// intersectionCursor over the same terms) when its consumer seeks beyond the
// window, and comes back after a run of Advance calls.
type bufferedIntersectionCursor struct {
	terms  []*termCursor // in the order of the query
	scored bool

	fb       docCursor
	advances int

	scratch  *bufIntersectScratch // nil in fallback, and once released
	start    uint32               // the first doc of the window
	span     uint32               // how many docs the next window covers, up to windowDocs
	nw       int                  // the words of the bitmap the window uses
	word     int                  // the word of the bitmap being read
	bitsLeft uint64               // what's left of it
	off      int                  // where the doc the cursor is on is in the window
	doc      uint32
	offset   uint64
}

// useBufferedIntersection reports whether the AND of the terms is done by windows:
// when no term has far more postings than the rest, as the windows pay for the
// postings of all of them and seeking only for those of the fewest.
func useBufferedIntersection(terms []*termCursor) bool {
	if len(terms) < 2 {
		return false
	}
	lo, hi := terms[0].Cost(), terms[0].Cost()
	for _, t := range terms[1:] {
		lo, hi = min(lo, t.Cost()), max(hi, t.Cost())
	}
	return intersectsByWindowsCounts(lo, hi)
}

// newBufferedIntersectionCursor is the intersection of the terms, in the order of
// the query.
func newBufferedIntersectionCursor(terms []*termCursor, scored bool) *bufferedIntersectionCursor {
	x := &bufferedIntersectionCursor{
		terms:   terms,
		scored:  scored,
		scratch: bufIntersectScratchPool.Get().(*bufIntersectScratch),
		offset:  terms[0].Offset(),
		off:     -1,
		span:    windowDocs,
	}
	x.doc = x.refill()
	return x
}

// release implements releaser. The memory is cleaned where it is used, so it goes
// back as it is.
func (x *bufferedIntersectionCursor) release() {
	if x.scratch != nil {
		bufIntersectScratchPool.Put(x.scratch)
		x.scratch = nil
	}
}

// finish is where the cursor is when it has no more matches.
func (x *bufferedIntersectionCursor) finish() uint32 {
	x.doc = noMoreDocs
	x.release()
	return noMoreDocs
}

// refill goes to the first window, from where the terms are, that has a match, and
// to the first match in it, which it returns (noMoreDocs if there are no more).
func (x *bufferedIntersectionCursor) refill() uint32 {
	s := x.scratch
	for {
		// nothing before the highest doc the terms are on can match
		start := uint32(0)
		for _, t := range x.terms {
			if t.doc == noMoreDocs {
				return x.finish()
			}
			start = max(start, t.doc)
		}
		end := uint32(noMoreDocs)
		if uint64(start)+uint64(x.span) < uint64(noMoreDocs) {
			end = start + x.span
		}
		// the window after this one covers more, up to a whole one
		x.span = min(x.span*2, windowDocs)
		nw := int(end-start+63) / 64

		if x.scored {
			clear(s.scores[:])
		}
		empty := false
		for i, t := range x.terms {
			if t.doc < start {
				t.Seek(start)
			}
			if t.doc == noMoreDocs {
				return x.finish()
			}
			if t.doc >= end {
				empty = true // nothing of this term in the window
				break
			}
			if i == 0 {
				clear(s.res[:])
				t.scatterAdd(start, end, s.scores[:], &s.res, &s.counts, x.scored)
				continue
			}
			clear(s.tmp[:])
			t.scatterAdd(start, end, s.scores[:], &s.tmp, &s.counts, x.scored)
			var any uint64
			for w := 0; w < nw; w++ {
				s.res[w] &= s.tmp[w]
				any |= s.res[w]
			}
			if any == 0 {
				empty = true
				break
			}
		}
		if empty {
			continue
		}
		x.start, x.word, x.off, x.nw = start, 0, -1, nw
		x.bitsLeft = s.res[0]
		return x.next()
	}
}

// next goes to the next doc of the window, or to the first of the next window with
// a match.
func (x *bufferedIntersectionCursor) next() uint32 {
	for {
		if x.bitsLeft != 0 {
			x.off = x.word*64 + bits.TrailingZeros64(x.bitsLeft)
			x.bitsLeft &= x.bitsLeft - 1
			return x.start + uint32(x.off)
		}
		x.word++
		if x.word >= x.nw {
			return x.refill()
		}
		x.bitsLeft = x.scratch.res[x.word]
	}
}

// toFallback sends the cursor to the intersectionCursor over the terms, once they
// are at the target.
func (x *bufferedIntersectionCursor) toFallback(target uint32) uint32 {
	x.release()
	cursors := make([]docCursor, len(x.terms))
	for i, t := range x.terms {
		if t.doc < target {
			t.Seek(target)
		}
		cursors[i] = t
	}
	for _, t := range x.terms {
		if t.doc == noMoreDocs {
			x.doc = noMoreDocs
			return noMoreDocs
		}
	}
	x.fb = newIntersectionCursor(cursors)
	x.advances = 0
	x.doc = x.fb.Doc()
	return x.doc
}

// toWindows takes the cursor back from the intersectionCursor, which is on the
// doc it is to be on.
func (x *bufferedIntersectionCursor) toWindows() uint32 {
	x.fb = nil
	x.scratch = bufIntersectScratchPool.Get().(*bufIntersectScratch)
	x.span = 256
	x.doc = x.refill()
	return x.doc
}

// Doc implements docCursor.
func (x *bufferedIntersectionCursor) Doc() uint32 { return x.doc }

// Advance implements docCursor.
func (x *bufferedIntersectionCursor) Advance() uint32 {
	if x.doc == noMoreDocs {
		return noMoreDocs
	}
	if x.fb != nil {
		x.doc = x.fb.Advance()
		x.advances++
		if x.doc != noMoreDocs && x.advances >= fallbackAdvances {
			return x.toWindows()
		}
		return x.doc
	}
	x.doc = x.next()
	return x.doc
}

// Seek implements docCursor. Within the window it skips to the target's word and
// masks off what's before it; just beyond it a new window is started at the target;
// further than a whole window, the cursor falls back to the intersectionCursor.
func (x *bufferedIntersectionCursor) Seek(target uint32) uint32 {
	if x.doc >= target {
		return x.doc
	}
	if x.fb != nil {
		x.advances = 0
		x.doc = x.fb.Seek(target)
		return x.doc
	}
	if windowEnd := uint64(x.start) + uint64(x.nw)*64; uint64(target) >= windowEnd {
		if uint64(target) >= windowEnd+windowDocs {
			return x.toFallback(target)
		}
		// just beyond the window: the next one has it, or is near
		for _, t := range x.terms {
			if t.doc < target {
				t.Seek(target)
			}
		}
		x.doc = x.refill()
		return x.doc
	}
	toff := int(target - x.start)
	if tword := toff / 64; tword > x.word {
		x.word, x.bitsLeft = tword, x.scratch.res[tword]
	}
	x.bitsLeft &^= 1<<(toff%64) - 1
	x.doc = x.next()
	return x.doc
}

// Score implements docCursor.
func (x *bufferedIntersectionCursor) Score() float32 {
	if x.fb != nil {
		return x.fb.Score()
	}
	return x.scratch.scores[x.off]
}

// Cost implements docCursor.
func (x *bufferedIntersectionCursor) Cost() uint64 {
	cost := x.terms[0].Cost()
	for _, t := range x.terms[1:] {
		cost = min(cost, t.Cost())
	}
	return cost
}

// Offset implements docCursor.
func (x *bufferedIntersectionCursor) Offset() uint64 { return x.offset }

// Err implements docCursor.
func (x *bufferedIntersectionCursor) Err() error {
	for _, t := range x.terms {
		if err := t.Err(); err != nil {
			return err
		}
	}
	return nil
}

// fillBlock puts the next matches, up to a block of them, in blk, and returns how
// many there are (0 when the cursor is done).
func (x *bufferedIntersectionCursor) fillBlock(blk *search.PerSegmentScoredBlock) int {
	n := 0
	var max float32
	for n < search.PerSegmentBlockLen && x.doc != noMoreDocs {
		var score float32
		if x.scored {
			score = x.Score()
			if score > max {
				max = score
			}
		}
		blk.Docs[n] = x.doc
		blk.Scores[n] = score
		n++
		x.Advance()
	}
	blk.Offset = x.offset
	blk.MaxScore = max
	return n
}

var (
	_ docCursor = (*bufferedIntersectionCursor)(nil)
	_ releaser  = (*bufferedIntersectionCursor)(nil)
)
