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
	"sync"

	"github.com/blevesearch/bleve/v2/search"
)

// bufferedUnionCursor is the matches of a plain OR of terms, found a window of
// docs at a time, the way tantivy's BufferedUnionScorer does. The cursors of the
// terms don't take turns, doc after doc, to find the lowest doc they are on,
// which is what unionCursor pays for with every match: the window starts at the
// lowest doc, each term puts its postings that are in it in a bitmap and in the
// slot of their doc, adding to the score there, and the matches are the bits of
// the bitmap, taken lowest first.
//
// The terms are visited in the order of the query, so the score of a doc is
// summed in that order, and is the unionCursor's, to the bit: the sum times the
// share of the clauses that match.
//
// It can only be moved forward a doc at a time (Advance), or a block of docs
// (fillBlock). That is all a collection that visits every match needs.
type bufferedUnionCursor struct {
	terms  []*termCursor // in the order of the query
	n      float32       // the clauses of the query, also those with no postings in the segment
	scored bool

	scratch  *bufUnionScratch
	start    uint32 // the first doc of the window
	word     int    // the word of the bitmap being read
	bitsLeft uint64 // what's left of it
	off      int    // where the doc the cursor is on is in the window
	doc      uint32
	offset   uint64
}

const (
	bufUnionWords   = 64
	bufUnionHorizon = bufUnionWords * 64
)

// bufUnionScratch is the working memory of a bufferedUnionCursor: the scores and
// the number of terms by doc of the window, and the bitmap. It is left zeroed by a
// cursor that has gone through all its matches, which is the only kind it is given
// back by.
type bufUnionScratch struct {
	scores [bufUnionHorizon]float32
	counts [bufUnionHorizon]uint8
	words  [bufUnionWords]uint64
}

var bufUnionScratchPool = sync.Pool{New: func() any { return new(bufUnionScratch) }}

// newBufferedUnionCursor is the union of the terms, which are in the order of the
// query; n is the number of clauses the query has.
func newBufferedUnionCursor(terms []*termCursor, n int, scored bool) *bufferedUnionCursor {
	u := &bufferedUnionCursor{
		terms:   terms,
		n:       float32(n),
		scored:  scored,
		scratch: bufUnionScratchPool.Get().(*bufUnionScratch),
		offset:  terms[0].Offset(),
		off:     -1,
	}
	u.doc = u.refill()
	return u
}

// release gives the memory back, if it is clean: the cursor went through all of
// its matches.
func (u *bufferedUnionCursor) release() {
	if u.scratch != nil && u.doc == noMoreDocs {
		bufUnionScratchPool.Put(u.scratch)
	}
	u.scratch = nil
}

// refill puts the window that starts at the lowest doc the terms are on in the
// bitmap, and goes to its first doc, which it returns (noMoreDocs if the terms
// have none left).
func (u *bufferedUnionCursor) refill() uint32 {
	start := uint32(noMoreDocs)
	for _, t := range u.terms {
		if t.doc < start {
			start = t.doc
		}
	}
	if start == noMoreDocs {
		u.doc = noMoreDocs
		u.release()
		return noMoreDocs
	}
	end := uint32(noMoreDocs)
	if uint64(start)+bufUnionHorizon < uint64(noMoreDocs) {
		end = start + bufUnionHorizon
	}
	s := u.scratch
	for _, t := range u.terms {
		if t.doc < end {
			t.scatterAdd(start, end, s.scores[:], &s.words, &s.counts, u.scored)
		}
	}
	u.start, u.word, u.off = start, 0, -1
	u.bitsLeft = s.words[0]
	return u.next()
}

// next goes to the next doc of the window, or to the first of the next one.
func (u *bufferedUnionCursor) next() uint32 {
	s := u.scratch
	for {
		if u.bitsLeft != 0 {
			bit := bits.TrailingZeros64(u.bitsLeft)
			u.bitsLeft &= u.bitsLeft - 1
			u.off = u.word*64 + bit
			return u.start + uint32(u.off)
		}
		s.words[u.word] = 0
		u.word++
		if u.word == bufUnionWords {
			return u.refill()
		}
		u.bitsLeft = s.words[u.word]
	}
}

// consumed puts back to zero the slot of the doc the cursor is on, which has been
// looked at.
func (u *bufferedUnionCursor) consumed() {
	if u.off >= 0 {
		u.scratch.scores[u.off] = 0
		u.scratch.counts[u.off] = 0
	}
}

// Doc implements docCursor.
func (u *bufferedUnionCursor) Doc() uint32 { return u.doc }

// Advance implements docCursor.
func (u *bufferedUnionCursor) Advance() uint32 {
	if u.doc == noMoreDocs {
		return noMoreDocs
	}
	u.consumed()
	u.doc = u.next()
	return u.doc
}

// Seek implements docCursor, the slow way: the cursor is made to be advanced.
func (u *bufferedUnionCursor) Seek(target uint32) uint32 {
	for u.doc < target {
		u.Advance()
	}
	return u.doc
}

// Score implements docCursor.
func (u *bufferedUnionCursor) Score() float32 {
	s := u.scratch
	return s.scores[u.off] * (float32(s.counts[u.off]) / u.n)
}

// Cost implements docCursor.
func (u *bufferedUnionCursor) Cost() uint64 {
	var cost uint64
	for _, t := range u.terms {
		cost += t.Cost()
	}
	return cost
}

// Offset implements docCursor.
func (u *bufferedUnionCursor) Offset() uint64 { return u.offset }

// Err implements docCursor.
func (u *bufferedUnionCursor) Err() error {
	for _, t := range u.terms {
		if err := t.Err(); err != nil {
			return err
		}
	}
	return nil
}

// fillBlock puts the next matches, up to a block of them, in blk, and returns how
// many there are (0 when the cursor is done). It is Advance and Score, without
// the call for each match.
func (u *bufferedUnionCursor) fillBlock(blk *search.PerSegmentScoredBlock) int {
	n := 0
	var max float32
	for n < search.PerSegmentBlockLen && u.doc != noMoreDocs {
		var score float32
		if u.scored {
			score = u.Score()
			if score > max {
				max = score
			}
		}
		blk.Docs[n] = u.doc
		blk.Scores[n] = score
		n++
		u.consumed()
		u.doc = u.next()
	}
	blk.Offset = u.offset
	blk.MaxScore = max
	return n
}

var _ docCursor = (*bufferedUnionCursor)(nil)
