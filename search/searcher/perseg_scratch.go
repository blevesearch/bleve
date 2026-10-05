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

import "sync"

// The algorithms that work on windows of docs -- MAXSCORE, the AND on bitmaps, and
// the unions and intersections that visit every match -- put the postings of a
// window in dense arrays by doc: the bits of a bitmap say which docs have one, and
// slots hold their scores. The arrays are big (a lane of scores is 16KB) and the
// same for every search, so they are pooled.
//
// There are four kinds of scratch, each with a pool of its own, because what a
// holder has to leave behind for the next one differs. A pool must never be shared
// between kinds, or one that leaves less than another expects poisons it:
//
//	kind                    what it holds                                 given back
//	msScratch               MAXSCORE: a lane of scores per term, the      lanes ALL ZERO, bitmaps and counts
//	                        bitmap and counts of a batch                  zero; never after an error
//	conjScratch             the AND on bitmaps: a lane per term, two      any state (a lane is only read where
//	                        bitmaps                                       this window has just written it)
//	bufUnionScratch         bufferedUnionCursor: scores, counts, bitmap   all zero, which it is once the cursor
//	                                                                       has gone through every match
//	bufIntersectScratch     bufferedIntersectionCursor                    any state (cleaned where it is used)
//
// A scratch that was in use when its holder was cut short -- by an error, a
// cancelled context or a panic -- is simply not given back, whatever its kind.

const (
	// windowDocs is how many docs a window (or batch) covers, and windowWords the
	// words of the bitmap that has a bit for each of them.
	windowDocs  = 4096
	windowWords = windowDocs / 64
)

// laneSet is a lane of scores, by doc offset in the window, for each term of a
// query, made when first asked for.
type laneSet struct {
	lanes [][]float32
}

// lane is the lane of the term in the place idx of the query.
func (l *laneSet) lane(idx int) []float32 {
	for len(l.lanes) <= idx {
		l.lanes = append(l.lanes, nil)
	}
	if l.lanes[idx] == nil {
		l.lanes[idx] = make([]float32, windowDocs)
	}
	return l.lanes[idx]
}

// msScratch is the working memory of maxScoreSegment.
type msScratch struct {
	laneSet
	words [windowWords]uint64
	count [windowDocs]uint8
	surv  []msSurvivor
	// the offsets, in the batch, of the docs that have a strong term: all that
	// the lanes have anything at
	cands []uint16
	// per segment
	all     []*msTerm
	live    []*msTerm
	weakPre []float32
	dirty   []bool
	terms   []msTerm
}

var msScratchPool = sync.Pool{New: func() any {
	return &msScratch{surv: make([]msSurvivor, 0, 256)}
}}

// conjScratch is the working memory of the windows of an AND that are done on
// bitmaps.
type conjScratch struct {
	laneSet
	res [windowWords]uint64
	tmp [windowWords]uint64
}

var conjScratchPool = sync.Pool{New: func() any { return new(conjScratch) }}

// bufUnionScratch is the working memory of a bufferedUnionCursor: the scores and
// the number of terms by doc of the window, and the bitmap.
type bufUnionScratch struct {
	scores [windowDocs]float32
	counts [windowDocs]uint8
	words  [windowWords]uint64
}

var bufUnionScratchPool = sync.Pool{New: func() any { return new(bufUnionScratch) }}

// bufIntersectScratch is the working memory of a bufferedIntersectionCursor.
type bufIntersectScratch struct {
	scores [windowDocs]float32
	counts [windowDocs]uint8 // not looked at: what a term puts the bits and scores with wants it
	res    [windowWords]uint64
	tmp    [windowWords]uint64
}

var bufIntersectScratchPool = sync.Pool{New: func() any { return new(bufIntersectScratch) }}

// buildTermCursors are the cursors over the postings of the terms in segment seg,
// in the order of the query. With all set, a term with no postings in the segment
// means there are no matches (an AND) and the result is nil; otherwise the terms
// that have none are left out (an OR).
func buildTermCursors(terms []*PerSegmentTermSearcher, seg int, scored, all bool) []*termCursor {
	curs := make([]*termCursor, 0, len(terms))
	for idx, t := range terms {
		if seg >= len(t.readers) || t.readers[seg] == nil {
			if all {
				return nil
			}
			continue
		}
		curs = append(curs, newTermCursor(idx, t.readers[seg], t.scorer, scored))
	}
	return curs
}

// bufferedIntersectionMaxSkew is how many times more postings a term may have
// than the one with the fewest for the intersection to be done by windows: they
// pay for the postings of all the terms, seeking only for those of the fewest.
const bufferedIntersectionMaxSkew = 8

// intersectsByWindowsCounts reports whether an AND of terms with the least and
// the most postings lo and hi is done by windows.
func intersectsByWindowsCounts(lo, hi uint64) bool {
	return hi <= lo*bufferedIntersectionMaxSkew
}
