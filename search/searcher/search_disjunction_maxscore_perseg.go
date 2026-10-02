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
	"math/bits"

	"github.com/blevesearch/bleve/v2/search"
)

const (
	// msOuterWindow is the least number of docs of the outer window, in which
	// each term has a single bound (the best of its blocks there) and so the
	// split into terms that can drive a match and terms that can't is made once.
	msOuterWindow = 8192
	// msBootstrapBatch is the batch while there is no threshold yet.
	msBootstrapBatch = 256
	// msMinPostings is how many postings the terms of a segment need to have
	// between them for MAXSCORE to be the algorithm.
	msMinPostings = 512
	// msRound inflates the bounds so that rounding in the float32 arithmetic of
	// a score can't leave one above its bound.
	msRound = 1 + 1e-5
)

// msTerm is a term in the window being worked on.
type msTerm struct {
	c        *termCursor
	bound    float32 // the most a posting of the term can score in the window
	priority float32 // bound per posting: terms with little to offer for what they cost go first
}

// msSurvivor is a doc that is still a candidate for the heap after the strong
// terms: where it is in the batch, how many terms matched it so far and the sum
// of what they scored.
type msSurvivor struct {
	doc uint32
	off int
	m   int
	s   float32
}

// useMaxScore reports whether maxScoreSegment is the algorithm for the cursors
// of a segment. It is, unless the terms have so few postings together that
// there is little for a pass over batches of docs to be good at, and WAND,
// which skips as much as can be, wins by a hair: MAXSCORE's cost per posting
// does not depend on the number of terms the way WAND's per document cost does,
// and it is not any worse for a single pair of terms.
func (s *PerSegmentDisjunctionSearcher) useMaxScore(curs []*termCursor) bool {
	switch disjunctionAlgo.Load() {
	case disjunctionAlgoWAND:
		return false
	case disjunctionAlgoMaxScore:
		return true
	}
	if len(curs) < 2 {
		return false
	}
	var postings uint64
	for _, c := range curs {
		postings += c.Cost()
	}
	return postings >= msMinPostings
}

// maxScoreSegment finds the best hits of a plain OR of terms in one segment with
// block-max MAXSCORE, processed in batches.
//
// The docs are covered by windows. In one, each term has a bound on its scores,
// and the terms are put in order of the bound they have per posting. The first
// of them, as long as together they can't reach the threshold, are weak: a doc
// that only they match can't make the heap, so every hit has a strong term.
// The strong terms are read for the whole window, a batch of docs at a time,
// into arrays by doc; which docs they have is a bitmap. Of the docs that have
// one, those that, with all the weak terms matching too, would still fall short
// are dropped. The weak terms are then looked at for those left, one term after
// the other, the last first, dropping a doc whenever the bounds of the weak
// terms not looked at yet can't make up for what it lacks. That is much less
// work than WAND does for a query with terms that match a lot and score little,
// as the cursors of those are only moved for the docs that are worth it, and
// the work of the strong terms is a pass over sequential memory.
//
// A disjunction's score is the sum of the matching terms' scores times the
// share of the clauses that match (coord), so a bound with m of the terms
// matching is the sum of their bounds times m/n. Scores are summed in the order
// of the query, as everywhere else, so that a doc has the same score whatever
// the algorithm.
func (s *PerSegmentDisjunctionSearcher) maxScoreSegment(ctx context.Context, sink search.PerSegmentSink,
	seg int, byIdx []*termCursor, scratch *msScratch) error {
	n := float32(len(s.children))
	numTerms := len(s.terms)
	h := sink.Heap(seg)
	offset := byIdx[0].Offset()

	var visited uint64
	var maxScore float32
	pruning := false
	markPruning := func(thr float32) {
		if !pruning && !math.IsInf(float64(thr), -1) {
			sink.MarkPruned()
			pruning = true
		}
	}

	if cap(scratch.terms) < len(byIdx) {
		scratch.terms = make([]msTerm, len(byIdx))
	}
	terms := scratch.terms[:len(byIdx)]
	all := scratch.all[:0]
	for i, c := range byIdx {
		terms[i] = msTerm{c: c}
		all = append(all, &terms[i])
	}
	scratch.all = all
	live := scratch.live[:0]
	if cap(scratch.weakPre) < len(all)+1 {
		scratch.weakPre = make([]float32, len(all)+1)
	}
	weakPre := scratch.weakPre[:len(all)+1]

	// scores by doc offset in the batch, for each term, by its place in the
	// query: there is one for each term that needs one
	for len(scratch.lanes) < numTerms {
		scratch.lanes = append(scratch.lanes, nil)
	}
	lanes := scratch.lanes
	if cap(scratch.dirty) < numTerms {
		scratch.dirty = make([]bool, numTerms)
	}
	laneDirty := scratch.dirty[:numTerms]
	clear(laneDirty)
	cands := scratch.cands[:0]
	laneFor := scratch.lane
	words, counts := &scratch.words, &scratch.count
	surv := scratch.surv[:0]

	start := uint32(noMoreDocs)
	for _, t := range all {
		if t.c.doc < start {
			start = t.c.doc
		}
	}
	var iterations uint64

	for start != noMoreDocs {
		select {
		case <-ctx.Done():
			search.RecordSearchCost(ctx, search.AbortM, 0)
			return ctx.Err()
		default:
		}
		iterations++

		// the bounds of the window; it ends where the first of the terms'
		// last blocks in it does
		target := uint64(start) + msOuterWindow - 1
		end := uint64(math.MaxUint32)
		live = live[:0]
		for _, t := range all {
			if t.c.doc == noMoreDocs {
				continue
			}
			bound, last, ok := t.c.boundUpTo(start, target)
			if !ok {
				continue // nothing from here on
			}
			t.bound = bound
			t.priority = bound / float32(t.c.Cost())
			if e := uint64(last) + 1; e < end {
				end = e
			}
			live = append(live, t)
		}
		if len(live) == 0 {
			break
		}
		// insertion sort: there are few terms, and they are nearly in order from
		// one window to the next
		for i := 1; i < len(live); i++ {
			for j := i; j > 0 && live[j].priority < live[j-1].priority; j-- {
				live[j], live[j-1] = live[j-1], live[j]
			}
		}

		// the docs before floor have been taken care of
		floor := start
		for {
			// The split is made for each batch, with the threshold as it is: as
			// the heap fills it moves up, and terms that could drive a match
			// before can't any more.
			thr := thresholdOf(sink)
			markPruning(thr)
			var sum float32
			weakCount := 0
			weakPre[0] = 0
			for k, t := range live {
				sum += t.bound
				if sum*(float32(k+1)/n)*msRound > thr {
					break
				}
				weakCount = k + 1
				weakPre[k+1] = sum
			}
			if weakCount == len(live) {
				break // nothing in the rest of the window can make the heap
			}
			weak, strong := live[:weakCount], live[weakCount:]
			weakAll := weakPre[weakCount]

			base := uint32(noMoreDocs)
			for _, t := range strong {
				if t.c.doc < floor {
					t.c.Seek(floor)
				}
				if t.c.doc < base {
					base = t.c.doc
				}
			}
			if uint64(base) >= end {
				break
			}
			if iterations++; iterations%64 == 0 {
				select {
				case <-ctx.Done():
					search.RecordSearchCost(ctx, search.AbortM, 0)
					return ctx.Err()
				default:
				}
			}
			// until the heap is full the threshold is nothing, so the batches
			// are small, to get one soon
			batch := uint64(windowDocs)
			if math.IsInf(float64(thr), -1) {
				batch = msBootstrapBatch
			}
			winEnd := uint32(min(end, uint64(base)+batch))
			floor = winEnd
			span := int(winEnd - base)

			// the strong terms' postings in the batch
			for _, t := range strong {
				t.c.scatterAdd(base, winEnd, laneFor(t.c.idx), words, counts, true) // the lane is zero: adding is setting
				laneDirty[t.c.idx] = true
			}

			// the docs that have one, less those that can't make the heap even
			// with every weak term matching
			surv = surv[:0]
			cands = cands[:0]
			for w := range words {
				word := words[w]
				words[w] = 0
				for ; word != 0; word &= word - 1 {
					off := w*64 + bits.TrailingZeros64(word)
					m := int(counts[off])
					counts[off] = 0
					cands = append(cands, uint16(off))
					var sc float32
					for _, t := range strong {
						sc += lanes[t.c.idx][off]
					}
					if (sc+weakAll)*(float32(m+weakCount)/n)*msRound <= thr {
						continue
					}
					surv = append(surv, msSurvivor{doc: base + uint32(off), off: off, m: m, s: sc})
				}
			}

			// the weak terms, the last first
			for wi := weakCount - 1; wi >= 0 && len(surv) > 0; wi-- {
				t := weak[wi]
				lane := laneFor(t.c.idx)
				laneDirty[t.c.idx] = true
				kept := 0
				for _, sv := range surv {
					if (sv.s+weakPre[wi+1])*(float32(sv.m+wi+1)/n)*msRound <= thr {
						continue
					}
					if t.c.doc < sv.doc {
						t.c.Seek(sv.doc)
					}
					if t.c.doc == sv.doc {
						sc := t.c.Score()
						lane[sv.off] = sc
						sv.s += sc
						sv.m++
					}
					if (sv.s+weakPre[wi])*(float32(sv.m+wi)/n)*msRound <= thr {
						continue
					}
					surv[kept] = sv
					kept++
				}
				surv = surv[:kept]
			}

			// what's left is scored, in the order of the query
			for _, sv := range surv {
				var total float32
				for idx := 0; idx < numTerms; idx++ {
					if lane := lanes[idx]; lane != nil {
						total += lane[sv.off]
					}
				}
				total *= float32(sv.m) / n
				if total > maxScore {
					maxScore = total
				}
				if total > thr {
					h.Offer(search.PerSegmentHit{Score: total, Doc: offset + uint64(sv.doc),
						Ord: uint32(visited), Seg: uint32(seg)})
					thr = thresholdOf(sink)
					markPruning(thr)
				}
				visited++
			}

			// the lanes go back to zero: where they were written if that is
			// few places, all of them if it is many
			for idx, dirty := range laneDirty {
				if !dirty {
					continue
				}
				lane := lanes[idx]
				if len(cands)*8 < span {
					for _, off := range cands {
						lane[off] = 0
					}
				} else {
					clear(lane[:span])
				}
				laneDirty[idx] = false
			}
		}
		if end >= math.MaxUint32 {
			break
		}
		start = uint32(end)
	}

	scratch.surv = surv
	scratch.cands = cands
	scratch.live = live
	sink.AddTotal(seg, visited)
	sink.ObserveMaxScore(maxScore)
	for _, c := range byIdx {
		if err := c.Err(); err != nil {
			return err
		}
	}
	return nil
}
