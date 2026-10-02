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
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
)

var reflectStaticSizePerSegmentConjunctionSearcher int

func init() {
	var s PerSegmentConjunctionSearcher
	reflectStaticSizePerSegmentConjunctionSearcher = int(reflect.TypeOf(s).Size())
}

// PerSegmentConjunctionSearcher is the conjunction of per segment searchers:
// the docs that match all of them, scored by the sum of their scores.
//
// It is a search.PerSegmentSearcher. NextMatch is the generic iteration, which
// handles every clause and visits every match: an AND of terms none of which has
// far more postings than the rest reads through a bufferedIntersectionCursor, a
// window of docs at a time; anything else through an intersectionCursor, leapfrogging.
// When its clauses are all terms, and there are at least two, it is also a
// search.OptimizedPerSegmentSearcher:
//
//   - with scores, CollectOptimized works in windows of the blocks of the terms,
//     skipping a window whose best possible score can't make the heap, and
//     doing the narrow ones that are worth it on bitmaps. See windowSegment.
//   - without scores, it takes the first matches in doc order by leapfrogging
//     cursors that decode doc numbers only, and stops there, the total being
//     a lower bound -- or, for a search for no hits (a count), it goes on to
//     count them all.
type PerSegmentConjunctionSearcher struct {
	perSegBase

	// terms are the clauses, if all of them are terms
	terms []*PerSegmentTermSearcher
}

var _ search.PerSegmentSearcher = (*PerSegmentConjunctionSearcher)(nil)
var _ search.OptimizedPerSegmentSearcher = (*PerSegmentConjunctionSearcher)(nil)
var _ perSegChild = (*PerSegmentConjunctionSearcher)(nil)

// NewPerSegmentConjunctionSearcher builds the conjunction of the searchers, or
// returns nil if one of them is not a per segment searcher, in which case the
// regular NewConjunctionSearcher is what has to be used (and the searchers
// have to be closed by whoever has them).
func NewPerSegmentConjunctionSearcher(qsearchers []search.Searcher,
	options search.SearcherOptions) *PerSegmentConjunctionSearcher {
	children := make([]perSegChild, len(qsearchers))
	wraps := make([][]wrapKind, len(qsearchers))
	for i, q := range qsearchers {
		c, ok := q.(perSegChild)
		if !ok {
			return nil
		}
		children[i], wraps[i] = unwrapSingleKinds(c)
	}
	rv := &PerSegmentConjunctionSearcher{
		perSegBase: perSegBase{children: children, wraps: wraps, scored: options.Score != "none"},
	}
	terms := make([]*PerSegmentTermSearcher, len(children))
	for i, c := range children {
		t, ok := c.(*PerSegmentTermSearcher)
		if !ok {
			terms = nil
			break
		}
		terms[i] = t
	}
	rv.terms = terms
	rv.computeQueryNorm()
	return rv
}

func (s *PerSegmentConjunctionSearcher) Size() int {
	sizeInBytes := reflectStaticSizePerSegmentConjunctionSearcher + size.SizeOfPtr
	for _, c := range s.children {
		sizeInBytes += c.Size()
	}
	return sizeInBytes
}

// Count is the number of matches of the clause with the fewest.
func (s *PerSegmentConjunctionSearcher) Count() uint64 {
	rv := uint64(math.MaxUint64)
	for _, c := range s.children {
		rv = min(rv, c.Count())
	}
	return rv
}

func (s *PerSegmentConjunctionSearcher) Min() int { return 0 }

func (s *PerSegmentConjunctionSearcher) numSegments() int { return s.segments() }

// segCursor implements perSegChild.
func (s *PerSegmentConjunctionSearcher) segCursor(seg int, scored bool) (docCursor, bool) {
	return s.segCursorSeeked(seg, scored, 0)
}

// segCost implements perSegChild: the matches of its clause that has the fewest, at
// most.
func (s *PerSegmentConjunctionSearcher) segCost(seg int) uint64 {
	cost := uint64(math.MaxUint64)
	for _, c := range s.children {
		cost = min(cost, c.segCost(seg))
	}
	if len(s.children) == 0 {
		return 0
	}
	return cost
}

// segCursorSeeked implements perSegChild.
func (s *PerSegmentConjunctionSearcher) segCursorSeeked(seg int, scored bool, seeks uint64) (docCursor, bool) {
	cost := s.segCost(seg)
	if cost == 0 {
		return nil, false // a clause that doesn't match here: nothing does
	}
	if s.pruningApplies() && s.intersectsByWindows(seg) && (seeks == 0 || cost <= nestedBufferedMaxRatio*seeks) {
		// an AND of terms with about as many postings as each other: a window of
		// docs at a time
		return newBufferedIntersectionCursor(buildTermCursors(s.terms, seg, scored, true), scored), true
	}
	// the cursors are walked cheapest first: the clause with the fewest matches is
	// read through and the others are sought about as many times as that has matches
	hint := cost
	if seeks > 0 {
		hint = min(hint, seeks)
	}
	cursors := make([]docCursor, 0, len(s.children))
	for _, c := range s.children {
		cur, ok := c.segCursorSeeked(seg, scored, hint)
		if !ok {
			return nil, false
		}
		cursors = append(cursors, cur)
	}
	if len(cursors) == 0 {
		return nil, false
	}
	return newIntersectionCursor(cursors), true
}

// NextMatch implements search.PerSegmentSearcher.
func (s *PerSegmentConjunctionSearcher) NextMatch() (search.PerSegmentMatch, bool, error) {
	return s.nextMatch(func(seg int) docCursor {
		if c, ok := s.segCursor(seg, s.scored); ok {
			return c
		}
		return nil
	})
}

// intersectsByWindows reports whether the matches of the segment are found a
// window of docs at a time (see bufferedIntersectionCursor): when every term has
// postings there and none has far more than the others. The clauses have to be
// terms.
func (s *PerSegmentConjunctionSearcher) intersectsByWindows(seg int) bool {
	var lo, hi uint64
	for i, t := range s.terms {
		if seg >= len(t.readers) || t.readers[seg] == nil {
			return false
		}
		n := t.readers[seg].Count()
		if i == 0 {
			lo, hi = n, n
		}
		lo, hi = min(lo, n), max(hi, n)
	}
	return len(s.terms) >= 2 && intersectsByWindowsCounts(lo, hi)
}

// CanCollectOptimized implements search.OptimizedPerSegmentSearcher.
func (s *PerSegmentConjunctionSearcher) CanCollectOptimized() bool {
	// what the algorithms work on, or a search that doesn't want scores, for
	// which it's enough to stop at the first matches
	return s.pruningApplies() || !s.scored
}

// pruningApplies reports whether the clauses are what the algorithms for
// conjunctions work on: an AND of at least two terms.
func (s *PerSegmentConjunctionSearcher) pruningApplies() bool {
	return s.terms != nil && len(s.terms) >= 2
}

// CollectOptimized implements search.OptimizedPerSegmentSearcher.
func (s *PerSegmentConjunctionSearcher) CollectOptimized(ctx context.Context,
	sink search.PerSegmentSink) error {
	if !s.pruningApplies() {
		// not an AND of terms, and so a search without scores
		return s.collectUnscoredGeneric(ctx, sink, func(seg int) docCursor {
			if c, ok := s.segCursor(seg, false); ok {
				return c
			}
			return nil
		})
	}
	for seg := 0; seg < s.segments(); seg++ {
		select {
		case <-ctx.Done():
			search.RecordSearchCost(ctx, search.AbortM, 0)
			return ctx.Err()
		default:
		}

		// all of the clauses have to have postings in the segment
		curs := buildTermCursors(s.terms, seg, s.scored, true)
		if curs == nil {
			continue
		}

		var err error
		if !s.scored && sink.Limit() > 0 {
			if heapIsFull(sink) {
				// the segment may have matches that were not asked for: see
				// intersectUnscored
				sink.MarkPruned()
				return nil
			}
			err = s.intersectUnscored(ctx, sink, seg, curs)
		} else if !s.scored {
			// a count
			err = s.countSegment(ctx, sink, seg, curs)
		} else if s.scored && sink.Limit() > 0 && prunable(curs) {
			err = s.windowSegment(ctx, sink, seg, curs)
		} else {
			// no scores, or the total and max score have to be exact, or the
			// scores can't be bounded: every match is visited, and counted
			if useBufferedIntersection(curs) {
				err = drainCursor(ctx, sink, seg, newBufferedIntersectionCursor(curs, s.scored), s.scored)
			} else {
				cursors := make([]docCursor, len(curs))
				for i, c := range curs {
					cursors[i] = c
				}
				err = drainCursor(ctx, sink, seg, newIntersectionCursor(cursors), s.scored)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// The window of docs, in bitmask words, in which a dense intersection is
// counted; and how much of the segment the leader has to cover for the
// intersection to be dense: more than one doc in denseCountInverse. These are
// tantivy's (query/intersection.rs).
const (
	intersectionWindowWords = 16
	intersectionWindowDocs  = intersectionWindowWords * 64
	denseCountInverse       = 32
)

// countSegment counts the matches of an AND of terms in one segment, as tantivy
// counts an intersection (Intersection::count_including_deleted).
//
// If the term that leads, the one with the fewest postings, covers less than one
// doc in 32 of the segment, the intersection is sparse: walking it, leapfrogging
// the cursors, counts it as it goes, and the cursors skip over the gaps.
// Otherwise, most windows have matches in them, and the work is in the postings:
// then a window of docs is gathered in a bitmask per term, the masks are ANDed,
// and the bits counted. No seek per doc, and no call per doc through an
// interface either.
func (s *PerSegmentConjunctionSearcher) countSegment(ctx context.Context, sink search.PerSegmentSink,
	seg int, curs []*termCursor) error {
	order := append([]*termCursor(nil), curs...)
	for i := 1; i < len(order); i++ {
		for j := i; j > 0 && order[j].cost < order[j-1].cost; j-- {
			order[j], order[j-1] = order[j-1], order[j]
		}
	}

	if order[0].cost*denseCountInverse < order[0].reader.SegmentDocs() {
		return s.countSparse(ctx, sink, seg, curs)
	}
	return s.countDense(ctx, sink, seg, order)
}

func (s *PerSegmentConjunctionSearcher) countSparse(ctx context.Context, sink search.PerSegmentSink,
	seg int, curs []*termCursor) error {
	cursors := make([]docCursor, len(curs))
	for i, c := range curs {
		cursors[i] = c
	}
	return drainCursor(ctx, sink, seg, newIntersectionCursor(cursors), false)
}

// countDense counts window by window. order is by cost, the leader first.
func (s *PerSegmentConjunctionSearcher) countDense(ctx context.Context, sink search.PerSegmentSink,
	seg int, order []*termCursor) error {
	leader, right, others := order[0], order[1], order[2:]
	var total uint64
	var windows uint64

	nextBase := leader.Doc()
	for nextBase != noMoreDocs {
		windows++
		if windows%checkDoneEvery == 0 {
			select {
			case <-ctx.Done():
				search.RecordSearchCost(ctx, search.AbortM, 0)
				return ctx.Err()
			default:
			}
		}

		// The window starts at the next doc that any cursor has left of the
		// last one. A cursor that was left behind (an empty mask ends a window
		// early) is brought up by its Seek.
		base := nextBase
		end := base + intersectionWindowDocs
		if end < base {
			end = noMoreDocs
		}

		var mask, other [intersectionWindowWords]uint64
		leader.Seek(base)
		leader.fillBits(base, end, mask[:])
		nextBase = max(nextBase, leader.Doc())

		right.Seek(base)
		right.fillBits(base, end, other[:])
		nextBase = max(nextBase, right.Doc())
		if andWindows(&mask, &other) {
			continue
		}

		for _, c := range others {
			other = [intersectionWindowWords]uint64{}
			c.Seek(base)
			c.fillBits(base, end, other[:])
			nextBase = max(nextBase, c.Doc())
			if andWindows(&mask, &other) {
				break
			}
		}

		for _, w := range mask {
			total += uint64(bits.OnesCount64(w))
		}
	}

	sink.AddTotal(seg, total)
	for _, c := range order {
		if err := c.Err(); err != nil {
			return err
		}
	}
	return nil
}

// andWindows ANDs b into a, and reports whether nothing is left of a.
func andWindows(a, b *[intersectionWindowWords]uint64) (empty bool) {
	var any uint64
	for i := range a {
		a[i] &= b[i]
		any |= a[i]
	}
	return any == 0
}

// intersectUnscored takes the first matches of an AND of terms in one segment in
// doc order, without scoring, and stops there: the matches that follow the last
// hit aren't counted, as counting them takes the whole intersection. The total
// is then a lower bound, which the sink is told. (A search for no hits at all
// is a count, and takes the generic path, which counts.)
func (s *PerSegmentConjunctionSearcher) intersectUnscored(ctx context.Context, sink search.PerSegmentSink,
	seg int, curs []*termCursor) error {
	cursors := make([]docCursor, len(curs))
	for i, c := range curs {
		cursors[i] = c
	}
	c := newIntersectionCursor(cursors)
	h := sink.Heap(seg)
	k := sink.Limit()
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
	return c.Err()
}

// conjunctionWindow looks at the blocks that hold the first postings >= doc of
// the leader and of the other terms, and gives the window that these blocks
// bound: the docs from doc to windowEnd, the first of the last docs of those
// blocks. In it, no posting of a term scores more than the term's block max,
// the bounds of the other terms (secs) being put in blockMaxes, and their sum
// returned. The leader's own is leader.BlockMaxScore(). It reports false if
// there are no more windows: a term has no block from doc on.
func conjunctionWindow(leader *termCursor, secs []*termCursor, doc uint32,
	blockMaxes []float32) (windowEnd uint32, secBlockMax float32, ok bool) {
	leader.ShallowSeek(doc)
	windowEnd = leader.LastDocInBlock()
	if windowEnd == noMoreDocs {
		return 0, 0, false
	}
	for i, c := range secs {
		c.ShallowSeek(doc)
		last := c.LastDocInBlock()
		if last == noMoreDocs {
			return 0, 0, false // a term has nothing from here on
		}
		windowEnd = min(windowEnd, last)
		blockMaxes[i] = c.BlockMaxScore()
		secBlockMax += blockMaxes[i]
	}
	return windowEnd, secBlockMax, true
}

// windowSegment finds the best hits of an AND of terms in one segment with the
// block-max pruning that tantivy does for intersections
// (query/boolean_query/block_wand_intersection.rs).
//
// The term with the fewest postings leads. The blocks of its postings define
// windows of docs; a window ends where the first block of any of the terms
// ends, so that, in it, each term has a single block, and with it a bound on
// its scores. If the bounds of the blocks add up to no more than the threshold,
// the score the heap's worst hit has, nothing in the window can make the heap
// and the whole window is skipped, undecoded.
//
// In a window that can't be ruled out, the leader's block is scored in one go
// by the SIMD kernel. A posting only gets as far as the other terms' cursors if
// its score, with the bounds of the others' blocks added, can beat the
// threshold; and once a term has been looked at, the bounds of the rest are all
// that's left to assume.
//
// A window that is narrow (so that its docs fit a bitmap), and has a good many of the
// leader's postings that could make the heap, is done on bitmaps instead: the
// candidates and the postings of each other term in the window are put in
// bitmaps, which are ANDed, and what's left is scored from lanes of block scores,
// rather than looking each candidate up with a seek. See conjBitmapMinCandidates.
func (s *PerSegmentConjunctionSearcher) windowSegment(ctx context.Context, sink search.PerSegmentSink,
	seg int, byIdx []*termCursor) (retErr error) {
	// working memory for the windows that are done with bitmaps, taken when
	// the first of them is
	minCands := int(conjBitmapMinCandidates.Load())
	var scratch *conjScratch
	defer func() {
		if scratch != nil {
			conjScratchPool.Put(scratch)
		}
	}()
	h := sink.Heap(seg)
	offset := byIdx[0].Offset()

	// the rarest term leads
	order := append([]*termCursor(nil), byIdx...)
	for i := 1; i < len(order); i++ {
		for j := i; j > 0 && order[j].cost < order[j-1].cost; j-- {
			order[j], order[j-1] = order[j-1], order[j]
		}
	}
	leader, secs := order[0], order[1:]

	var secMaxSum float32
	for _, c := range secs {
		secMaxSum += c.MaxScore()
	}
	globalMax := leader.MaxScore() + secMaxSum

	var visited uint64
	var maxScore float32
	pruning := false
	markPruning := func(thr float32) {
		if !pruning && !math.IsInf(float64(thr), -1) {
			sink.MarkPruned()
			pruning = true
		}
	}

	blockMaxes := make([]float32, len(secs))
	suffix := make([]float32, len(secs)) // suffix[i]: the block bounds of secs[i+1:]
	scoreByIdx := make([]float32, len(byIdx))

	thr := thresholdOf(sink)
	markPruning(thr)
	finished := globalMax <= thr // nothing in the segment can make the heap
	doc := leader.Doc()
	var windows uint64

	for !finished && doc != noMoreDocs {
		windows++
		if windows%checkDoneEvery == 0 {
			select {
			case <-ctx.Done():
				search.RecordSearchCost(ctx, search.AbortM, 0)
				return ctx.Err()
			default:
			}
		}

		// the window: where all the terms are within one block each
		windowEnd, secBlockMax, ok := conjunctionWindow(leader, secs, doc, blockMaxes)
		if !ok {
			break
		}

		thr = thresholdOf(sink)
		markPruning(thr)
		if leader.BlockMaxScore()+secBlockMax <= thr {
			doc = windowEnd + 1 // the whole window is out
			continue
		}

		// score the leader's block, and look at the postings in the window
		if leader.Seek(doc) == noMoreDocs {
			break
		}
		scores := leader.blockScores()
		docs := leader.blk.Docs[:leader.n]
		start := leader.pos
		end := start // the first posting past the window
		{
			lo, hi := start, len(docs)
			for lo < hi {
				mid := int(uint(lo+hi) >> 1)
				if docs[mid] <= windowEnd {
					lo = mid + 1
				} else {
					hi = mid
				}
			}
			end = lo
		}

		var running float32
		for i := len(secs) - 1; i >= 0; i-- {
			suffix[i] = running
			running += blockMaxes[i]
		}
		scoreThreshold := thr - secBlockMax

		// A window that is narrow, with many of the leader's postings that could
		// make the heap, is done on bitmaps: the leader's candidates and each
		// term's postings in the window are put in one, the bitmaps are ANDed,
		// and the matches that are left are scored from the scores of the terms,
		// which were worked out for blocks. No cursor is moved for any single doc.
		if len(secs) > 0 && uint64(windowEnd-doc) < windowDocs {
			cands := 0
			for _, sc := range scores[start:end] {
				cands += b2i(sc > scoreThreshold)
			}
			if cands >= minCands {
				if scratch == nil {
					scratch = conjScratchPool.Get().(*conjScratch)
				}
				base := doc
				nw := int(windowEnd-base)/64 + 1
				res, tmp := scratch.res[:nw], scratch.tmp[:nw]
				clear(res)
				lead := scratch.lane(leader.idx)
				for i := start; i < end; i++ {
					if sc := scores[i]; sc > scoreThreshold {
						off := docs[i] - base
						res[off>>6] |= 1 << (off & 63)
						lead[off] = sc
					}
				}
				alive := true
				for _, c := range secs {
					if c.Seek(base) == noMoreDocs {
						finished = true // a term has run out: there are no more matches
						alive = false
						break
					}
					sdocs := c.blk.Docs[c.pos:c.n]
					sscores := c.blockScores()[c.pos:c.n]
					lane := scratch.lane(c.idx)
					clear(tmp)
					for j, d := range sdocs {
						if d > windowEnd {
							break
						}
						off := d - base
						tmp[off>>6] |= 1 << (off & 63)
						lane[off] = sscores[j]
					}
					var any uint64
					for w := range res {
						res[w] &= tmp[w]
						any |= res[w]
					}
					if any == 0 {
						alive = false
						break
					}
				}
				if alive {
				matches:
					for w, word := range res {
						for ; word != 0; word &= word - 1 {
							off := w*64 + bits.TrailingZeros64(word)
							var total float32
							for _, c := range byIdx {
								total += scratch.lanes[c.idx][off]
							}
							if total > maxScore {
								maxScore = total
							}
							if total > thr {
								h.Offer(search.PerSegmentHit{Score: total, Doc: offset + uint64(base) + uint64(off),
									Ord: uint32(visited), Seg: uint32(seg)})
								thr = thresholdOf(sink)
								markPruning(thr)
								if globalMax <= thr {
									finished = true
									visited++
									break matches
								}
							}
							visited++
						}
					}
				}
				doc = windowEnd + 1
				continue
			}
		}

	candidates:
		for i := start; i < end; i++ {
			leaderScore := scores[i]
			if leaderScore <= scoreThreshold {
				continue
			}
			cand := docs[i]
			partial := leaderScore
			scoreByIdx[leader.idx] = leaderScore
			for si, c := range secs {
				if c.doc == noMoreDocs {
					finished = true // a term has run out: there are no more matches
					break candidates
				}
				if c.doc > cand {
					continue candidates // an earlier candidate took it past
				}
				if c.Seek(cand) != cand {
					continue candidates
				}
				sc := c.Score()
				scoreByIdx[c.idx] = sc
				partial += sc
				// even with the best the other blocks have, no use
				if partial+suffix[si] <= thr {
					continue candidates
				}
			}

			// a match. Scores are summed in the order of the query.
			var total float32
			for _, sc := range scoreByIdx {
				total += sc
			}
			if total > maxScore {
				maxScore = total
			}
			if total > thr {
				h.Offer(search.PerSegmentHit{Score: total, Doc: offset + uint64(cand), Ord: uint32(visited), Seg: uint32(seg)})
				thr = thresholdOf(sink)
				markPruning(thr)
				if globalMax <= thr {
					finished = true
					visited++
					break candidates
				}
			}
			visited++
		}
		doc = windowEnd + 1
	}

	sink.AddTotal(seg, visited)
	sink.ObserveMaxScore(maxScore)
	for _, c := range byIdx {
		if err := c.Err(); err != nil {
			return err
		}
	}
	return nil
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
