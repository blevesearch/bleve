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
	"math/bits"
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
)

var reflectStaticSizePerSegmentDisjunctionSearcher int

func init() {
	var s PerSegmentDisjunctionSearcher
	reflectStaticSizePerSegmentDisjunctionSearcher = int(reflect.TypeOf(s).Size())
}

// PerSegmentDisjunctionSearcher is the disjunction of per segment searchers:
// the docs that match at least min of them, scored by the sum of the scores of
// the clauses that match times the share of the clauses that do (coord).
//
// It is a search.PerSegmentSearcher. NextMatch is the generic iteration, which
// handles every clause and every min, and visits every match: a plain OR of terms
// (all clauses terms, min 1) reads through a bufferedUnionCursor, a window of
// docs at a time; anything else through a unionCursor, a match at a time. When its
// clauses are all terms and min is 1 it is also a
// search.OptimizedPerSegmentSearcher:
//
//   - with scores, CollectOptimized finds the top hits without visiting the docs that,
//     by what is known of the best score of a term, or of a block of its
//     postings, cannot make them: block-max MAXSCORE (maxScoreSegment), a window
//     of docs at a time, or, for terms that have few postings between them, block-max
//     WAND (wandSegment). A search for no hits, or whose scores can't be
//     bounded, visits every match instead.
//   - without scores, it is a union by windows of a bitset: the first matches
//     are taken in doc order, and the matches are counted with a popcount, up to
//     the window in which the last hit was found (a lower bound), or all of
//     them for a search for no hits (a count).
type PerSegmentDisjunctionSearcher struct {
	perSegBase
	min int

	// terms are the clauses, if all of them are terms
	terms []*PerSegmentTermSearcher
}

var _ search.PerSegmentSearcher = (*PerSegmentDisjunctionSearcher)(nil)
var _ search.OptimizedPerSegmentSearcher = (*PerSegmentDisjunctionSearcher)(nil)
var _ perSegChild = (*PerSegmentDisjunctionSearcher)(nil)

// NewPerSegmentDisjunctionSearcher builds the disjunction of the searchers, which
// have to be searchers of this package: it returns an error if one is not, or if
// there are more of them than a disjunction may have. The searchers are closed by
// whoever has them if it does; if not, they are the disjunction's, and closed with
// it.
func NewPerSegmentDisjunctionSearcher(qsearchers []search.PerSegmentSearcher, min float64,
	options search.SearcherOptions) (*PerSegmentDisjunctionSearcher, error) {
	if tooManyClauses(len(qsearchers)) {
		// the very error the regular searchers give
		return nil, tooManyClausesErr("", len(qsearchers))
	}
	children := make([]perSegChild, len(qsearchers))
	wraps := make([][]wrapKind, len(qsearchers))
	for i, q := range qsearchers {
		c, ok := q.(perSegChild)
		if !ok {
			return nil, fmt.Errorf("searcher: %T can't be a clause of a per segment disjunction", q)
		}
		children[i], wraps[i] = unwrapSingleKinds(c)
	}
	rv := &PerSegmentDisjunctionSearcher{
		perSegBase: perSegBase{children: children, wraps: wraps, scored: options.Score != "none"},
		min:        int(min),
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
	return rv, nil
}

func (s *PerSegmentDisjunctionSearcher) Size() int {
	sizeInBytes := reflectStaticSizePerSegmentDisjunctionSearcher + size.SizeOfPtr
	for _, c := range s.children {
		sizeInBytes += c.Size()
	}
	return sizeInBytes
}

func (s *PerSegmentDisjunctionSearcher) Count() uint64 {
	var rv uint64
	for _, c := range s.children {
		rv += c.Count()
	}
	return rv
}

func (s *PerSegmentDisjunctionSearcher) Min() int { return s.min }

func (s *PerSegmentDisjunctionSearcher) numSegments() int { return s.segments() }

// segCursor implements perSegChild.
func (s *PerSegmentDisjunctionSearcher) segCursor(seg int, scored bool) (docCursor, bool) {
	return s.segCursorSeeked(seg, scored, 0)
}

// segCost implements perSegChild: the matches of its clauses, at most.
func (s *PerSegmentDisjunctionSearcher) segCost(seg int) uint64 {
	var cost uint64
	for _, c := range s.children {
		cost += c.segCost(seg)
	}
	return cost
}

// segCursorSeeked implements perSegChild.
func (s *PerSegmentDisjunctionSearcher) segCursorSeeked(seg int, scored bool, seeks uint64) (docCursor, bool) {
	if s.pruningApplies() {
		// a plain OR of terms
		cost := s.segCost(seg)
		if cost == 0 {
			return nil, false
		}
		if seeks == 0 || cost <= nestedBufferedMaxRatio*seeks {
			// read through, or sought about as often as it has matches: a window of
			// docs at a time
			return newBufferedUnionCursor(buildTermCursors(s.terms, seg, scored, false),
				len(s.children), scored), true
		}
	}
	cursors := make([]docCursor, 0, len(s.children))
	for _, c := range s.children {
		if cur, ok := c.segCursorSeeked(seg, scored, seeks); ok {
			cursors = append(cursors, cur)
		}
	}
	// not enough clauses match here to satisfy min
	if len(cursors) == 0 || len(cursors) < s.min {
		return nil, false
	}
	return newUnionCursor(cursors, len(s.children), s.min), true
}

// NextMatch implements search.PerSegmentSearcher.
func (s *PerSegmentDisjunctionSearcher) NextMatch() (search.PerSegmentMatch, bool, error) {
	return s.nextMatch(func(seg int) docCursor {
		if c, ok := s.segCursor(seg, s.scored); ok {
			return c
		}
		return nil
	})
}

// CanCollectOptimized implements search.OptimizedPerSegmentSearcher.
func (s *PerSegmentDisjunctionSearcher) CanCollectOptimized() bool {
	// what the algorithms work on, or a search that doesn't want scores, for
	// which it's enough to stop at the first matches
	return s.pruningApplies() || !s.scored
}

// pruningApplies reports whether the clauses are what the algorithms for
// disjunctions work on: a plain OR of terms.
func (s *PerSegmentDisjunctionSearcher) pruningApplies() bool {
	return s.terms != nil && s.min <= 1
}

// CollectOptimized implements search.OptimizedPerSegmentSearcher.
func (s *PerSegmentDisjunctionSearcher) CollectOptimized(ctx context.Context,
	sink search.PerSegmentSink) (err error) {
	if !s.pruningApplies() {
		// not a plain OR of terms, and so a search without scores
		return s.collectUnscoredGeneric(ctx, sink, func(seg int) docCursor {
			if c, ok := s.segCursor(seg, false); ok {
				return c
			}
			return nil
		})
	}
	var scratch *msScratch
	// scratchBusy is whether the scratch is being worked in, and so may not be clean:
	// it's given back only when it isn't -- not after an error, nor a panic
	scratchBusy := false
	defer func() {
		if scratch != nil && !scratchBusy {
			msScratchPool.Put(scratch)
		}
	}()
	for seg := 0; seg < s.segments(); seg++ {
		select {
		case <-ctx.Done():
			search.RecordSearchCost(ctx, search.AbortM, 0)
			return ctx.Err()
		default:
		}

		// the clauses that have postings in the segment, in the order of the
		// query
		curs := buildTermCursors(s.terms, seg, s.scored, false)
		if len(curs) == 0 {
			continue
		}

		if !s.scored && sink.Limit() > 0 && heapIsFull(sink) {
			// the segment has matches that were not asked for: see unionUnscored
			sink.MarkPruned()
			return nil
		}

		switch {
		case !s.scored:
			err = s.unionUnscored(ctx, sink, seg, curs)
		case sink.Limit() == 0 || !prunable(curs):
			// the total and the max score have to be exact, or the scores
			// can't be bounded: no pruning, then
			err = s.drain(ctx, sink, seg, curs)
		case s.useMaxScore(curs):
			if scratch == nil {
				scratch = msScratchPool.Get().(*msScratch)
			}
			scratchBusy = true
			err = s.maxScoreSegment(ctx, sink, seg, curs, scratch)
			scratchBusy = err != nil
		default:
			err = s.wandSegment(ctx, sink, seg, curs)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// prunable reports whether the scores of the cursors can be bounded
func prunable(curs []*termCursor) bool {
	for _, c := range curs {
		if !c.scorer.Prunable() || !c.reader.HasBlockMax() {
			return false
		}
	}
	return true
}

func (s *PerSegmentDisjunctionSearcher) drain(ctx context.Context, sink search.PerSegmentSink,
	seg int, curs []*termCursor) error {
	return drainCursor(ctx, sink, seg, newBufferedUnionCursor(curs, len(s.children), s.scored), s.scored)
}

// sortCursorsByDoc orders the cursors by the doc they are on. They are nearly
// in order most of the time, which is what insertion sort is good at.
func sortCursorsByDoc(curs []*termCursor) {
	for i := 1; i < len(curs); i++ {
		for j := i; j > 0 && curs[j].doc < curs[j-1].doc; j-- {
			curs[j], curs[j-1] = curs[j-1], curs[j]
		}
	}
}

// restoreOrdering puts curs[ord], which may have moved forward, back in its
// place in a list otherwise ordered by doc.
func restoreOrdering(curs []*termCursor, ord int) {
	doc := curs[ord].doc
	for i := ord + 1; i < len(curs); i++ {
		if curs[i].doc >= doc {
			break
		}
		curs[i], curs[i-1] = curs[i-1], curs[i]
	}
}

// removeCursor takes curs[ord] out of the list, keeping its order.
func removeCursor(curs []*termCursor, ord int) []*termCursor {
	copy(curs[ord:], curs[ord+1:])
	return curs[:len(curs)-1]
}

// wandSegment finds the best hits of a plain OR of terms in one segment with
// block-max WAND, from "Faster Top-k Document Retrieval Using Block-Max
// Indexes", as tantivy does it (query/boolean_query/block_wand_union.rs).
//
// The cursors are kept ordered by the doc they are on. The score a doc can reach
// is bounded by the best score of the terms that can match it. So, going down
// the list, the pivot is the first cursor where the bounds of the terms so far
// add up to more than the threshold -- the score the heap's worst hit has: no
// doc before the pivot's can make the heap, as no term that comes after has
// anything on those. Then the same is asked of the bounds of the blocks the
// pivot doc would be in, which are far tighter. If they pass it too, the
// cursors before the pivot are brought to its doc and, if all are on it,
// it's scored. If not, the cursor that has the most to offer is moved past the
// shortest of the blocks, since nothing before that could do either.
//
// A disjunction's score is the sum of the matching terms' scores times the
// share of the clauses that match (coord). Its bound is the same with the most
// that can match in its place: the cursors that are on, or before, the doc.
func (s *PerSegmentDisjunctionSearcher) wandSegment(ctx context.Context, sink search.PerSegmentSink,
	seg int, byIdx []*termCursor) error {
	n := float32(len(s.children))
	h := sink.Heap(seg)
	offset := byIdx[0].Offset()

	live := make([]*termCursor, 0, len(byIdx))
	for _, c := range byIdx {
		if c.Doc() != noMoreDocs {
			live = append(live, c)
		}
	}
	sortCursorsByDoc(live)

	var visited uint64
	var maxScore float32
	pruning := false
	var iterations uint64

	for len(live) > 0 {
		iterations++
		if iterations%checkDoneEvery == 0 {
			select {
			case <-ctx.Done():
				search.RecordSearchCost(ctx, search.AbortM, 0)
				return ctx.Err()
			default:
			}
		}

		thr := thresholdOf(sink)
		if !pruning && !math.IsInf(float64(thr), -1) {
			// from here on, docs get passed over
			sink.MarkPruned()
			pruning = true
		}

		// the pivot: the first cursor where the term-wide bounds so far can
		// beat the threshold
		pivot := -1
		var cum float32
		for i, c := range live {
			cum += c.MaxScore()
			if cum*(float32(i+1)/n) > thr {
				pivot = i
				break
			}
		}
		if pivot < 0 {
			break // nothing left can make the heap
		}
		pivotDoc := live[pivot].doc
		pivotLen := pivot + 1
		for pivotLen < len(live) && live[pivotLen].doc == pivotDoc {
			pivotLen++
		}

		// the same, with the bounds of the blocks of the pivot doc
		var blockMax float32
		for _, c := range live[:pivotLen] {
			c.ShallowSeek(pivotDoc)
			blockMax += c.BlockMaxScore()
		}
		if blockMax*(float32(pivotLen)/n) <= thr {
			live = advanceOneWithLowBlockMax(live, pivotLen)
			continue
		}

		// bring the cursors before the pivot to it
		aligned := true
		for i := pivot - 1; i >= 0; i-- {
			d := live[i].Seek(pivotDoc)
			if d == pivotDoc {
				continue
			}
			if d == noMoreDocs {
				live = removeCursor(live, i)
			} else {
				restoreOrdering(live, i)
			}
			aligned = false
			break
		}
		if !aligned {
			continue
		}

		// all of live[:pivotLen] are on the pivot doc. Scores are summed in the
		// order of the query, whatever the order of the cursors is.
		sum := sumInQueryOrder(byIdx, pivotDoc)
		total := sum * (float32(pivotLen) / n)
		if total > maxScore {
			maxScore = total
		}
		if total > thr {
			h.Offer(search.PerSegmentHit{Score: total, Doc: offset + uint64(pivotDoc), Ord: uint32(visited), Seg: uint32(seg)})
		}
		visited++

		// on to the next docs
		for _, c := range live[:pivotLen] {
			c.Advance()
		}
		kept := live[:0]
		for _, c := range live {
			if c.doc != noMoreDocs {
				kept = append(kept, c)
			}
		}
		live = kept
		sortCursorsByDoc(live)
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

// sumInQueryOrder is the sum of the scores of the cursors that are on doc,
// taken in the order of the query (byIdx is).
func sumInQueryOrder(byIdx []*termCursor, doc uint32) float32 {
	var sum float32
	for _, c := range byIdx {
		if c.doc == doc {
			sum += c.Score()
		}
	}
	return sum
}

// advanceOneWithLowBlockMax is what's done when the blocks that the pivot doc is
// in can't make the heap: one of the cursors on or before the pivot is moved,
// past the end of the shortest of those blocks -- or to the next cursor's doc,
// if that comes first, as then the pivot changes -- since no doc before that
// can do any better. The one moved is that with the highest term-wide bound: it
// is the one that holds the others back the most.
func advanceOneWithLowBlockMax(live []*termCursor, pivotLen int) []*termCursor {
	toSeek := pivotLen - 1
	globalMax := live[toSeek].MaxScore()
	seekAfter := live[toSeek].LastDocInBlock()
	for i := pivotLen - 2; i >= 0; i-- {
		c := live[i]
		if c.LastDocInBlock() <= seekAfter {
			seekAfter = c.LastDocInBlock()
		}
		if c.MaxScore() > globalMax {
			globalMax = c.MaxScore()
			toSeek = i
		}
	}
	// the next block, unless there's none
	if seekAfter != noMoreDocs {
		seekAfter++
	}
	for _, c := range live[pivotLen:] {
		if c.doc <= seekAfter {
			seekAfter = c.doc
		}
	}
	if live[toSeek].Seek(seekAfter) == noMoreDocs {
		return removeCursor(live, toSeek)
	}
	restoreOrdering(live, toSeek)
	return live
}

// unionUnscored takes the first matches of a plain OR of terms in one segment in
// doc order, without scoring, and counts the matches it passes: all of them if
// no hits are wanted, and otherwise those up to the window that the last hit
// is in. The matches of a window
// of docs are gathered in a bitset; the bits are counted with a popcount, and
// read off in order while hits are still wanted.
func (s *PerSegmentDisjunctionSearcher) unionUnscored(ctx context.Context, sink search.PerSegmentSink,
	seg int, curs []*termCursor) error {
	h := sink.Heap(seg)
	k := sink.Limit()
	offset := curs[0].Offset()

	var words [unionWindowWords]uint64
	var total uint64 // the matches so far

	start := lowestDoc(curs)
	for start != noMoreDocs {
		select {
		case <-ctx.Done():
			search.RecordSearchCost(ctx, search.AbortM, 0)
			return ctx.Err()
		default:
		}

		// All that was asked for is in hand and there are matches left that
		// haven't been counted: counting them takes a walk over every posting
		// of the terms, which is exactly what a search for the first few
		// matches is meant to spare. The total is a lower bound, then. (A
		// search for no hits at all is a count, and counts.)
		if k > 0 && h.Len() >= k {
			sink.MarkPruned()
			break
		}

		end := start + unionWindowDocs
		if end < start {
			end = noMoreDocs
		}
		clear(words[:])
		for _, c := range curs {
			c.fillBits(start, end, words[:])
		}

		for w, word := range words {
			if word == 0 {
				continue
			}
			if h.Len() >= k {
				// all that's wanted is in the heap: count what remains
				total += uint64(bits.OnesCount64(word))
				continue
			}
			for ; word != 0 && h.Len() < k; word &= word - 1 {
				doc := start + uint32(w*64+bits.TrailingZeros64(word))
				h.Offer(search.PerSegmentHit{Doc: offset + uint64(doc), Ord: uint32(total), Seg: uint32(seg)})
				total++
			}
			// what's left of the word, if k was reached in it
			if word != 0 {
				total += uint64(bits.OnesCount64(word))
			}
		}
		start = lowestDoc(curs)
	}
	sink.AddTotal(seg, total)
	for _, c := range curs {
		if err := c.Err(); err != nil {
			return err
		}
	}
	return nil
}

// heapIsFull reports whether the sink has all the hits it can use
func heapIsFull(sink search.PerSegmentSink) bool {
	_, full := sink.Threshold()
	return full
}

func lowestDoc(curs []*termCursor) uint32 {
	lowest := noMoreDocs
	for _, c := range curs {
		if c.doc < lowest {
			lowest = c.doc
		}
	}
	return lowest
}
