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

package collector

import (
	"context"
	"fmt"
	"reflect"
	"runtime"
	"runtime/debug"
	"sort"
	"time"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizePerSegmentTopNCollector int
var reflectStaticSizePerSegmentHit int

func init() {
	var c PerSegmentTopNCollector
	reflectStaticSizePerSegmentTopNCollector = int(reflect.TypeOf(c).Size())
	var h search.PerSegmentHit
	reflectStaticSizePerSegmentHit = int(reflect.TypeOf(h).Size())
}

// PerSegmentTopNCollector collects the top N hits of a search.PerSegmentSearcher,
// ordered by descending score. It is generic: all it knows about the search is
// what the searcher tells it, a scored match at a time.
//
// Collect repeatedly asks the searcher for its next match and offers it to a
// min-heap of the best hits so far, shared by all the segments (which are read one
// after the other, in index order, so what a hit has to beat to be kept is the
// worst of the best hits of all the segments so far), until the searcher is
// exhausted. Then it sorts the heap into the top hits.
//
// A searcher that has a faster way to fill the heap for the kind of search it
// is (see search.OptimizedPerSegmentSearcher) is asked to do so: it is handed
// the heap through a search.PerSegmentSink, and what it does to fill it is its
// own business (skipping blocks that can't make the top hits, say, in which case
// the total is a lower bound, see EarlyStopped). The result is the same either
// way.
//
// For a scored search it gives the hits TopNCollector gives for a score sorted
// search on the same searcher -- same order, ties going to the lower doc
// number, same total unless it pruned -- with scores within float32 precision.
// A search without scores comes out as the first N matches in doc order (all
// scores are 0, and ties go to the lower doc number), with the exact total. Hits
// can be explained (see SetExplain).
//
// Descending score is the only order it serves, and it doesn't count facets or
// page after a hit: both need every match, which it may skip. Those searches are
// collected by PerSegmentSortedCollector.
type PerSegmentTopNCollector struct {
	size int
	skip int

	total    uint64
	maxScore float64
	took     time.Duration
	results  search.DocumentMatchCollection
	pruned   bool

	// explain: the hits returned come with their explanations
	explain bool
}

// NewPerSegmentTopNCollector builds a collector to find the top 'size' hits,
// skipping over the first 'skip' hits.
func NewPerSegmentTopNCollector(size int, skip int) *PerSegmentTopNCollector {
	return &PerSegmentTopNCollector{size: size, skip: skip}
}

// SetExplain makes the collection explain the hits it returns. Nothing is
// explained while the matches are found: the explanations are asked of the
// searcher (see search.PerSegmentSearcher.ExplainMatch) for the few hits that are left once
// the collection is done.
func (hc *PerSegmentTopNCollector) SetExplain(explain bool) { hc.explain = explain }

// perSegmentState is what a collection builds up: the best hits, the number of
// matches of each segment, and the highest score. It is the sink an optimized
// searcher fills.
//
// Segments are collected one after the other, in index order, so there is a
// single heap for all of them: the threshold a hit has to beat is that of the
// best hits of all the segments so far, which is what lets a searcher that
// prunes start pruning from the first doc of a later segment.
type perSegmentState struct {
	k        int
	heap     *search.PerSegmentHeap
	totals   []uint64
	maxScore float32
	pruned   bool

	// room for the totals of the usual number of segments, so that counting
	// doesn't grow a slice one append at a time
	totalsBuf [16]uint64
}

var _ search.PerSegmentSink = (*perSegmentState)(nil)

func (st *perSegmentState) Limit() int { return st.k }

func (st *perSegmentState) Heap(seg int) *search.PerSegmentHeap {
	if st.heap == nil {
		st.heap = search.NewPerSegmentHeap(st.k)
	}
	return st.heap
}

func (st *perSegmentState) Threshold() (float32, bool) {
	if st.heap == nil {
		return 0, st.k == 0
	}
	return st.heap.Threshold()
}

func (st *perSegmentState) AddTotal(seg int, n uint64) {
	for len(st.totals) <= seg {
		st.totals = append(st.totals, 0)
	}
	st.totals[seg] += n
}

func (st *perSegmentState) ObserveMaxScore(score float32) {
	if score > st.maxScore {
		st.maxScore = score
	}
}

func (st *perSegmentState) MarkPruned() { st.pruned = true }

// Collect goes to the searcher to find the matching documents. (It is not a
// search.Collector, which collects the matches of a search.Searcher.)
func (hc *PerSegmentTopNCollector) Collect(ctx context.Context, searcher search.PerSegmentSearcher,
	reader index.IndexReader) (err error) {
	defer recoverPerSegmentPanic(&err)
	return hc.collect(ctx, searcher, reader)
}

// recoverPerSegmentPanic turns a runtime error panic into the error of a collection,
// to be deferred. The algorithms index dense arrays by doc offset and trust that
// their cursors are where they say: a mistake there is a runtime panic. It fails
// the search that hit it, not the process, and says what went wrong and where.
// (Other panics are not touched.)
func recoverPerSegmentPanic(err *error) {
	if r := recover(); r != nil {
		rerr, isRuntime := r.(runtime.Error)
		if !isRuntime {
			panic(r)
		}
		stack := debug.Stack()
		if len(stack) > 2048 {
			stack = stack[:2048]
		}
		*err = fmt.Errorf("collector: the per segment search failed: %v\n%s", rerr, stack)
	}
}

// explainPerSegmentHit gives the hit, which is a match of the doc in segment seg,
// the explanation its searcher has of it, and checks that the explanation shows
// the score the hit was ranked by, to the bit: a searcher whose explanation
// works scores out another way has to be told, not shown.
func explainPerSegmentHit(searcher search.PerSegmentSearcher, dm *search.DocumentMatch, seg int, doc uint64) error {
	expl, match, err := searcher.ExplainMatch(seg, doc)
	if err != nil {
		return err
	}
	if !match {
		return fmt.Errorf("collector: hit %s of segment %d is not a match to explain", dm.ID, seg)
	}
	if expl.Value != dm.Score {
		return fmt.Errorf("collector: the explanation of hit %s scores %v, the hit %v",
			dm.ID, expl.Value, dm.Score)
	}
	dm.Expl = expl
	return nil
}

func (hc *PerSegmentTopNCollector) collect(ctx context.Context, searcher search.PerSegmentSearcher,
	reader index.IndexReader) error {
	startTime := time.Now()

	state := &perSegmentState{k: hc.size + hc.skip}
	state.totals = state.totalsBuf[:0]
	var err error
	if opt, ok := searcher.(search.OptimizedPerSegmentSearcher); ok && opt.CanCollectOptimized() {
		err = opt.CollectOptimized(ctx, state)
	} else {
		err = search.DrainPerSegmentSearcher(ctx, searcher, state, CheckDoneEvery)
	}
	if err != nil {
		return err
	}

	top, hitBase := hc.merge(state)
	hc.pruned = state.pruned

	statsCallbackFn := ctx.Value(search.SearchIOStatsCallbackKey)
	if statsCallbackFn != nil {
		// no docvalues are read by this collector
		statsCallbackFn.(search.SearchIOStatsCallbackFunc)(0)
		search.RecordSearchCost(ctx, search.AddM, 0)
	}

	hc.took = time.Since(startTime)

	if hc.skip < len(top) {
		top = top[hc.skip:]
	} else {
		top = nil
	}
	if len(top) > hc.size {
		top = top[:hc.size]
	}

	// only the hits to be returned become DocumentMatches
	hc.results = make(search.DocumentMatchCollection, 0, len(top))
	for _, hit := range top {
		dm := &search.DocumentMatch{
			IndexInternalID: index.NewIndexInternalID(nil, hit.Doc),
			Score:           float64(hit.Score),
			HitNumber:       hitBase[hit.Seg] + uint64(hit.Ord) + 1,
			Sort:            sortByScoreOpt,
		}
		var err error
		dm.ID, err = reader.ExternalID(dm.IndexInternalID)
		if err != nil {
			return err
		}
		if hc.explain {
			if err := explainPerSegmentHit(searcher, dm, int(hit.Seg), hit.Doc); err != nil {
				return err
			}
		}
		dm.Complete(nil)
		hc.results = append(hc.results, dm)
	}
	return nil
}

// merge turns what was collected into the best skip+size hits, best first. It
// also returns, for each segment, how many matches precede its own: the hit
// number TopNCollector gives a hit is its place in the stream of matches, which
// runs segment after segment.
func (hc *PerSegmentTopNCollector) merge(state *perSegmentState) ([]search.PerSegmentHit, []uint64) {
	hitBase := make([]uint64, len(state.totals))
	var total uint64
	for seg, n := range state.totals {
		hitBase[seg] = total
		total += n
	}
	hc.total = total
	hc.maxScore = float64(state.maxScore)

	var top []search.PerSegmentHit
	if state.heap != nil {
		top = state.heap.Hits()
	}
	sort.Slice(top, func(i, j int) bool { return top[j].Worse(top[i]) })
	return top, hitBase
}

// Size is an estimate of the memory the collector needs: the hits kept for a
// segment, and for the merge.
func (hc *PerSegmentTopNCollector) Size() int {
	k := min(hc.size+hc.skip, 1000)
	return reflectStaticSizePerSegmentTopNCollector + size.SizeOfPtr +
		2*k*reflectStaticSizePerSegmentHit
}

// Results returns the collected hits
func (hc *PerSegmentTopNCollector) Results() search.DocumentMatchCollection {
	return hc.results
}

// Total returns the total number of hits
func (hc *PerSegmentTopNCollector) Total() uint64 {
	return hc.total
}

// MaxScore returns the maximum score seen across all the hits
func (hc *PerSegmentTopNCollector) MaxScore() float64 {
	return hc.maxScore
}

// Took returns the time spent collecting hits
func (hc *PerSegmentTopNCollector) Took() time.Duration {
	return hc.took
}

// EarlyStopped reports whether the searcher pruned, and so left matches out of
// the total: it is then a lower bound.
func (hc *PerSegmentTopNCollector) EarlyStopped() bool {
	return hc.pruned
}
