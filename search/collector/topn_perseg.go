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
	"sort"
	"time"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var _ search.Collector = (*PerSegmentTopNCollector)(nil)

var reflectStaticSizePerSegmentTopNCollector int
var reflectStaticSizePerSegmentHit int

func init() {
	var c PerSegmentTopNCollector
	reflectStaticSizePerSegmentTopNCollector = int(reflect.TypeOf(c).Size())
	var h search.PerSegmentHit
	reflectStaticSizePerSegmentHit = int(reflect.TypeOf(h).Size())
}

// PerSegmentTopNCollector collects the top N hits of a search.PerSegmentSearcher,
// ordered by score. It is generic: all it knows about the search is what the
// searcher tells it, a block of scored matches at a time.
//
// Collect repeatedly asks the searcher for its next block and offers the
// matches to the heap of the segment they are from, until the searcher is
// exhausted; then it merges the segment heaps into the top hits. That's all.
//
// A searcher that has a faster way to fill the heaps for the kind of search it
// is (see search.OptimizedPerSegmentSearcher) is asked to do so: it is handed
// the heaps through a search.PerSegmentSink, and what it does to fill them is
// its own business. The merge at the end is the same either way.
//
// For a scored search it gives the hits TopNCollector gives for a score sorted
// search on the same searcher -- same order, ties going to the lower doc
// number, same total -- with scores within float32 precision. A search without
// scores comes out as the first N matches in doc order (all scores are 0, and
// ties go to the lower doc number), with the exact total. It supports nothing
// beyond that: no other sort, no facets, no search after, no explanations, no
// locations.
type PerSegmentTopNCollector struct {
	size int
	skip int

	total    uint64
	maxScore float64
	took     time.Duration
	results  search.DocumentMatchCollection
	pruned   bool
}

// NewPerSegmentTopNCollector builds a collector to find the top 'size' hits,
// skipping over the first 'skip' hits.
func NewPerSegmentTopNCollector(size int, skip int) *PerSegmentTopNCollector {
	return &PerSegmentTopNCollector{size: size, skip: skip}
}

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

// Collect goes to the searcher to find the matching documents. The searcher has
// to be a search.PerSegmentSearcher.
func (hc *PerSegmentTopNCollector) Collect(ctx context.Context, searcher search.Searcher,
	reader index.IndexReader) error {
	startTime := time.Now()

	psSearcher, ok := searcher.(search.PerSegmentSearcher)
	if !ok {
		return fmt.Errorf("collector: per segment collector needs a per segment searcher, got %T", searcher)
	}

	state := &perSegmentState{k: hc.size + hc.skip}
	var err error
	if opt, ok := searcher.(search.OptimizedPerSegmentSearcher); ok && opt.CanCollectOptimized() {
		err = opt.CollectOptimized(ctx, state)
	} else {
		err = search.DrainPerSegmentSearcher(ctx, psSearcher, state, CheckDoneEvery)
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

// SetFacetsBuilder isn't supported; a request with facets isn't served by this
// collector.
func (hc *PerSegmentTopNCollector) SetFacetsBuilder(facetsBuilder *search.FacetsBuilder) {
}

// FacetResults has nothing to return.
func (hc *PerSegmentTopNCollector) FacetResults() search.FacetResults {
	return nil
}
