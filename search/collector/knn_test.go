// Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build vectors
// +build vectors

package collector

import (
	"context"
	"errors"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

var errTestKNNSearch = errors.New("knn searcher error")

func makeKNNDoc(sb map[int]float64) *search.DocumentMatch {
	return &search.DocumentMatch{ScoreBreakdown: sb}
}

// knnStubSearcher is like stubSearcher, but also propagates ScoreBreakdown
// (needed to exercise the per-heap routing in collectStoreKNN) and can be
// made to fail on a specific match index.
type knnStubSearcher struct {
	matches []*search.DocumentMatch
	index   int
	errAt   int
}

func (ss *knnStubSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	if ss.errAt >= 0 && ss.index == ss.errAt {
		return nil, errTestKNNSearch
	}
	if ss.index < len(ss.matches) {
		rv := ctx.DocumentMatchPool.Get()
		rv.IndexInternalID = ss.matches[ss.index].IndexInternalID
		rv.Score = ss.matches[ss.index].Score
		for k, v := range ss.matches[ss.index].ScoreBreakdown {
			if rv.ScoreBreakdown == nil {
				rv.ScoreBreakdown = make(map[int]float64)
			}
			rv.ScoreBreakdown[k] = v
		}
		ss.index++
		return rv, nil
	}
	return nil, nil
}

func (ss *knnStubSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	return nil, nil
}
func (ss *knnStubSearcher) Close() error               { return nil }
func (ss *knnStubSearcher) Weight() float64            { return 0.0 }
func (ss *knnStubSearcher) SetQueryNorm(float64)       {}
func (ss *knnStubSearcher) Count() uint64              { return uint64(len(ss.matches)) }
func (ss *knnStubSearcher) Min() int                   { return 0 }
func (ss *knnStubSearcher) Size() int                  { return 0 }
func (ss *knnStubSearcher) DocumentMatchPoolSize() int { return 0 }

// errorIDReader wraps stubReader but fails ID lookups, to exercise the
// error path in finalizeResults.
type errorIDReader struct {
	stubReader
}

func (r *errorIDReader) ExternalID(id index.IndexInternalID) (string, error) {
	return "", errors.New("external id lookup failed")
}

func TestGetNewKNNCollectorStore(t *testing.T) {
	store := GetNewKNNCollectorStore([]int64{3, 12})
	if len(store.internalHeaps) != 2 {
		t.Fatalf("internalHeaps len=%d want 2", len(store.internalHeaps))
	}
	if len(store.kValues) != 2 || store.kValues[0] != 3 || store.kValues[1] != 12 {
		t.Errorf("kValues=%v want [3 12]", store.kValues)
	}
	// size 3 (<=10) picks the slice-backed store, size 12 (>10) picks the heap.
	if _, ok := store.internalHeaps[0].(*collectStoreSlice); !ok {
		t.Errorf("internalHeaps[0] is %T, want *collectStoreSlice", store.internalHeaps[0])
	}
	if _, ok := store.internalHeaps[1].(*collectStoreHeap); !ok {
		t.Errorf("internalHeaps[1] is %T, want *collectStoreHeap", store.internalHeaps[1])
	}
}

func TestCollectStoreKNNAddDocumentSkipsNonParticipatingHeap(t *testing.T) {
	store := GetNewKNNCollectorStore([]int64{5})
	released := store.AddDocument(makeKNNDoc(map[int]float64{}))
	if len(released) != 0 {
		t.Errorf("released=%v want none", released)
	}
	if len(store.internalHeaps[0].Internal()) != 0 {
		t.Errorf("heap should stay empty when doc has no matching ScoreBreakdown entries")
	}
}

func TestCollectStoreKNNAddDocumentDelayedEject(t *testing.T) {
	// Two size-1 heaps; a doc evicted from one heap is only released once
	// it has also been evicted from every other heap it participated in.
	store := GetNewKNNCollectorStore([]int64{1, 1})

	doc1 := makeKNNDoc(map[int]float64{0: 5, 1: 3})
	if released := store.AddDocument(doc1); len(released) != 0 {
		t.Fatalf("first insert should not release anything, got %v", released)
	}

	// Better in heap 0 only; evicts doc1 from heap 0, but doc1 survives in heap 1.
	doc2 := makeKNNDoc(map[int]float64{0: 10})
	if released := store.AddDocument(doc2); len(released) != 0 {
		t.Fatalf("doc1 still lives in heap 1, should not be released, got %v", released)
	}
	if _, ok := doc1.ScoreBreakdown[0]; ok {
		t.Errorf("doc1 ScoreBreakdown should have lost key 0 after eviction from heap 0")
	}
	if _, ok := doc1.ScoreBreakdown[1]; !ok {
		t.Errorf("doc1 ScoreBreakdown should still have key 1")
	}

	// Better in heap 1 only; evicts doc1 from its last remaining heap.
	doc3 := makeKNNDoc(map[int]float64{1: 100})
	released := store.AddDocument(doc3)
	if len(released) != 1 || released[0] != doc1 {
		t.Fatalf("expected doc1 to be released once evicted from all heaps, got %v", released)
	}
}

func TestCollectStoreKNNAddDocumentEjectSameCall(t *testing.T) {
	// A single AddDocument call can evict the same old doc from more
	// than one heap; it should only be released once, in that same call.
	store := GetNewKNNCollectorStore([]int64{1, 1})

	doc1 := makeKNNDoc(map[int]float64{0: 1, 1: 1})
	store.AddDocument(doc1)

	doc2 := makeKNNDoc(map[int]float64{0: 5, 1: 5})
	released := store.AddDocument(doc2)
	if len(released) != 1 || released[0] != doc1 {
		t.Fatalf("expected doc1 released exactly once, got %v", released)
	}
	if len(doc1.ScoreBreakdown) != 0 {
		t.Errorf("doc1 ScoreBreakdown should be empty, got %v", doc1.ScoreBreakdown)
	}
}

func TestCollectStoreKNNFinalDedupesAcrossHeaps(t *testing.T) {
	store := GetNewKNNCollectorStore([]int64{5, 5})

	docA := makeKNNDoc(map[int]float64{0: 1, 1: 2}) // in both heaps
	docB := makeKNNDoc(map[int]float64{0: 2})       // heap 0 only
	docC := makeKNNDoc(map[int]float64{1: 3})       // heap 1 only

	for _, d := range []*search.DocumentMatch{docA, docB, docC} {
		if released := store.AddDocument(d); len(released) != 0 {
			t.Fatalf("no eviction expected with size-5 heaps, got %v", released)
		}
	}

	fixupCalls := 0
	result, err := store.Final(func(*search.DocumentMatch) error {
		fixupCalls++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("Final len=%d want 3 (docA deduped across heaps)", len(result))
	}
	if fixupCalls != 3 {
		t.Errorf("fixupCalls=%d want 3", fixupCalls)
	}
}

func TestCollectStoreKNNFinalEmpty(t *testing.T) {
	store := GetNewKNNCollectorStore([]int64{5})
	result, err := store.Final(noFixup)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Errorf("Final on empty store len=%d want 0", len(result))
	}
}

func TestCollectStoreKNNFinalNilFixup(t *testing.T) {
	store := GetNewKNNCollectorStore([]int64{5})
	store.AddDocument(makeKNNDoc(map[int]float64{0: 1}))
	result, err := store.Final(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 {
		t.Errorf("Final(nil) len=%d want 1", len(result))
	}
}

func TestCollectStoreKNNFinalFixupError(t *testing.T) {
	store := GetNewKNNCollectorStore([]int64{5})
	store.AddDocument(makeKNNDoc(map[int]float64{0: 1}))

	_, err := store.Final(func(*search.DocumentMatch) error {
		return errTestFixup
	})
	if err != errTestFixup {
		t.Errorf("Final fixup error not propagated: got %v", err)
	}
}

func TestMakeKNNDocMatchHandlerWrongCollectorType(t *testing.T) {
	ctx := &search.SearchContext{Collector: NewTopNCollector(1, 0, search.SortOrder{&search.SortScore{Desc: true}})}
	handler, err := MakeKNNDocMatchHandler(ctx)
	if handler != nil || err != nil {
		t.Errorf("expected (nil, nil) for a non-KNN collector, got (%v, %v)", handler, err)
	}
}

func TestMakeKNNDocMatchHandlerAddsAndReleases(t *testing.T) {
	hc := NewKNNCollector([]int64{1}, 1)
	ctx := &search.SearchContext{
		Collector:         hc,
		DocumentMatchPool: search.NewDocumentMatchPool(0, 0),
	}

	handler, err := MakeKNNDocMatchHandler(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if handler == nil {
		t.Fatal("expected a non-nil handler")
	}

	if err := handler(nil); err != nil {
		t.Errorf("handler(nil) should be a no-op, got %v", err)
	}

	doc1 := makeKNNDoc(map[int]float64{0: 1})
	if err := handler(doc1); err != nil {
		t.Fatal(err)
	}

	doc2 := makeKNNDoc(map[int]float64{0: 5})
	if err := handler(doc2); err != nil {
		t.Fatal(err)
	}

	// doc1 lost its only heap slot to doc2, so it should have been
	// recycled back into the document match pool.
	recycled := ctx.DocumentMatchPool.Get()
	if recycled != doc1 {
		t.Errorf("expected doc1 to be recycled into the pool, got %v", recycled)
	}
}

func TestKNNCollectorCollectBasic(t *testing.T) {
	matches := []*search.DocumentMatch{
		{IndexInternalID: index.IndexInternalID("a"), ScoreBreakdown: map[int]float64{0: 1}},
		{IndexInternalID: index.IndexInternalID("b"), ScoreBreakdown: map[int]float64{0: 5}},
		{IndexInternalID: index.IndexInternalID("c"), ScoreBreakdown: map[int]float64{0: 3}},
		{IndexInternalID: index.IndexInternalID("d"), ScoreBreakdown: map[int]float64{0: 0.5}},
	}
	searcher := &knnStubSearcher{matches: matches, errAt: -1}

	hc := NewKNNCollector([]int64{2}, 2)
	err := hc.Collect(context.Background(), searcher, &stubReader{})
	if err != nil {
		t.Fatal(err)
	}

	if hc.Total() != 4 {
		t.Errorf("Total()=%d want 4", hc.Total())
	}
	if hc.MaxScore() != 0 {
		t.Errorf("MaxScore()=%f want 0 (KNNCollector never tracks it)", hc.MaxScore())
	}
	if hc.Took() < 0 {
		t.Errorf("Took()=%v want >= 0", hc.Took())
	}

	results := hc.Results()
	if len(results) != 2 {
		t.Fatalf("Results() len=%d want 2 (top-2 by ScoreBreakdown[0])", len(results))
	}
	got := map[string]bool{}
	for _, r := range results {
		got[r.ID] = true
	}
	if !got["b"] || !got["c"] {
		t.Errorf("expected top-2 hits {b, c}, got %v", got)
	}

	hc.SetFacetsBuilder(nil)
	if fr := hc.FacetResults(); fr != nil {
		t.Errorf("FacetResults()=%v want nil (facets unsupported)", fr)
	}
}

func TestKNNCollectorCollectContextCancelledBeforeStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	hc := NewKNNCollector([]int64{1}, 1)
	searcher := &knnStubSearcher{
		matches: []*search.DocumentMatch{{IndexInternalID: index.IndexInternalID("a"), ScoreBreakdown: map[int]float64{0: 1}}},
		errAt:   -1,
	}
	err := hc.Collect(ctx, searcher, &stubReader{})
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestKNNCollectorCollectSearcherError(t *testing.T) {
	searcher := &knnStubSearcher{
		matches: []*search.DocumentMatch{{IndexInternalID: index.IndexInternalID("a"), ScoreBreakdown: map[int]float64{0: 1}}},
		errAt:   0,
	}
	hc := NewKNNCollector([]int64{1}, 1)
	err := hc.Collect(context.Background(), searcher, &stubReader{})
	if err != errTestKNNSearch {
		t.Errorf("expected searcher error to propagate, got %v", err)
	}
}

func TestKNNCollectorCollectFinalizeIDLookupError(t *testing.T) {
	searcher := &knnStubSearcher{
		matches: []*search.DocumentMatch{{IndexInternalID: index.IndexInternalID("a"), ScoreBreakdown: map[int]float64{0: 1}}},
		errAt:   -1,
	}
	hc := NewKNNCollector([]int64{1}, 1)
	err := hc.Collect(context.Background(), searcher, &errorIDReader{})
	if err == nil {
		t.Fatal("expected an error from the failing ExternalID lookup")
	}
}

func TestKNNCollectorCollectCustomHandlerError(t *testing.T) {
	searcher := &knnStubSearcher{
		matches: []*search.DocumentMatch{{IndexInternalID: index.IndexInternalID("a"), ScoreBreakdown: map[int]float64{0: 1}}},
		errAt:   -1,
	}
	hc := NewKNNCollector([]int64{1}, 1)

	var handlerMaker search.MakeKNNDocumentMatchHandler = func(ctx *search.SearchContext) (search.DocumentMatchHandler, error) {
		return func(d *search.DocumentMatch) error {
			if d == nil {
				return nil
			}
			return errTestFixup
		}, nil
	}
	ctx := context.WithValue(context.Background(), search.MakeKNNDocumentMatchHandlerKey, handlerMaker)

	err := hc.Collect(ctx, searcher, &stubReader{})
	if err != errTestFixup {
		t.Errorf("expected custom handler error to propagate, got %v", err)
	}
}

func TestKNNCollectorCollectCustomHandlerMakerError(t *testing.T) {
	searcher := &knnStubSearcher{errAt: -1}
	hc := NewKNNCollector([]int64{1}, 1)

	var handlerMaker search.MakeKNNDocumentMatchHandler = func(ctx *search.SearchContext) (search.DocumentMatchHandler, error) {
		return nil, errTestFixup
	}
	ctx := context.WithValue(context.Background(), search.MakeKNNDocumentMatchHandlerKey, handlerMaker)

	err := hc.Collect(ctx, searcher, &stubReader{})
	if err != errTestFixup {
		t.Errorf("expected handler-maker error to propagate, got %v", err)
	}
}

func TestKNNCollectorCollectFinalizeHandlerError(t *testing.T) {
	searcher := &knnStubSearcher{
		matches: []*search.DocumentMatch{{IndexInternalID: index.IndexInternalID("a"), ScoreBreakdown: map[int]float64{0: 1}}},
		errAt:   -1,
	}
	hc := NewKNNCollector([]int64{1}, 1)

	var handlerMaker search.MakeKNNDocumentMatchHandler = func(ctx *search.SearchContext) (search.DocumentMatchHandler, error) {
		return func(d *search.DocumentMatch) error {
			if d == nil {
				// fail only on the final flush call
				return errTestFixup
			}
			return nil
		}, nil
	}
	ctx := context.WithValue(context.Background(), search.MakeKNNDocumentMatchHandlerKey, handlerMaker)

	err := hc.Collect(ctx, searcher, &stubReader{})
	if err != errTestFixup {
		t.Errorf("expected finalize-flush handler error to propagate, got %v", err)
	}
}
