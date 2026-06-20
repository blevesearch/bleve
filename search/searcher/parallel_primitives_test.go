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

package searcher

// Unit tests for two lock-free primitives added by §7 parallel segment search:
//
//   sharedThreshold — monotone float64 updated via CAS (IEEE 754 positive float
//                     ordering == uint64 bit-pattern ordering, so no mutex needed).
//                     Bug class: a CAS implementation that swaps unconditionally,
//                     or that forgets the compare loop, would let a low score race
//                     past a higher one and allow weak segments to pass the
//                     WAND pruning threshold.
//
//   dmMinHeap       — per-shard min-heap bounded to k elements.  pushBounded must
//                     evict the minimum (not maximum) when k is exceeded, and
//                     minScore must reflect the true heap minimum once full.
//                     Bug class: swapping the heap root incorrectly, or returning
//                     the wrong evictee, would leak suboptimal docs into the final
//                     merge or cause good docs to be silently discarded.

import (
	"sync"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

// ---------------------------------------------------------------------------
// sharedThreshold
// ---------------------------------------------------------------------------

func TestSharedThresholdInitial(t *testing.T) {
	var st sharedThreshold
	if got := st.Get(); got != 0 {
		t.Errorf("zero-valued sharedThreshold.Get()=%f, want 0", got)
	}
}

// TestSharedThresholdUpdateMonotone verifies that Update() only raises the
// threshold — lower or equal values are silently ignored.
func TestSharedThresholdUpdateMonotone(t *testing.T) {
	var st sharedThreshold

	st.Update(3.0)
	if got := st.Get(); got != 3.0 {
		t.Errorf("after Update(3.0): Get()=%f want 3.0", got)
	}

	st.Update(1.0) // lower value → no-op
	if got := st.Get(); got != 3.0 {
		t.Errorf("after Update(1.0): Get()=%f want 3.0 (monotone violated)", got)
	}

	st.Update(3.0) // equal value → no-op
	if got := st.Get(); got != 3.0 {
		t.Errorf("after Update(3.0) (equal): Get()=%f want 3.0", got)
	}

	st.Update(7.5) // higher value → must succeed
	if got := st.Get(); got != 7.5 {
		t.Errorf("after Update(7.5): Get()=%f want 7.5", got)
	}
}

// TestSharedThresholdConcurrentRace confirms that concurrent calls to Update()
// leave the threshold at the maximum submitted value. Run with -race to detect
// data races in the CAS loop.
func TestSharedThresholdConcurrentRace(t *testing.T) {
	var st sharedThreshold
	const n = 200

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		v := float64(i)
		go func() {
			defer wg.Done()
			st.Update(v)
		}()
	}
	wg.Wait()

	if got := st.Get(); got != float64(n-1) {
		t.Errorf("after %d concurrent updates: Get()=%f want %f", n, got, float64(n-1))
	}
}

// ---------------------------------------------------------------------------
// dmMinHeap / pushBounded
// ---------------------------------------------------------------------------

func makeTestDoc(score float64) *search.DocumentMatch {
	return &search.DocumentMatch{Score: score}
}

// TestDmMinHeapOrdering verifies that heapPop extracts elements in ascending
// score order (min-heap property).
func TestDmMinHeapOrdering(t *testing.T) {
	var h dmMinHeap
	for _, s := range []float64{5, 1, 8, 3, 9, 2} {
		h.heapPush(makeTestDoc(s))
	}

	prev := -1.0
	for h.Len() > 0 {
		got := h.heapPop().Score
		if got < prev {
			t.Errorf("heapPop returned %f after %f: not ascending (min-heap broken)", got, prev)
		}
		prev = got
	}
}

// TestDmMinHeapPushBoundedEviction verifies that pushBounded(k=3) evicts the
// minimum-score element when size exceeds k, and reports the new minimum.
func TestDmMinHeapPushBoundedEviction(t *testing.T) {
	var h dmMinHeap
	const k = 3

	// Fill to k without overflow — no eviction expected.
	for _, s := range []float64{5, 3, 7} {
		ev, _ := h.pushBounded(makeTestDoc(s), k)
		if ev != nil {
			t.Errorf("unexpected eviction at len≤k: evicted score=%f", ev.Score)
		}
	}

	// Push score=9 → heap=[3,5,7,9] → pop min=3 → heap=[5,7,9].
	ev, minScore := h.pushBounded(makeTestDoc(9), k)
	if ev == nil {
		t.Fatal("expected eviction on overflow, got nil")
	}
	if ev.Score != 3 {
		t.Errorf("evicted score=%f, want 3 (the former minimum)", ev.Score)
	}
	if h.Len() != k {
		t.Errorf("heap len=%d after eviction, want %d", h.Len(), k)
	}
	if minScore != 5 {
		t.Errorf("minScore=%f, want 5 (new heap minimum after eviction)", minScore)
	}
}

// TestDmMinHeapPushBoundedRetainsTopK verifies that after many pushes the heap
// contains exactly the top-k highest-scored documents.
func TestDmMinHeapPushBoundedRetainsTopK(t *testing.T) {
	var h dmMinHeap
	const k = 3
	for _, s := range []float64{1, 9, 3, 7, 5, 8, 2, 6} {
		h.pushBounded(makeTestDoc(s), k)
	}
	if h.Len() != k {
		t.Fatalf("heap len=%d want %d", h.Len(), k)
	}

	// Pop all in ascending order; expect 7, 8, 9.
	got := make([]float64, 0, k)
	for h.Len() > 0 {
		got = append(got, h.heapPop().Score)
	}
	want := []float64{7, 8, 9}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("pop[%d]=%f want %f", i, got[i], w)
		}
	}
}

// TestDmMinHeapMinScoreWhenNotFull verifies that pushBounded returns minScore=0
// while the heap has fewer than k elements.
func TestDmMinHeapMinScoreWhenNotFull(t *testing.T) {
	var h dmMinHeap
	_, minScore := h.pushBounded(makeTestDoc(5), 3)
	if minScore != 0 {
		t.Errorf("minScore=%f when heap not full, want 0", minScore)
	}
}
