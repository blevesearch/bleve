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
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

// scoreDesc is a collectorCompare for score-descending order.
// Returns positive when a.Score < b.Score (a is worse — lower score — and
// should sit closer to the root of the min-heap).
func scoreDesc(a, b *search.DocumentMatch) int {
	if a.Score < b.Score {
		return 1
	}
	if a.Score > b.Score {
		return -1
	}
	return 0
}

func makeScoreDoc(score float64) *search.DocumentMatch {
	return &search.DocumentMatch{Score: score}
}

// checkTernaryInvariant verifies the ternary min-heap property: for every node
// i the parent (at (i-1)/3) must be as bad or worse than the child, i.e.
// compare(parent, child) >= 0.
func checkTernaryInvariant(t *testing.T, h *collectStoreHeap) {
	t.Helper()
	for i := 1; i < len(h.heap); i++ {
		parent := (i - 1) / 3
		if h.compare(h.heap[parent], h.heap[i]) < 0 {
			t.Errorf("ternary heap invariant violated at index %d (score=%.2f): parent %d (score=%.2f) is better, should be at least as bad",
				i, h.heap[i].Score, parent, h.heap[parent].Score)
		}
	}
}

// TestTernaryHeapInvariantAfterInserts inserts scores in several orderings and
// verifies the ternary invariant holds after every insertion.
func TestTernaryHeapInvariantAfterInserts(t *testing.T) {
	for _, scores := range [][]float64{
		{5, 3, 8, 1, 7, 2, 9, 4, 6, 10},  // unsorted
		{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},  // ascending
		{10, 9, 8, 7, 6, 5, 4, 3, 2, 1},  // descending
		{3, 3, 3, 3},                       // all equal
	} {
		h := newStoreHeap(len(scores), scoreDesc)
		for _, s := range scores {
			h.add(makeScoreDoc(s))
			checkTernaryInvariant(t, h)
		}
		// Root must be the minimum score in the heap.
		min := scores[0]
		for _, s := range scores[1:] {
			if s < min {
				min = s
			}
		}
		if h.heap[0].Score != min {
			t.Errorf("root score %.2f, want min %.2f (scores %v)", h.heap[0].Score, min, scores)
		}
	}
}

// TestTernaryHeapRemoveLastAscending verifies that successive removeLast calls
// return elements in ascending score order (worst first = heap-sort property).
func TestTernaryHeapRemoveLastAscending(t *testing.T) {
	scores := []float64{5, 3, 8, 1, 7, 2, 9, 4, 6, 10}
	h := newStoreHeap(len(scores), scoreDesc)
	for _, s := range scores {
		h.add(makeScoreDoc(s))
	}

	prev := -1.0
	for len(h.heap) > 0 {
		doc := h.removeLast()
		if doc.Score < prev {
			t.Errorf("removeLast out of order: got %.2f after %.2f", doc.Score, prev)
		}
		prev = doc.Score
		checkTernaryInvariant(t, h)
	}
}

// TestTernaryHeapAddNotExceedingSize checks that size enforcement works:
// adding more than k docs keeps the best k and evicts the rest.
func TestTernaryHeapAddNotExceedingSize(t *testing.T) {
	const k = 5
	h := newStoreHeap(k, scoreDesc)
	for i := 1; i <= 10; i++ {
		evicted := h.AddNotExceedingSize(makeScoreDoc(float64(i)), k)
		if i <= k {
			if evicted != nil {
				t.Errorf("insert %d: expected no eviction, got score %.2f", i, evicted.Score)
			}
		} else {
			if evicted == nil {
				t.Errorf("insert %d: expected eviction", i)
			}
		}
		checkTernaryInvariant(t, h)
	}
	if h.Len() != k {
		t.Errorf("heap size %d, want %d", h.Len(), k)
	}
	// The heap should hold the best k scores (6..10); root is their minimum.
	if h.heap[0].Score != 6.0 {
		t.Errorf("root score %.2f, want 6.00 (worst of top-%d)", h.heap[0].Score, k)
	}
}

// TestTernaryHeapFinalOrder verifies that Final(0, ...) returns results in
// descending score order (best first).
func TestTernaryHeapFinalOrder(t *testing.T) {
	scores := []float64{3, 1, 4, 1, 5, 9, 2, 6, 5, 3}
	h := newStoreHeap(len(scores), scoreDesc)
	for _, s := range scores {
		h.add(makeScoreDoc(s))
	}

	fixup := func(*search.DocumentMatch) error { return nil }
	result, err := h.Final(0, fixup)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != len(scores) {
		t.Fatalf("len(result)=%d, want %d", len(result), len(scores))
	}
	for i := 1; i < len(result); i++ {
		if result[i].Score > result[i-1].Score {
			t.Errorf("result[%d]=%.2f > result[%d]=%.2f (not sorted descending)",
				i, result[i].Score, i-1, result[i-1].Score)
		}
	}
}

// TestTernaryHeapFinalWithSkip verifies that Final(skip, ...) skips the skip
// worst results and returns the rest best-first.
func TestTernaryHeapFinalWithSkip(t *testing.T) {
	// Heap holds 1..5; skip=2 should return scores [3, 2, 1] (skipping 4 and 5).
	scores := []float64{1, 2, 3, 4, 5}
	h := newStoreHeap(len(scores), scoreDesc)
	for _, s := range scores {
		h.add(makeScoreDoc(s))
	}

	fixup := func(*search.DocumentMatch) error { return nil }
	result, err := h.Final(2, fixup)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("len(result)=%d, want 3", len(result))
	}
	want := []float64{3, 2, 1}
	for i, w := range want {
		if result[i].Score != w {
			t.Errorf("result[%d]=%.2f, want %.2f", i, result[i].Score, w)
		}
	}
}

// TestTernaryHeapLargeN exercises multiple levels of siftDown (n > 3^3 = 27).
func TestTernaryHeapLargeN(t *testing.T) {
	const n = 200
	h := newStoreHeap(n, scoreDesc)
	for i := n; i >= 1; i-- { // insert descending to stress siftUp
		h.add(makeScoreDoc(float64(i)))
		checkTernaryInvariant(t, h)
	}

	prev := -1.0
	count := 0
	for len(h.heap) > 0 {
		doc := h.removeLast()
		if doc.Score < prev {
			t.Errorf("removeLast out of order at position %d: %.2f after %.2f", count, doc.Score, prev)
		}
		prev = doc.Score
		count++
		checkTernaryInvariant(t, h)
	}
	if count != n {
		t.Errorf("extracted %d elements, want %d", count, n)
	}
}
