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

package collector

import (
	"errors"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

var errTestFixup = errors.New("fixup error")

// noFixup is a no-op fixup function used throughout the list tests.
var noFixup collectorFixup = func(*search.DocumentMatch) error { return nil }

// TestCollectStoreListRoundTrip verifies that add() keeps elements in
// ascending-score order (Front=worst, Back=best) and that Final(0, ...) returns
// them best-first.  This exercises the core insertion-sort invariant of the
// linked-list store, which had 0% coverage before this test.
func TestCollectStoreListRoundTrip(t *testing.T) {
	l := newStoreList(20, scoreDesc)
	for _, s := range []float64{3, 1, 4, 1, 5, 9, 2, 6} {
		l.add(makeScoreDoc(s))
	}
	if l.len() != 8 {
		t.Fatalf("len=%d want 8", l.len())
	}
	result, err := l.Final(0, noFixup)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 8 {
		t.Fatalf("Final len=%d want 8", len(result))
	}
	for i := 1; i < len(result); i++ {
		if result[i].Score > result[i-1].Score {
			t.Errorf("Final not descending: result[%d]=%.2f > result[%d]=%.2f",
				i, result[i].Score, i-1, result[i-1].Score)
		}
	}
}

// TestCollectStoreListAddNotExceedingSize verifies that AddNotExceedingSize caps
// the list at k elements by evicting the worst (lowest-score) element.
func TestCollectStoreListAddNotExceedingSize(t *testing.T) {
	const k = 3
	l := newStoreList(k, scoreDesc)
	var evictedScores []float64
	for _, s := range []float64{1, 5, 3, 7, 2} {
		ev := l.AddNotExceedingSize(makeScoreDoc(s), k)
		if ev != nil {
			evictedScores = append(evictedScores, ev.Score)
		}
	}
	if l.len() != k {
		t.Fatalf("list len=%d want %d after capping at k", l.len(), k)
	}
	// Inserted {1,5,3,7,2} with k=3 → evicted the 2 worst: 1 and 2.
	if len(evictedScores) != 2 {
		t.Fatalf("evicted %d docs want 2", len(evictedScores))
	}
	// Remaining best-3: {7, 5, 3} in descending order.
	result, err := l.Final(0, noFixup)
	if err != nil {
		t.Fatal(err)
	}
	want := []float64{7, 5, 3}
	for i, w := range want {
		if result[i].Score != w {
			t.Errorf("result[%d]=%.2f want %.2f", i, result[i].Score, w)
		}
	}
}

// TestCollectStoreListSkip verifies that Final(skip, ...) skips the top-skip
// best results and returns the remaining docs in descending order.
// This models pagination: skip=page*pageSize to start at a later page.
func TestCollectStoreListSkip(t *testing.T) {
	l := newStoreList(20, scoreDesc)
	for _, s := range []float64{1, 2, 3, 4, 5} {
		l.add(makeScoreDoc(s))
	}
	// skip=2 omits the 2 best (scores 5 and 4) → returns [3, 2, 1].
	result, err := l.Final(2, noFixup)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 3 {
		t.Fatalf("Final(skip=2) len=%d want 3", len(result))
	}
	want := []float64{3, 2, 1}
	for i, w := range want {
		if result[i].Score != w {
			t.Errorf("result[%d]=%.2f want %.2f", i, result[i].Score, w)
		}
	}
}

// TestCollectStoreListSkipAll verifies Final returns empty when skip ≥ len.
func TestCollectStoreListSkipAll(t *testing.T) {
	l := newStoreList(10, scoreDesc)
	for _, s := range []float64{1, 2, 3} {
		l.add(makeScoreDoc(s))
	}
	result, err := l.Final(10, noFixup) // skip > len
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Errorf("Final(skip=10) on 3-elem list returned %d docs, want 0", len(result))
	}
}

// TestCollectStoreListInternal verifies Internal() returns all elements in
// ascending-score order (Front to Back of the linked list).
func TestCollectStoreListInternal(t *testing.T) {
	l := newStoreList(10, scoreDesc)
	for _, s := range []float64{3, 1, 4} {
		l.add(makeScoreDoc(s))
	}
	iv := l.Internal()
	if len(iv) != 3 {
		t.Fatalf("Internal len=%d want 3", len(iv))
	}
	// Linked list: Front=worst→Back=best, so Internal() iterates Front→Back = ascending.
	want := []float64{1, 3, 4}
	for i, w := range want {
		if iv[i].Score != w {
			t.Errorf("Internal[%d]=%.2f want %.2f (ascending from worst)", i, iv[i].Score, w)
		}
	}
}

// TestCollectStoreListRemoveLast verifies removeLast removes the Front element,
// which is the worst (lowest-score) document in the list.
func TestCollectStoreListRemoveLast(t *testing.T) {
	l := newStoreList(10, scoreDesc)
	for _, s := range []float64{3, 1, 5} {
		l.add(makeScoreDoc(s))
	}
	evicted := l.removeLast()
	if evicted.Score != 1 {
		t.Errorf("removeLast returned score=%.2f, want 1.0 (the worst)", evicted.Score)
	}
	if l.len() != 2 {
		t.Errorf("len=%d after removeLast, want 2", l.len())
	}
}

// TestCollectStoreListSingleElement verifies that a list with one element
// round-trips correctly through add / Final / Internal.
func TestCollectStoreListSingleElement(t *testing.T) {
	l := newStoreList(5, scoreDesc)
	l.add(makeScoreDoc(7.5))

	result, err := l.Final(0, noFixup)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 1 || result[0].Score != 7.5 {
		t.Errorf("single-element Final: got %v", result)
	}

	iv := l.Internal()
	if len(iv) != 1 || iv[0].Score != 7.5 {
		t.Errorf("single-element Internal: got %v", iv)
	}
}

// TestCollectStoreListEqualScores verifies correct handling of equal-scored
// documents: they should all be retained, and Final preserves their relative
// insertion order within equal-scored groups.
func TestCollectStoreListEqualScores(t *testing.T) {
	l := newStoreList(10, scoreDesc)
	for range 5 {
		l.add(makeScoreDoc(3.0))
	}
	if l.len() != 5 {
		t.Fatalf("len=%d after 5 equal-scored adds, want 5", l.len())
	}
	result, err := l.Final(0, noFixup)
	if err != nil {
		t.Fatal(err)
	}
	for _, dm := range result {
		if dm.Score != 3.0 {
			t.Errorf("expected all scores=3.0, got %.2f", dm.Score)
		}
	}
}

// TestCollectStoreListFixupError verifies that an error returned by the fixup
// function propagates correctly from Final.
func TestCollectStoreListFixupError(t *testing.T) {
	l := newStoreList(10, scoreDesc)
	l.add(makeScoreDoc(1.0))

	errFixup := func(*search.DocumentMatch) error {
		return errTestFixup
	}
	_, err := l.Final(0, errFixup)
	if err != errTestFixup {
		t.Errorf("Final fixup error not propagated: got %v", err)
	}
}
