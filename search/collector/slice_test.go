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
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

func TestCollectStoreSliceRoundTrip(t *testing.T) {
	l := newStoreSlice(20, search.ScoreCompare)
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

func TestCollectStoreSliceAddNotExceedingSize(t *testing.T) {
	const k = 3
	l := newStoreSlice(k, search.ScoreCompare)
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

func TestCollectStoreSliceSkip(t *testing.T) {
	l := newStoreSlice(20, search.ScoreCompare)
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

func TestCollectStoreSliceSkipAll(t *testing.T) {
	l := newStoreSlice(10, search.ScoreCompare)
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

func TestCollectStoreSliceInternal(t *testing.T) {
	l := newStoreSlice(10, search.ScoreCompare)
	for _, s := range []float64{3, 1, 4} {
		l.add(makeScoreDoc(s))
	}
	iv := l.Internal()
	if len(iv) != 3 {
		t.Fatalf("Internal len=%d want 3", len(iv))
	}
	// Slice is kept sorted best→worst, so Internal() is descending.
	want := []float64{4, 3, 1}
	for i, w := range want {
		if iv[i].Score != w {
			t.Errorf("Internal[%d]=%.2f want %.2f (descending from best)", i, iv[i].Score, w)
		}
	}
}

func TestCollectStoreSliceRemoveLast(t *testing.T) {
	l := newStoreSlice(10, search.ScoreCompare)
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

func TestCollectStoreSliceSingleElement(t *testing.T) {
	l := newStoreSlice(5, search.ScoreCompare)
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

func TestCollectStoreSliceEqualScores(t *testing.T) {
	l := newStoreSlice(10, search.ScoreCompare)
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

func TestCollectStoreSliceFixupError(t *testing.T) {
	l := newStoreSlice(10, search.ScoreCompare)
	l.add(makeScoreDoc(1.0))

	errFixup := func(*search.DocumentMatch) error {
		return errTestFixup
	}
	_, err := l.Final(0, errFixup)
	if err != errTestFixup {
		t.Errorf("Final fixup error not propagated: got %v", err)
	}
}
