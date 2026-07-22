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

package util

import (
	"reflect"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

func TestBitsetAddContains(t *testing.T) {
	b := NewBitset(100, nil)

	// values not yet added must not be present
	for _, v := range []int{0, 1, 63, 64, 65, 100} {
		if b.Contains(v) {
			t.Fatalf("expected %d to be absent from an empty bitset", v)
		}
	}

	// exercise word boundaries explicitly: 63 is the last bit of word 0,
	// 64 is the first bit of word 1, 65 the second
	added := []int{0, 1, 63, 64, 65, 100}
	for _, v := range added {
		b.Add(v)
	}
	for _, v := range added {
		if !b.Contains(v) {
			t.Fatalf("expected %d to be present after Add", v)
		}
	}

	// a value between two set bits must remain absent
	if b.Contains(50) {
		t.Fatalf("expected 50 to be absent")
	}
}

func TestBitsetAddDuplicate(t *testing.T) {
	b := NewBitset(100, nil)
	b.Add(42)
	b.Add(42)
	if got := b.Count(); got != 1 {
		t.Fatalf("expected duplicate Add to keep count at 1, got %d", got)
	}
	if !b.Contains(42) {
		t.Fatalf("expected 42 to be present")
	}
}

func TestBitsetRemove(t *testing.T) {
	b := NewBitset(100, nil)
	b.Add(10)
	b.Add(64)
	b.Remove(10)
	if b.Contains(10) {
		t.Fatalf("expected 10 to be absent after Remove")
	}
	if !b.Contains(64) {
		t.Fatalf("expected 64 to still be present")
	}
	// removing an absent value must be a no-op
	b.Remove(99)
	if b.Count() != 1 {
		t.Fatalf("expected count 1 after removing an absent value, got %d", b.Count())
	}
}

func TestBitsetCountAndClear(t *testing.T) {
	b := NewBitset(200, nil)
	for _, v := range []int{0, 5, 63, 64, 128, 200} {
		b.Add(v)
	}
	if got := b.Count(); got != 6 {
		t.Fatalf("expected count 6, got %d", got)
	}
	b.Clear()
	if got := b.Count(); got != 0 {
		t.Fatalf("expected count 0 after Clear, got %d", got)
	}
}

func TestBitsetIterateAscending(t *testing.T) {
	b := NewBitset(200, nil)
	// add out of order to confirm Iterate returns ascending order
	for _, v := range []int{130, 0, 64, 63, 7} {
		b.Add(v)
	}
	var got []int
	b.Iterate(func(v int) {
		got = append(got, v)
	})
	want := []int{0, 7, 63, 64, 130}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected Iterate to yield %v in ascending order, got %v", want, got)
	}
}

func TestBitsetExcludeBlocksAdd(t *testing.T) {
	exclude := roaring.New()
	exclude.AddInt(5)
	exclude.AddInt(70)

	b := NewBitset(100, exclude)
	b.Add(5)  // excluded, must be ignored
	b.Add(6)  // allowed
	b.Add(70) // excluded, must be ignored

	if b.Contains(5) {
		t.Fatalf("expected excluded value 5 to be blocked by Add")
	}
	if b.Contains(70) {
		t.Fatalf("expected excluded value 70 to be blocked by Add")
	}
	if !b.Contains(6) {
		t.Fatalf("expected non-excluded value 6 to be present")
	}
}

func TestBitsetInvertRespectsNumBits(t *testing.T) {
	// numDocs = 10 means the valid doc IDs are 0..9. NewBitset allocates a
	// full 64-bit word, so bits 10..63 are unused and must never surface.
	numDocs := 10
	b := NewBitset(numDocs, nil)
	b.Add(3)
	b.Add(7)

	b.Invert()

	var got []int
	b.Iterate(func(v int) {
		got = append(got, v)
	})

	// after inverting, exactly the doc IDs in [0, 10) that were NOT set
	// should be present - and nothing >= 10
	want := []int{0, 1, 2, 4, 5, 6, 8, 9}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("expected inverted bitset to yield %v, got %v", want, got)
	}
	if got := b.Count(); got != len(want) {
		t.Fatalf("expected inverted count %d, got %d", len(want), got)
	}
}

func TestBitsetInvertWordBoundaries(t *testing.T) {
	// exercise numBits at and around 64-bit word boundaries to make sure
	// the trailing-bit mask is computed correctly
	for _, numDocs := range []int{1, 63, 64, 65, 128, 129} {
		b := NewBitset(numDocs, nil)
		b.Invert()
		max := -1
		b.Iterate(func(v int) {
			if v > max {
				max = v
			}
		})
		// every valid doc ID [0, numDocs) should be present after inverting
		// an empty bitset, and none at or beyond numDocs
		if got := b.Count(); got != numDocs {
			t.Fatalf("numDocs=%d: expected inverted-empty count %d, got %d",
				numDocs, numDocs, got)
		}
		if max >= numDocs {
			t.Fatalf("numDocs=%d: Invert surfaced out-of-range value %d",
				numDocs, max)
		}
	}
}

func TestBitsetInvertClearsExcluded(t *testing.T) {
	// excluded docs must remain unset even after Invert, since they are
	// never valid hits
	exclude := roaring.New()
	exclude.AddInt(2)
	exclude.AddInt(8)

	b := NewBitset(10, exclude)
	b.Add(3)
	b.Invert()

	if b.Contains(2) || b.Contains(8) {
		t.Fatalf("expected excluded docs to stay unset after Invert")
	}
	if b.Contains(3) {
		t.Fatalf("expected the originally-set doc 3 to be cleared after Invert")
	}
	// a normal, non-excluded, originally-unset doc should now be set
	if !b.Contains(0) {
		t.Fatalf("expected doc 0 to be set after Invert")
	}
}
