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
	"math/rand"
	"reflect"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

func roaringOf(vals ...uint32) *roaring.Bitmap {
	return roaring.BitmapOf(vals...)
}

func drainIterator(it BitsetIterator) []int {
	var rv []int
	for {
		v, ok := it.Next()
		if !ok {
			return rv
		}
		rv = append(rv, v)
	}
}

func collectIterate(b *Bitset) []int {
	var rv []int
	b.Iterate(func(v int) { rv = append(rv, v) })
	return rv
}

// naiveBits is the independent reference: it reads the lower level directly,
// bit by bit, ignoring the summary entirely. Both Iterate and Iterator are now
// summary-driven, so cross-checking them against each other would pass even if
// the summary were wrong -- they have to be checked against this instead.
func naiveBits(b *Bitset) []int {
	var rv []int
	for wordIdx, word := range b.data {
		for bit := 0; bit < 64; bit++ {
			if word&(1<<uint(bit)) != 0 {
				rv = append(rv, wordIdx<<6+bit)
			}
		}
	}
	return rv
}

// assertSummaryExact checks the invariant every mutating method must maintain:
// a summary bit is set if and only if its data word is non-zero.
func assertSummaryExact(t *testing.T, b *Bitset) {
	t.Helper()
	for wordIdx, word := range b.data {
		want := word != 0
		got := b.summary[wordIdx>>6]&(1<<uint(wordIdx&63)) != 0
		if got != want {
			t.Fatalf("summary bit for word %d is %v, want %v (word=%#x)",
				wordIdx, got, want, word)
		}
	}
	// and no summary bit may name a word that does not exist
	for i := len(b.data); i < len(b.summary)*64; i++ {
		if b.summary[i>>6]&(1<<uint(i&63)) != 0 {
			t.Fatalf("summary bit set for out-of-range word %d", i)
		}
	}
}

func TestBitsetIteratorEmpty(t *testing.T) {
	b := NewBitset(100, nil)
	if got := drainIterator(b.Iterator()); len(got) != 0 {
		t.Fatalf("expected no values, got %v", got)
	}

	// a zero-value iterator must be usable and empty: the reader relies on this
	// for segments that carry no data for the field
	var zero BitsetIterator
	if _, ok := zero.Next(); ok {
		t.Fatal("zero-value iterator yielded a value")
	}
}

func TestBitsetIteratorAscending(t *testing.T) {
	b := NewBitset(200, nil)
	// deliberately added out of order, and with word-boundary neighbours
	for _, v := range []int{130, 0, 63, 64, 65, 199, 1, 128} {
		b.Add(v)
	}

	want := []int{0, 1, 63, 64, 65, 128, 130, 199}
	if got := drainIterator(b.Iterator()); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestBitsetIteratorAllSet(t *testing.T) {
	const n = 200
	b := NewBitset(n, nil)
	for i := 0; i <= n; i++ {
		b.Add(i)
	}
	got := drainIterator(b.Iterator())
	if len(got) != n+1 {
		t.Fatalf("expected %d values, got %d", n+1, len(got))
	}
	for i, v := range got {
		if v != i {
			t.Fatalf("at %d: got %d", i, v)
		}
	}
}

func TestBitsetIteratorPastEndIsStable(t *testing.T) {
	b := NewBitset(10, nil)
	b.Add(3)

	it := b.Iterator()
	if v, ok := it.Next(); !ok || v != 3 {
		t.Fatalf("first Next gave (%d, %v)", v, ok)
	}
	// repeated calls past the end must keep returning false rather than
	// walking the word index off into the distance
	for i := 0; i < 100; i++ {
		if _, ok := it.Next(); ok {
			t.Fatalf("Next yielded a value after exhaustion (call %d)", i)
		}
	}
}

func TestBitsetIteratorRespectsExclude(t *testing.T) {
	excl := roaringOf(2, 5)
	b := NewBitset(10, excl)
	for i := 0; i <= 6; i++ {
		b.Add(i)
	}
	want := []int{0, 1, 3, 4, 6}
	if got := drainIterator(b.Iterator()); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestBitsetIteratorAdvanceTo(t *testing.T) {
	b := NewBitset(300, nil)
	for _, v := range []int{5, 63, 64, 100, 200, 250} {
		b.Add(v)
	}

	cases := []struct {
		name    string
		advance int
		want    []int
	}{
		{"before everything", 0, []int{5, 63, 64, 100, 200, 250}},
		{"exact hit", 64, []int{64, 100, 200, 250}},
		{"between values in the same word", 6, []int{63, 64, 100, 200, 250}},
		{"crosses a word boundary", 65, []int{100, 200, 250}},
		{"to the last value", 250, []int{250}},
		{"past the last value", 251, nil},
		{"past the end of the bitset", 100000, nil},
		{"negative is ignored", -5, []int{5, 63, 64, 100, 200, 250}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			it := b.Iterator()
			it.AdvanceTo(tc.advance)
			got := drainIterator(it)
			if len(got) == 0 && len(tc.want) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("AdvanceTo(%d): got %v, want %v", tc.advance, got, tc.want)
			}
		})
	}
}

func TestBitsetIteratorAdvanceToNeverGoesBackwards(t *testing.T) {
	b := NewBitset(300, nil)
	for _, v := range []int{5, 100, 200} {
		b.Add(v)
	}

	it := b.Iterator()
	it.AdvanceTo(100)
	if v, ok := it.Next(); !ok || v != 100 {
		t.Fatalf("expected 100, got (%d, %v)", v, ok)
	}
	// rewinding is not supported and must not resurrect consumed values
	it.AdvanceTo(0)
	if got := drainIterator(it); !reflect.DeepEqual(got, []int{200}) {
		t.Fatalf("expected [200] after a backwards advance, got %v", got)
	}
}

func TestBitsetIteratorAdvanceToWithinConsumedWord(t *testing.T) {
	b := NewBitset(64, nil)
	for _, v := range []int{1, 2, 3} {
		b.Add(v)
	}

	it := b.Iterator()
	if v, _ := it.Next(); v != 1 {
		t.Fatalf("expected 1, got %d", v)
	}
	// advancing inside the word we are already partway through
	it.AdvanceTo(3)
	if got := drainIterator(it); !reflect.DeepEqual(got, []int{3}) {
		t.Fatalf("got %v, want [3]", got)
	}
}

// TestBitsetIteratorMatchesIterate cross-checks the word-walking iterator
// against the pre-existing bit-by-bit Iterate over random contents, which is
// the cheapest way to catch an off-by-one in the trailing-zero arithmetic.
func TestBitsetIteratorMatchesIterate(t *testing.T) {
	rng := rand.New(rand.NewSource(1))

	for trial := 0; trial < 200; trial++ {
		maxVal := 1 + rng.Intn(500)
		b := NewBitset(maxVal, nil)
		for i := 0; i < rng.Intn(maxVal+1); i++ {
			b.Add(rng.Intn(maxVal + 1))
		}
		// interleave removals so the summary-clearing path is covered, and so
		// that words which empty out are actually produced
		for i := 0; i < rng.Intn(maxVal+1); i++ {
			b.Remove(rng.Intn(maxVal + 1))
		}

		assertSummaryExact(t, b)

		want := naiveBits(b)
		got := drainIterator(b.Iterator())
		if len(want) != 0 || len(got) != 0 {
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("trial %d (maxVal %d): iterator gave %v, naive scan gave %v",
					trial, maxVal, got, want)
			}
		}

		// Iterate is summary-driven too, so check it against the same reference
		gotIterate := collectIterate(b)
		if len(want) != 0 || len(gotIterate) != 0 {
			if !reflect.DeepEqual(gotIterate, want) {
				t.Fatalf("trial %d (maxVal %d): Iterate gave %v, naive scan gave %v",
					trial, maxVal, gotIterate, want)
			}
		}

		// and Count must agree with both
		if b.Count() != len(want) {
			t.Fatalf("trial %d: Count %d, but %d values", trial, b.Count(), len(want))
		}
	}
}

// TestBitsetIteratorAdvanceToMatchesLinearScan cross-checks AdvanceTo against a
// filter over the full value list.
func TestBitsetIteratorAdvanceToMatchesLinearScan(t *testing.T) {
	rng := rand.New(rand.NewSource(2))

	for trial := 0; trial < 200; trial++ {
		maxVal := 1 + rng.Intn(400)
		b := NewBitset(maxVal, nil)
		for i := 0; i < rng.Intn(maxVal+1); i++ {
			b.Add(rng.Intn(maxVal + 1))
		}
		all := naiveBits(b)

		target := rng.Intn(maxVal + 2)
		var want []int
		for _, v := range all {
			if v >= target {
				want = append(want, v)
			}
		}

		it := b.Iterator()
		it.AdvanceTo(target)
		got := drainIterator(it)

		if len(want) == 0 && len(got) == 0 {
			continue
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("trial %d: AdvanceTo(%d) gave %v, want %v", trial, target, got, want)
		}
	}
}

func TestBitsetSizeInBytes(t *testing.T) {
	// 639/64+1 = 10 data words, plus 1 summary word covering them
	b := NewBitset(639, nil)
	if got, want := b.SizeInBytes(), 11*8; got != want {
		t.Fatalf("got %d, want %d", got, want)
	}

	// the summary is 1/64th of the data, rounded up
	b = NewBitset(64*64*3-1, nil) // 192 data words
	if got, want := len(b.data), 192; got != want {
		t.Fatalf("data words: got %d, want %d", got, want)
	}
	if got, want := len(b.summary), 3; got != want {
		t.Fatalf("summary words: got %d, want %d", got, want)
	}
}

// TestBitsetSummaryInvariant exercises every mutating path and checks the
// summary stays exact after each.
func TestBitsetSummaryInvariant(t *testing.T) {
	b := NewBitset(500, nil)
	assertSummaryExact(t, b)

	for _, v := range []int{0, 63, 64, 65, 200, 499, 500} {
		b.Add(v)
		assertSummaryExact(t, b)
	}

	// removing one of two bits in a word must keep the summary bit set
	b.Add(66)
	assertSummaryExact(t, b)
	b.Remove(65)
	assertSummaryExact(t, b)
	if b.summary[1>>6]&(1<<1) == 0 {
		t.Fatal("summary bit cleared while word 1 still holds bits 64 and 66")
	}

	// and emptying a word entirely must clear it. Word 3 (bits 192-255) holds
	// only 200, so removing that is the case that actually exercises the
	// clearing path -- word 1 keeps bit 64 no matter what else is removed.
	b.Remove(200)
	assertSummaryExact(t, b)
	if b.summary[3>>6]&(1<<3) != 0 {
		t.Fatal("summary bit still set after word 3 was emptied")
	}
	b.Remove(66)
	assertSummaryExact(t, b)

	b.Clear()
	assertSummaryExact(t, b)
	if b.Count() != 0 {
		t.Fatalf("Clear left %d bits", b.Count())
	}

	// Invert rewrites every word, so the summary is rebuilt wholesale
	b.Add(5)
	b.Invert()
	assertSummaryExact(t, b)
	if b.Contains(5) {
		t.Fatal("Invert did not clear bit 5")
	}

	// and again with an exclude set, which Invert also has to honour
	excl := roaringOf(7, 300)
	b2 := NewBitset(500, excl)
	b2.Add(9)
	b2.Invert()
	assertSummaryExact(t, b2)
	if b2.Contains(7) || b2.Contains(300) {
		t.Fatal("Invert did not respect the exclude set")
	}
}
