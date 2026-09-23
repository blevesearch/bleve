package util

import "testing"

// TestAcquireBitsetIsZeroed is the test that matters for pooling: a recycled
// bitset must not carry bits from whoever used it last.
func TestAcquireBitsetIsZeroed(t *testing.T) {
	const maxVal = 4096

	for i := 0; i < 32; i++ {
		b := AcquireBitset(maxVal, nil)
		for v := 0; v <= maxVal; v++ {
			b.Add(v)
		}
		b.Release()
	}

	for i := 0; i < 32; i++ {
		b := AcquireBitset(maxVal, nil)
		if got := b.Count(); got != 0 {
			t.Fatalf("acquire %d returned %d stale bits", i, got)
		}
		b.Release()
	}
}

// TestAcquireBitsetSmallerReuse covers reusing a large pooled slice for a
// smaller bitset: the tail beyond the new size must be invisible.
func TestAcquireBitsetSmallerReuse(t *testing.T) {
	big := AcquireBitset(8192, nil)
	for v := 0; v <= 8192; v++ {
		big.Add(v)
	}
	big.Release()

	for i := 0; i < 16; i++ {
		small := AcquireBitset(100, nil)
		if got := small.Count(); got != 0 {
			t.Fatalf("small bitset saw %d stale bits", got)
		}
		small.Release()
	}
}

func TestBitsetReleaseIsIdempotent(t *testing.T) {
	b := AcquireBitset(100, nil)
	b.Add(5)
	b.Release()
	b.Release() // must not double-insert the same slice into the pool

	var nilBitset *Bitset
	nilBitset.Release() // must not panic
}

// TestAcquireBitsetSummaryIsPooled guards the interaction that broke when
// pooling and the two-level summary were combined: a pooled bitset has to carry
// both levels out of one allocation, and Release has to return the whole
// backing rather than the cap-limited data view.
func TestAcquireBitsetSummaryIsPooled(t *testing.T) {
	const maxVal = 100000
	words, summaryWords := bitsetSizes(maxVal)

	for i := 0; i < 16; i++ {
		b := AcquireBitset(maxVal, nil)

		if len(b.backing) != words+summaryWords {
			t.Fatalf("backing is %d words, want %d", len(b.backing), words+summaryWords)
		}
		if len(b.summary) != summaryWords {
			t.Fatalf("summary is %d words, want %d", len(b.summary), summaryWords)
		}
		// data must not be able to grow into the summary
		if cap(b.data) != words {
			t.Fatalf("data cap is %d, want %d", cap(b.data), words)
		}

		// exercise both levels, which is what panicked when summary was nil
		for v := 0; v <= maxVal; v += 7 {
			b.Add(v)
		}
		for wordIdx, word := range b.data {
			want := word != 0
			got := b.summary[wordIdx>>6]&(1<<uint(wordIdx&63)) != 0
			if got != want {
				t.Fatalf("summary bit for word %d is %v, want %v", wordIdx, got, want)
			}
		}

		b.Release()
	}
}

// TestBitsetReleaseReturnsFullBacking checks that a released bitset can serve a
// later request of the same size. Returning only the data view would leave the
// pooled slice too small, and every acquire would silently allocate instead.
func TestBitsetReleaseReturnsFullBacking(t *testing.T) {
	const maxVal = 100000
	words, summaryWords := bitsetSizes(maxVal)

	b := AcquireBitset(maxVal, nil)
	b.Add(1)
	b.Release()

	// drain the pool looking for a slice big enough to serve the same request
	for i := 0; i < 64; i++ {
		v := bitsetPool.Get()
		if v == nil {
			break
		}
		if cap(*v.(*[]uint64)) >= words+summaryWords {
			return // found it: Release returned the whole backing
		}
	}
	t.Fatalf("no pooled slice held %d words; Release did not return the full backing",
		words+summaryWords)
}
