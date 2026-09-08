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

package numericv2

import (
	"math"
	"sort"
	"testing"

	"github.com/blevesearch/bleve/v2/numeric"
)

// TestEncodeIsMonotone checks that Encode preserves float64 ordering across the
// awkward regions: negatives, zero, denormals and the infinities.
func TestEncodeIsMonotone(t *testing.T) {
	vals := []float64{
		math.Inf(-1),
		-math.MaxFloat64,
		-1e300, -1e10, -1.5, -1, -0.5,
		-math.SmallestNonzeroFloat64,
		0,
		math.SmallestNonzeroFloat64,
		0.5, 1, 1.5, 1e10, 1e300,
		math.MaxFloat64,
		math.Inf(1),
	}

	if !sort.SliceIsSorted(vals, func(i, j int) bool { return vals[i] < vals[j] }) {
		t.Fatal("test input is not sorted")
	}

	for i := 1; i < len(vals); i++ {
		prev, cur := Encode(vals[i-1]), Encode(vals[i])
		if prev >= cur {
			t.Fatalf("Encode not monotone at %v -> %v: %d >= %d",
				vals[i-1], vals[i], prev, cur)
		}
	}

	// -0.0 and +0.0 compare equal as floats but are distinct bit patterns;
	// what matters is that neither breaks ordering against its neighbours
	if Encode(math.Copysign(0, -1)) > Encode(0) {
		t.Fatal("negative zero encodes above positive zero")
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	for _, f := range []float64{-1e300, -1.5, -1, 0, 0.5, 1, 42, 1e300} {
		if got := Decode(Encode(f)); got != f {
			t.Fatalf("round trip of %v gave %v", f, got)
		}
	}
}

func f64(v float64) *float64 { return &v }
func b(v bool) *bool         { return &v }

// TestBoundsMatchesInvertedPath is the load-bearing test for the encoding: it
// recomputes the int64 bounds exactly the way NewNumericRangeSearcher does and
// requires Bounds to agree after the sign-bit lift. If these ever diverge, the
// two numeric paths silently return different hits for the same query.
func TestBoundsMatchesInvertedPath(t *testing.T) {
	// mirror of the arithmetic at the top of NewNumericRangeSearcher
	reference := func(min, max *float64, incMin, incMax *bool) (int64, int64) {
		if min == nil {
			negInf := math.Inf(-1)
			min = &negInf
		}
		if max == nil {
			inf := math.Inf(1)
			max = &inf
		}
		if incMin == nil {
			d := true
			incMin = &d
		}
		if incMax == nil {
			d := false
			incMax = &d
		}
		minInt64 := numeric.Float64ToInt64(*min)
		if !*incMin && minInt64 != math.MaxInt64 {
			minInt64++
		}
		maxInt64 := numeric.Float64ToInt64(*max)
		if !*incMax && maxInt64 != math.MinInt64 {
			maxInt64--
		}
		return minInt64, maxInt64
	}

	mins := []*float64{nil, f64(math.Inf(-1)), f64(-1e300), f64(-1), f64(0), f64(1), f64(42.5), f64(math.MaxFloat64)}
	maxs := []*float64{nil, f64(math.Inf(1)), f64(-1e300), f64(-1), f64(0), f64(1), f64(42.5), f64(math.MaxFloat64)}
	incs := []*bool{nil, b(true), b(false)}

	for _, min := range mins {
		for _, max := range maxs {
			for _, incMin := range incs {
				for _, incMax := range incs {
					wantLo, wantHi := reference(min, max, incMin, incMax)
					gotLo, gotHi := Bounds(min, max, incMin, incMax)

					if gotLo != EncodeInt64(wantLo) {
						t.Fatalf("lo mismatch for [%v,%v] inc(%v,%v): got %d, want %d",
							deref(min), deref(max), derefB(incMin), derefB(incMax),
							gotLo, EncodeInt64(wantLo))
					}
					if gotHi != EncodeInt64(wantHi) {
						t.Fatalf("hi mismatch for [%v,%v] inc(%v,%v): got %d, want %d",
							deref(min), deref(max), derefB(incMin), derefB(incMax),
							gotHi, EncodeInt64(wantHi))
					}
				}
			}
		}
	}
}

// TestBoundsExtremeGuards pins down what the MaxInt64/MinInt64 guards in
// Bounds actually protect. They are not infinity guards: Float64ToInt64(+Inf)
// is 0x7FF0000000000000, comfortably short of MaxInt64, so an exclusive bound
// at an infinity increments normally -- into a NaN bit pattern, exactly as the
// inverted-index path does. The int64 extremes correspond to NaN payloads, so
// the guards are only reachable through NaN, which Validate rejects. They still
// have to stay, for bit-exact parity with NewNumericRangeSearcher.
func TestBoundsExtremeGuards(t *testing.T) {
	// the floats that actually sit at the int64 extremes are NaNs
	maxKey := math.Float64frombits(0x7FFFFFFFFFFFFFFF)
	minKey := math.Float64frombits(0xFFFFFFFFFFFFFFFF)

	if got := numeric.Float64ToInt64(maxKey); got != math.MaxInt64 {
		t.Fatalf("expected maxKey to map to MaxInt64, got %d", got)
	}
	if got := numeric.Float64ToInt64(minKey); got != math.MinInt64 {
		t.Fatalf("expected minKey to map to MinInt64, got %d", got)
	}

	// an exclusive minimum at the top must not wrap to zero
	lo, _ := Bounds(&maxKey, nil, b(false), nil)
	if lo != EncodeInt64(math.MaxInt64) {
		t.Fatalf("exclusive min at the int64 max wrapped: got %d, want %d",
			lo, uint64(EncodeInt64(math.MaxInt64)))
	}

	// an exclusive maximum at the bottom must not wrap to the top
	_, hi := Bounds(nil, &minKey, nil, b(false))
	if hi != EncodeInt64(math.MinInt64) {
		t.Fatalf("exclusive max at the int64 min wrapped: got %d, want %d",
			hi, uint64(EncodeInt64(math.MinInt64)))
	}

	// and the infinities, which do increment, must still move upward
	loInf, _ := Bounds(f64(math.Inf(1)), nil, b(false), nil)
	if loInf <= Encode(math.Inf(1)) {
		t.Fatalf("exclusive min at +Inf did not move upward: got %d", loInf)
	}
	_, hiInf := Bounds(nil, f64(math.Inf(-1)), nil, b(false))
	if hiInf >= Encode(math.Inf(-1)) {
		t.Fatalf("exclusive max at -Inf did not move downward: got %d", hiInf)
	}
}

// TestBoundsExclusiveIsAdjacentFloat documents that a step in this space moves
// to the neighbouring representable float64, so exclusive bounds are exact.
func TestBoundsExclusiveIsAdjacentFloat(t *testing.T) {
	lo, _ := Bounds(f64(1.0), nil, b(false), nil)
	if got, want := Decode(lo), math.Nextafter(1.0, math.Inf(1)); got != want {
		t.Fatalf("exclusive min above 1.0: got %v, want %v", got, want)
	}

	_, hi := Bounds(nil, f64(1.0), nil, b(false))
	if got, want := Decode(hi), math.Nextafter(1.0, math.Inf(-1)); got != want {
		t.Fatalf("exclusive max below 1.0: got %v, want %v", got, want)
	}
}

// TestBoundsEmptyRange checks that an inverted or empty range yields lo > hi,
// which Evaluate relies on to short-circuit.
func TestBoundsEmptyRange(t *testing.T) {
	// min == max with both endpoints exclusive is empty
	lo, hi := Bounds(f64(5), f64(5), b(false), b(false))
	if lo <= hi {
		t.Fatalf("expected an empty range, got lo=%d hi=%d", lo, hi)
	}

	// an inverted range is empty
	lo, hi = Bounds(f64(10), f64(1), nil, nil)
	if lo <= hi {
		t.Fatalf("expected an empty range, got lo=%d hi=%d", lo, hi)
	}

	// min == max inclusive on both sides matches exactly one value
	lo, hi = Bounds(f64(5), f64(5), b(true), b(true))
	if lo != hi {
		t.Fatalf("expected a single-value range, got lo=%d hi=%d", lo, hi)
	}
}

func deref(f *float64) interface{} {
	if f == nil {
		return "nil"
	}
	return *f
}

func derefB(v *bool) interface{} {
	if v == nil {
		return "nil"
	}
	return *v
}
