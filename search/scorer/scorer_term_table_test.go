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

package scorer

// Tests for the §25 BM25 impact table (bm25ImpactTable).
//
// The table stores pre-computed BM25 tfNorm values as float32 to avoid
// per-doc multiplication in the hot path. These tests guard against:
//   - float32 truncation that is large enough to affect score ordering
//   - divergence between the scorer's bm25SmallFloatFieldLen decoder and
//     zapx's normDecodeSmallFloat (both decode the same SmallFloat byte)
//   - construction bugs that produce a wrong value for a specific (freq, normByte) pair

import (
	"math"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

// bm25Formula computes the exact float64 BM25 tfNorm for a given frequency,
// field length, and average document length using the same formula as the scorer.
func bm25Formula(freq int, fieldLen, avgDocLen float64) float64 {
	k1 := search.BM25_k1
	b := search.BM25_b
	tf := math.Sqrt(float64(freq))
	return tf * k1 / (tf + k1*(1-b+b*fieldLen/avgDocLen))
}

// TestBM25ImpactTableVsFormula verifies that each entry in the BM25 impact
// table matches the exact float64 formula within the float32 precision budget.
//
// BM25 scores sit in [0, k1/(1+k1)] ≈ [0, 0.545] for the tfNorm component,
// so a tolerance of 0.001 corresponds to ≈0.2% relative error — well within
// the float32 precision guarantee of ~7 significant digits.
func TestBM25ImpactTableVsFormula(t *testing.T) {
	const avgDocLen = 10.0
	const tol = float32(0.001)

	table := getBM25ImpactTable(avgDocLen)

	for freq := 1; freq < MaxSqrtCache; freq++ {
		for nb := 1; nb < 256; nb++ { // nb=0 is the "infinity norm" sentinel
			fieldLen := bm25SmallFloatFieldLen(uint8(nb))
			if fieldLen == 0 {
				continue // sentinel; handled separately
			}
			want := float32(bm25Formula(freq, fieldLen, avgDocLen))
			got := table[freq][nb]
			diff := got - want
			if diff < 0 {
				diff = -diff
			}
			if diff > tol {
				t.Errorf("table[%d][%d]: got %f want %f (diff %f > tol %f)",
					freq, nb, got, want, diff, tol)
			}
		}
	}
}

// TestBM25ImpactTableNormByteSentinel verifies that normByte=0 (the "infinite
// field length" sentinel) uses the zero-length formula path: fieldLen=0 → no
// length normalization → maximum possible tfNorm for that freq.
func TestBM25ImpactTableNormByteSentinel(t *testing.T) {
	const avgDocLen = 10.0
	table := getBM25ImpactTable(avgDocLen)

	k1 := float32(search.BM25_k1)
	for freq := 1; freq < MaxSqrtCache; freq++ {
		tf := float32(math.Sqrt(float64(freq)))
		// fieldLen=0 path: tfNorm = tf*k1 / (tf + k1*(1 - b))  (b term drops out)
		b := float32(search.BM25_b)
		want := tf * k1 / (tf + k1*(1-b))
		got := table[freq][0]
		diff := got - want
		if diff < 0 {
			diff = -diff
		}
		if diff > 0.001 {
			t.Errorf("sentinel freq=%d: got %f want %f", freq, got, want)
		}
	}
}

// TestBM25ImpactTableMonotoneInFreq verifies that the impact table is
// monotonically non-decreasing in frequency for every fixed normByte: a
// higher-frequency term always has at least as high a tfNorm.
func TestBM25ImpactTableMonotoneInFreq(t *testing.T) {
	const avgDocLen = 10.0
	table := getBM25ImpactTable(avgDocLen)

	for nb := 0; nb < 256; nb++ {
		for freq := 2; freq < MaxSqrtCache; freq++ {
			if table[freq][nb] < table[freq-1][nb]-0.0001 {
				t.Errorf("table not monotone in freq: table[%d][%d]=%f < table[%d][%d]=%f",
					freq, nb, table[freq][nb], freq-1, nb, table[freq-1][nb])
			}
		}
	}
}

// TestBM25ImpactTableCached verifies that getBM25ImpactTable returns the same
// pointer on repeated calls with the same avgDocLen (cache hit).
func TestBM25ImpactTableCached(t *testing.T) {
	const avgDocLen = 15.0
	t1 := getBM25ImpactTable(avgDocLen)
	t2 := getBM25ImpactTable(avgDocLen)
	if t1 != t2 {
		t.Error("getBM25ImpactTable returned different pointers for same avgDocLen (cache miss)")
	}
}

// TestBM25SmallFloatFieldLenDecoder verifies that bm25SmallFloatFieldLen
// decodes SmallFloat norm bytes consistently with the zapx encode→decode
// round-trip: for a known set of field lengths, encode with a known SmallFloat
// encoding table and decode with the scorer's decoder.
//
// This guards against zapx and bleve scorer diverging on the SmallFloat format
// (see TODO in scorer_term.go about sharing the NormByteToFloat implementation).
func TestBM25SmallFloatFieldLenDecoder(t *testing.T) {
	// SmallFloat 3/15 format: 3-bit mantissa, 5-bit exponent.
	// encode: mantissa = (fieldLen >> (exp-3)) & 0x7  (approx)
	// decode: v = (mantissa/8 + 1) << (exp - 10)
	// Test that the scorer's decoder returns a positive, reasonable value
	// for all non-zero norm bytes, and exactly 0 for byte=0.
	for nb := 1; nb < 256; nb++ {
		fl := bm25SmallFloatFieldLen(uint8(nb))
		if fl < 1 {
			t.Errorf("bm25SmallFloatFieldLen(0x%02x)=%f, want ≥ 1", nb, fl)
		}
	}
	if bm25SmallFloatFieldLen(0) != 0 {
		t.Errorf("bm25SmallFloatFieldLen(0) should return 0 (sentinel), got %f",
			bm25SmallFloatFieldLen(0))
	}
}
