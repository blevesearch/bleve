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

// Tests for the §25 BM25 field-norm table (bm25FieldNormTable).
//
// The table stores the pre-computed BM25 length-normalisation denominator for
// every exact analyzed field length, keyed on avgDocLen. These tests guard
// against:
//   - divergence between the table-build loop and the query-time formula
//   - a wrong value for a specific (freq, fieldLen) pair
//   - the fieldLen→fieldLength conversion drifting from the norm round-trip
//     that zapx's Posting.Norm() performs

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

// TestBM25FieldNormTableVsFormula verifies that scoring via the field-norm table
// matches the exact float64 formula for every (freq, fieldLen) pair in range.
func TestBM25FieldNormTableVsFormula(t *testing.T) {
	const avgDocLen = 10.0
	const tol = 1e-12

	table := getBM25FieldNormTable(avgDocLen)

	for freq := 1; freq < MaxSqrtCache; freq++ {
		for fieldLen := uint32(1); fieldLen < bm25FieldNormCacheSize; fieldLen++ {
			tf := SqrtCache[freq]
			got := tf * search.BM25_k1 / (tf + table[fieldLen])
			want := bm25Formula(freq, fieldLengthFromFieldLen(fieldLen), avgDocLen)
			if math.Abs(got-want)/want > tol {
				t.Fatalf("fieldLen=%d freq=%d: got %v want %v", fieldLen, freq, got, want)
			}
		}
	}
}

// TestBM25FieldNormTableMonotoneInFieldLen verifies the table is monotonically
// non-decreasing in field length: a longer field is penalized at least as much.
func TestBM25FieldNormTableMonotoneInFieldLen(t *testing.T) {
	const avgDocLen = 10.0
	table := getBM25FieldNormTable(avgDocLen)

	for fieldLen := 2; fieldLen < bm25FieldNormCacheSize; fieldLen++ {
		if table[fieldLen] < table[fieldLen-1] {
			t.Fatalf("table not monotone in fieldLen: table[%d]=%v < table[%d]=%v",
				fieldLen, table[fieldLen], fieldLen-1, table[fieldLen-1])
		}
	}
}

// TestBM25FieldNormTableMonotoneInFreq verifies that the resulting tfNorm is
// monotonically non-decreasing in frequency for every fixed field length: a
// higher-frequency term always has at least as high a tfNorm.
func TestBM25FieldNormTableMonotoneInFreq(t *testing.T) {
	const avgDocLen = 10.0
	table := getBM25FieldNormTable(avgDocLen)

	tfNorm := func(freq int, fieldLen uint32) float64 {
		tf := SqrtCache[freq]
		return tf * search.BM25_k1 / (tf + table[fieldLen])
	}

	for _, fieldLen := range []uint32{1, 3, 17, 250, 4095} {
		for freq := 2; freq < MaxSqrtCache; freq++ {
			if tfNorm(freq, fieldLen) < tfNorm(freq-1, fieldLen)-0.0001 {
				t.Errorf("tfNorm not monotone in freq at fieldLen=%d: freq %d = %f < freq %d = %f",
					fieldLen, freq, tfNorm(freq, fieldLen), freq-1, tfNorm(freq-1, fieldLen))
			}
		}
	}
}

// TestBM25FieldNormTableCached verifies that getBM25FieldNormTable returns the
// same pointer on repeated calls with the same avgDocLen (cache hit).
func TestBM25FieldNormTableCached(t *testing.T) {
	const avgDocLen = 15.0
	t1 := getBM25FieldNormTable(avgDocLen)
	t2 := getBM25FieldNormTable(avgDocLen)
	if t1 != t2 {
		t.Error("getBM25FieldNormTable returned different pointers for same avgDocLen (cache miss)")
	}
}

// TestFieldLengthFromFieldLenMatchesNormRoundTrip verifies that converting an
// exact field length to a float64 field length agrees with recovering it from
// the norm zapx stores — the invariant that lets the table path and the formula
// path produce the same score.
func TestFieldLengthFromFieldLenMatchesNormRoundTrip(t *testing.T) {
	for fieldLen := uint32(1); fieldLen < bm25FieldNormCacheSize; fieldLen++ {
		// zapx Posting.Norm() = float64(float32(1/sqrt(fieldLen)))
		norm := float64(float32(1.0 / math.Sqrt(float64(fieldLen))))
		if got, want := fieldLengthFromFieldLen(fieldLen), fieldLengthFromNorm(norm); got != want {
			t.Fatalf("fieldLen=%d: fieldLengthFromFieldLen=%v, fieldLengthFromNorm=%v",
				fieldLen, got, want)
		}
	}
}
