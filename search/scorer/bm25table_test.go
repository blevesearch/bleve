// Copyright (c) 2024 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scorer

import (
	"math"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

// TestBM25ImpactTableValues verifies that every entry in the BM25 impact table
// matches the direct BM25 tf-norm formula within float32 rounding tolerance.
// This catches any divergence between the table-build loop and the formula used
// at query time (§25).
//
// The formula for table[freq][normByte] is:
//
//	tf = sqrt(freq)
//	fieldLen = bm25SmallFloatFieldLen(normByte)
//	tfNorm = tf * k1 / (tf + k1*(1 - b + b*fieldLen/avgDocLen))
func TestBM25ImpactTableValues(t *testing.T) {
	const avgDocLen = 100.0
	table := getBM25ImpactTable(avgDocLen)

	k1 := search.BM25_k1
	b := search.BM25_b

	// Check every (freq, normByte) pair that is within the table's domain.
	for freq := 1; freq < MaxSqrtCache; freq++ {
		for nb := 1; nb < 256; nb++ { // normByte=0 is a sentinel (norm→Inf path)
			tf := math.Sqrt(float64(freq))
			fieldLen := bm25SmallFloatFieldLen(uint8(nb))
			expected := float32(tf * k1 / (tf + k1*(1-b+b*fieldLen/avgDocLen)))
			got := table[freq][nb]

			// float32 rounding can cause a difference of up to 1 ULP (~6e-7).
			// We allow 1e-5 relative tolerance to be safe.
			diff := math.Abs(float64(got) - float64(expected))
			rel := diff / float64(expected)
			if expected > 0 && rel > 1e-5 {
				t.Errorf("table[%d][%d]: got %f, want %f (relErr %.2e)",
					freq, nb, got, expected, rel)
			}
		}
	}
}

// TestBM25ImpactTableNormByte0Sentinel verifies that normByte=0 (the
// "no NormByte available" sentinel) takes the Inf-fieldLen path:
// when fieldLen→0, the denominator approaches k1*(1-b), so
// tfNorm = tf*k1 / (tf + k1*(1-b)).
func TestBM25ImpactTableNormByte0Sentinel(t *testing.T) {
	const avgDocLen = 100.0
	table := getBM25ImpactTable(avgDocLen)

	k1 := search.BM25_k1
	b := search.BM25_b

	for freq := 1; freq < MaxSqrtCache; freq++ {
		tf := math.Sqrt(float64(freq))
		expected := float32(tf * k1 / (tf + k1*(1-b)))
		got := table[freq][0]

		diff := math.Abs(float64(got) - float64(expected))
		if float64(expected) > 0 {
			rel := diff / float64(expected)
			if rel > 1e-5 {
				t.Errorf("table[%d][0] (sentinel): got %f, want %f (relErr %.2e)",
					freq, got, expected, rel)
			}
		}
	}
}

// TestBM25ImpactTableDifferentAvgDocLen verifies that the table changes when
// avgDocLen changes — confirming the cache is keyed on avgDocLen.
func TestBM25ImpactTableDifferentAvgDocLen(t *testing.T) {
	t1 := getBM25ImpactTable(50.0)
	t2 := getBM25ImpactTable(200.0)

	// For any non-zero normByte with freq=1, the values must differ.
	if t1[1][0x5c] == t2[1][0x5c] {
		t.Error("impact table with avgDocLen=50 and avgDocLen=200 returned identical entries — cache key broken")
	}
}
