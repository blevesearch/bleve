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

// TestBM25FieldNormTableValues verifies that every entry in the BM25 field-norm
// table matches the direct BM25 length-normalisation formula exactly.  This
// catches any divergence between the table-build loop and the formula used at
// query time (§25).
//
// The formula for table[fieldLen] is:
//
//	fieldLength = 1/norm²   where norm = float32(1/sqrt(fieldLen))
//	fieldNorm   = k1*(1 - b + b*fieldLength/avgDocLen)
func TestBM25FieldNormTableValues(t *testing.T) {
	const avgDocLen = 100.0
	table := getBM25FieldNormTable(avgDocLen)

	k1 := search.BM25_k1
	b := search.BM25_b

	for fieldLen := 1; fieldLen < bm25FieldNormCacheSize; fieldLen++ {
		norm := float64(float32(1.0 / math.Sqrt(float64(fieldLen))))
		fieldLength := 1 / (norm * norm)
		expected := k1 * (1 - b + b*fieldLength/avgDocLen)
		got := table[fieldLen]

		// The table stores float64 computed by the same expression, so this
		// must hold bit-for-bit; a tiny tolerance guards only against a
		// compiler reassociating the two spellings above.
		if math.Abs(got-expected)/expected > 1e-12 {
			t.Errorf("table[%d]: got %v, want %v", fieldLen, got, expected)
		}
	}
}

// TestBM25FieldNormTableIsExact verifies that scoring through the table gives
// the same result as the full float64 formula on the stored norm — the property
// the table exists to preserve now that §20 records exact field lengths.
func TestBM25FieldNormTableIsExact(t *testing.T) {
	const avgDocLen = 100.0
	table := getBM25FieldNormTable(avgDocLen)

	k1 := search.BM25_k1
	b := search.BM25_b

	for _, fieldLen := range []uint32{1, 2, 3, 7, 12, 40, 137, 999, 4095} {
		norm := float64(float32(1.0 / math.Sqrt(float64(fieldLen))))
		for freq := 1; freq < MaxSqrtCache; freq++ {
			tf := SqrtCache[freq]
			viaTable := tf * k1 / (tf + table[fieldLen])
			fieldLength := 1 / (norm * norm)
			viaFormula := tf * k1 / (tf + k1*(1-b+(b*fieldLength/avgDocLen)))
			if math.Abs(viaTable-viaFormula)/viaFormula > 1e-12 {
				t.Errorf("fieldLen=%d freq=%d: table %v vs formula %v",
					fieldLen, freq, viaTable, viaFormula)
			}
		}
	}
}

// TestBM25FieldNormTableDifferentAvgDocLen verifies that the table changes when
// avgDocLen changes — confirming the cache is keyed on avgDocLen.
func TestBM25FieldNormTableDifferentAvgDocLen(t *testing.T) {
	t1 := getBM25FieldNormTable(50.0)
	v1 := t1[3]
	t2 := getBM25FieldNormTable(200.0)
	v2 := t2[3]

	if v1 == v2 {
		t.Error("field-norm table with avgDocLen=50 and avgDocLen=200 returned identical entries — cache key broken")
	}
}
