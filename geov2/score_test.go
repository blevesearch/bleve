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

package geov2

import (
	"testing"

	"github.com/blevesearch/geo/s2"
)

func TestPow4(t *testing.T) {
	tests := []struct {
		exp  uint64
		want uint64
	}{
		{0, 1},
		{1, 4},
		{10, 1048576},
		{15, 1073741824},
		{31, 4611686018427387904}, // last representable power of 4 in a uint64
		{32, 0},                   // out of range, guarded to 0
		{100, 0},                  // well out of range, guarded to 0
	}
	for _, test := range tests {
		if got := pow4(test.exp); got != test.want {
			t.Errorf("pow4(%d) = %d, want %d", test.exp, got, test.want)
		}
	}
}

func TestCalcScore(t *testing.T) {
	tests := []struct {
		name           string
		queryCellLevel uint64
		indexCellLevel uint64
		want           uint64
	}{
		{
			// equal levels: the overlap is a single cell at that level
			name: "equal levels", queryCellLevel: 16, indexCellLevel: 16, want: pow4(0),
		},
		{
			name: "equal levels shallow", queryCellLevel: 6, indexCellLevel: 6, want: pow4(10),
		},
		{
			// index cell is deeper (smaller): the overlap is the index cell
			name: "index deeper", queryCellLevel: 6, indexCellLevel: 10, want: pow4(6),
		},
		{
			// query cell is deeper (smaller): the overlap is the query cell
			name: "query deeper", queryCellLevel: 11, indexCellLevel: 8, want: pow4(5),
		},
		{
			name: "both at level 0", queryCellLevel: 0, indexCellLevel: 0, want: pow4(16),
		},
	}
	for _, test := range tests {
		if got := calcScore(test.queryCellLevel, test.indexCellLevel); got != test.want {
			t.Errorf("%s: calcScore(%d, %d) = %d, want %d",
				test.name, test.queryCellLevel, test.indexCellLevel, got, test.want)
		}
	}
}

// cellAtLevel returns a valid S2 cell ID at the requested level, derived
// deterministically from a fixed face so tests do not depend on any RNG.
func cellAtLevel(level int) uint64 {
	return uint64(s2.CellIDFromFace(1).ChildBeginAtLevel(level))
}

func TestCalcCellsScore(t *testing.T) {
	// build cells at known levels; each contributes calcScore(0, level)
	// which equals pow4(16 - level)
	cells := []uint64{
		cellAtLevel(16), // contributes pow4(0) = 1
		cellAtLevel(15), // contributes pow4(1) = 4
		cellAtLevel(14), // contributes pow4(2) = 16
	}
	want := pow4(0) + pow4(1) + pow4(2)
	if got := CalcCellsScore(cells); got != want {
		t.Fatalf("CalcCellsScore = %d, want %d", got, want)
	}

	// an empty slice scores zero
	if got := CalcCellsScore(nil); got != 0 {
		t.Fatalf("CalcCellsScore(nil) = %d, want 0", got)
	}
}

func TestCellLevelAndParent(t *testing.T) {
	cell := cellAtLevel(12)
	if got := getCellLevel(cell); got != 12 {
		t.Fatalf("getCellLevel = %d, want 12", got)
	}
	parent := getParentCell(cell, 8)
	if got := getCellLevel(parent); got != 8 {
		t.Fatalf("getCellLevel(parent) = %d, want 8", got)
	}
	// the parent's range must contain the child cell
	minVal, maxVal := getCellSearchBounds(parent)
	if cell < minVal || cell > maxVal {
		t.Fatalf("expected child cell %d to fall within parent bounds [%d, %d]",
			cell, minVal, maxVal)
	}
}
