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
	"sort"
	"testing"

	"github.com/blevesearch/geo/s2"
)

func TestBinarySearchLeftmostGreaterOrEqual(t *testing.T) {
	tests := []struct {
		name   string
		arr    []uint64
		target uint64
		want   int
	}{
		{"empty", nil, 5, 0},
		{"all less than target", []uint64{1, 2, 3}, 5, 3},
		{"all greater than target", []uint64{5, 6, 7}, 1, 0},
		{"exact match", []uint64{1, 3, 5, 7}, 5, 2},
		{"target between elements", []uint64{1, 3, 5, 7}, 4, 2},
		{"leftmost of duplicates", []uint64{1, 3, 3, 3, 5}, 3, 1},
		{"target before all", []uint64{2, 4, 6}, 0, 0},
		{"target equals last", []uint64{2, 4, 6}, 6, 2},
		{"single element hit", []uint64{9}, 9, 0},
		{"single element miss high", []uint64{9}, 10, 1},
	}
	for _, test := range tests {
		if got := binarySearchLeftmostGreaterOrEqual(test.arr, test.target); got != test.want {
			t.Errorf("%s: binarySearchLeftmostGreaterOrEqual(%v, %d) = %d, want %d",
				test.name, test.arr, test.target, got, test.want)
		}
	}
}

// TestRangeScanOne verifies the two scoring paths of rangeScanOne: index
// cells that fall within the query cell's range (the query cell itself and
// its descendants), and ancestor cells found by walking the query cell's
// parents (which lie outside the range).
func TestRangeScanOne(t *testing.T) {
	const queryLevel = 10

	queryCell := uint64(s2.CellIDFromFace(2).ChildBeginAtLevel(queryLevel))
	childCell := uint64(s2.CellID(queryCell).ChildBeginAtLevel(12)) // descendant, in range
	parentCell := uint64(s2.CellID(queryCell).Parent(5))            // ancestor, out of range
	unrelated := uint64(s2.CellIDFromFace(4).ChildBeginAtLevel(queryLevel))

	// assign each cell a distinct doc ID
	type entry struct {
		cell  uint64
		docID uint64
	}
	entries := []entry{
		{queryCell, 0},
		{childCell, 1},
		{parentCell, 2},
		{unrelated, 3},
	}

	// index cells must be sorted ascending, with docIds kept parallel
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].cell < entries[j].cell
	})
	indexCells := make([]uint64, len(entries))
	docIds := make([]uint64, len(entries))
	for i, e := range entries {
		indexCells[i] = e.cell
		docIds[i] = e.docID
	}

	scores := make([]uint64, len(entries))
	minVal, maxVal := getCellSearchBounds(queryCell)
	rangeScanOne(queryCell, minVal, maxVal, queryLevel, indexCells, docIds, scores)

	// docID 0 is the query cell itself: in range, equal levels
	if want := calcScore(queryLevel, queryLevel); scores[0] != want {
		t.Errorf("query cell score = %d, want %d", scores[0], want)
	}
	// docID 1 is a descendant at level 12: in range, index cell deeper
	if want := calcScore(queryLevel, 12); scores[1] != want {
		t.Errorf("descendant cell score = %d, want %d", scores[1], want)
	}
	// docID 2 is an ancestor at level 5: found via the parent walk
	if want := calcScore(queryLevel, 5); scores[2] != want {
		t.Errorf("ancestor cell score = %d, want %d", scores[2], want)
	}
	// docID 3 is unrelated (different face): must not be scored
	if scores[3] != 0 {
		t.Errorf("unrelated cell score = %d, want 0", scores[3])
	}
}

// TestRangeScanOneNoMatches confirms that a query cell with no overlapping
// index cells produces no scores, exercising the empty-range case where
// end < start.
func TestRangeScanOneNoMatches(t *testing.T) {
	const queryLevel = 10
	queryCell := uint64(s2.CellIDFromFace(0).ChildBeginAtLevel(queryLevel))

	// index cells entirely on a different, non-ancestor part of the tree
	indexCells := []uint64{
		uint64(s2.CellIDFromFace(5).ChildBeginAtLevel(queryLevel)),
		uint64(s2.CellIDFromFace(5).ChildBeginAtLevel(queryLevel).Next()),
	}
	docIds := []uint64{0, 1}
	scores := make([]uint64, 2)

	minVal, maxVal := getCellSearchBounds(queryCell)
	rangeScanOne(queryCell, minVal, maxVal, queryLevel, indexCells, docIds, scores)

	for i, s := range scores {
		if s != 0 {
			t.Errorf("expected no score for doc %d, got %d", i, s)
		}
	}
}
