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

import "github.com/blevesearch/geo/s2"

// getCellSearchBounds takes a raw uint64 S2 cell ID and returns the
// minimum and maximum uint64 values defining its absolute spatial range.
func getCellSearchBounds(cellUint uint64) (min uint64, max uint64) {
	cellID := s2.CellID(cellUint)

	rangeMin := cellID.RangeMin()
	rangeMax := cellID.RangeMax()

	return uint64(rangeMin), uint64(rangeMax)
}

// Returns the level of the given S2 cell ID
func getCellLevel(cell uint64) uint64 {
	return uint64(s2.CellID(cell).Level())
}

// Returns the parent cell ID of the given S2 cell ID at the specified level
func getParentCell(cell uint64, level int) uint64 {
	return uint64(s2.CellID(cell).Parent(level))
}

// CalcCellsScore returns the total area of the given S2 cells
// in level 14 cell units (maxCellLevel in the geo repo's region coverer
// configuration - see geo/geojson/geojson_v2.go)
func CalcCellsScore(cells []uint64) uint64 {
	var score uint64
	for _, cell := range cells {
		score += calcScore(0, getCellLevel(cell))
	}
	return score
}
