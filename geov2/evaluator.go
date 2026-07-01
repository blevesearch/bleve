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

import segment "github.com/blevesearch/scorch_segment_api/v2"

type queryEvaluator struct {
	queryInnerCells []uint64
	queryCrossCells []uint64

	innerCells  []uint64
	innerDocIds []uint64

	crossCells  []uint64
	crossDocIds []uint64
}

func NewQueryEvaluator(query Query, geoData segment.GeoShapeV2Data) *queryEvaluator {
	return &queryEvaluator{
		queryInnerCells: query.InnerCells(),
		queryCrossCells: query.CrossCells(),
		innerCells:      geoData.InnerCells(),
		innerDocIds:     geoData.InnerDocIDs(),
		crossCells:      geoData.CrossCells(),
		crossDocIds:     geoData.CrossDocIDs(),
	}
}

// find the leftmost index in arr where arr[i] >= target, or
// len(arr) if no such index exists
func binarySearchLeftmostGreaterOrEqual(arr []uint64, target uint64) int {
	lo, hi := 0, len(arr)
	for lo < hi {
		mid := int(uint(lo+hi) >> 1) // bitshift avoids the addition overflow risk
		if arr[mid] < target {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// scan and score the overlap of query inner cells with all index cells
func (qe *queryEvaluator) rangeScanInner(innerScores []uint64, crossScores []uint64) {
	for _, cell := range qe.queryInnerCells {
		minVal, maxVal := getCellSearchBounds(cell)
		cellLevel := getCellLevel(cell)
		rangeScanOne(cell, minVal, maxVal, cellLevel, qe.innerCells, qe.innerDocIds, innerScores)
		rangeScanOne(cell, minVal, maxVal, cellLevel, qe.crossCells, qe.crossDocIds, crossScores)
	}
}

// scan and score the overlap of query cross cells with all index cells
func (qe *queryEvaluator) rangeScanCross(innerScores, crossScores []uint64) {
	for _, cell := range qe.queryCrossCells {
		minVal, maxVal := getCellSearchBounds(cell)
		cellLevel := getCellLevel(cell)
		rangeScanOne(cell, minVal, maxVal, cellLevel, qe.innerCells, qe.innerDocIds, innerScores)
		rangeScanOne(cell, minVal, maxVal, cellLevel, qe.crossCells, qe.crossDocIds, crossScores)
	}
}

// scan and score the overlap of a single query cell with the given index cells
func rangeScanOne(queryCell uint64, minVal, maxVal, cellLevel uint64,
	indexCells, docIds, scores []uint64) {
	// find the range of index cells within the min/max bounds of the query cell
	// end will be < start if there are no index cells within the bounds
	start := binarySearchLeftmostGreaterOrEqual(indexCells, minVal)
	end := binarySearchLeftmostGreaterOrEqual(indexCells, maxVal+1) - 1

	// score all index cells within the bounds of the query cell
	for i := start; i <= end; i++ {
		id := docIds[i]
		val := indexCells[i]

		valLevel := getCellLevel(val)
		scores[id] += calcScore(cellLevel, valLevel)
	}

	// score all parent cells of the query cell that are present in the index
	// since parent cells are not within the min/max bounds
	for level := int(cellLevel) - 1; level >= 0; level-- {
		// get the parent cell of the query cell at this level
		parentCell := getParentCell(queryCell, level)
		// search for the leftmost index of this parent cell in the index cells
		parentStart := binarySearchLeftmostGreaterOrEqual(indexCells, parentCell)

		// score all index cells that match this parent cell exactly
		for i := parentStart; i < len(indexCells) && indexCells[i] == parentCell; i++ {
			id := docIds[i]
			scores[id] += calcScore(cellLevel, uint64(level))
		}
	}
}
