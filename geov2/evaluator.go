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

func NewQueryEvaluator(query Query, geoData segment.GeoCellData) *queryEvaluator {
	return &queryEvaluator{
		queryInnerCells: query.InnerCells(),
		queryCrossCells: query.CrossCells(),
		innerCells:      geoData.InnerCells(),
		innerDocIds:     geoData.InnerDocIDs(),
		crossCells:      geoData.CrossCells(),
		crossDocIds:     geoData.CrossDocIDs(),
	}
}

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
	return lo // first index where arr[i] >= target, or len(arr) if none
}

func (qe *queryEvaluator) rangeScanInner(innerScores []uint64, crossScores []uint64) {
	for _, cell := range qe.queryInnerCells {
		rangeScanOne(cell, qe.innerCells, qe.innerDocIds, innerScores)
		rangeScanOne(cell, qe.crossCells, qe.crossDocIds, crossScores)
	}
}

func (qe *queryEvaluator) rangeScanCross(innerScores, crossScores []uint64) {
	for _, cell := range qe.queryCrossCells {
		rangeScanOne(cell, qe.innerCells, qe.innerDocIds, innerScores)
		rangeScanOne(cell, qe.crossCells, qe.crossDocIds, crossScores)
	}
}

func rangeScanOne(queryCell uint64, indexCells, docIds, scores []uint64) {
	minVal, maxVal := getCellSearchBounds(queryCell)
	cellLevel := getCellLevel(queryCell)

	start := binarySearchLeftmostGreaterOrEqual(indexCells, minVal)
	end := binarySearchLeftmostGreaterOrEqual(indexCells, maxVal+1) - 1

	for i := start; i <= end; i++ {
		id := docIds[i]
		val := indexCells[i]

		valLevel := getCellLevel(val)
		scores[id] += calcScore(cellLevel, valLevel)
	}

	for level := int(cellLevel) - 1; level >= 0; level-- {
		parentCell := getParentCell(queryCell, level)
		parentStart := binarySearchLeftmostGreaterOrEqual(indexCells, parentCell)

		for i := parentStart; i < len(indexCells) && indexCells[i] == parentCell; i++ {
			id := docIds[i]
			scores[id] += calcScore(cellLevel, uint64(level))
		}
	}
}
