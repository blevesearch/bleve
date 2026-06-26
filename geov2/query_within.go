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
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/geo/geojson"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

type withinQuery struct {
	innerCells []uint64
	crossCells []uint64

	shape index.GeoJSON
	bBox  index.GeoJSON

	score uint64
}

func NewWithinQuery(shape index.GeoJSON) Query {
	inner, cross := shape.Cells()

	score := CalcCellsScore(inner) + CalcCellsScore(cross)

	return &withinQuery{
		innerCells: inner,
		crossCells: cross,
		shape:      shape,
		bBox:       shape.BoundingBox(),
		score:      score,
	}
}

func (wq *withinQuery) Evaluate(geoData segment.GeoCellData) *util.Bitset {
	numDocs := int(geoData.NumDocs())
	exclude := geoData.Exclude()

	hits := util.NewBitset(numDocs, exclude)
	maybeHits := util.NewBitset(numDocs, exclude)

	innerScores := make([]uint64, numDocs)
	crossScores := make([]uint64, numDocs)

	evaluator := NewQueryEvaluator(wq, geoData)

	evaluator.rangeScanInner(innerScores, crossScores)
	evaluator.rangeScanCross(innerScores, crossScores)

	for i := 0; i < numDocs; i++ {
		if innerScores[i] == wq.score {
			hits.Add(i)
		} else if innerScores[i]+crossScores[i] == wq.score {
			maybeHits.Add(i)
		}
	}

	boxFilter := func(docNum int) {
		docBBoxBytes, err := geoData.BoundingBox(uint64(docNum))
		if docBBoxBytes == nil || err != nil {
			return
		}

		docBBox, err := geojson.ExtractShapesFromBytes(docBBoxBytes, nil, nil)
		if err != nil {
			return
		}

		if ok, err := docBBox.Contains(wq.bBox); err == nil && !ok {
			maybeHits.Remove(docNum)
		}
	}

	maybeHits.Iterate(boxFilter)

	shapeFilter := func(docNum int) {
		docShapeBytes, err := geoData.Shape(uint64(docNum))
		if docShapeBytes == nil || err != nil {
			return
		}

		docShape, err := geojson.ExtractShapesFromBytes(docShapeBytes, nil, nil)
		if err != nil {
			return
		}

		if ok, err := docShape.Contains(wq.shape); err == nil && ok {
			hits.Add(docNum)
		}
	}

	maybeHits.Iterate(shapeFilter)

	return hits
}

func (wq *withinQuery) InnerCells() []uint64 {
	return wq.innerCells
}

func (wq *withinQuery) CrossCells() []uint64 {
	return wq.crossCells
}
