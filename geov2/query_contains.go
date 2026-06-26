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
	"bytes"

	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/geo/geojson"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

type containsQuery struct {
	innerCells []uint64
	crossCells []uint64

	shape index.GeoJSON
	bBox  index.GeoJSON

	score uint64
}

func NewContainsQuery(shape index.GeoJSON) Query {
	inner, cross := shape.Cells()

	score := CalcCellsScore(inner) + CalcCellsScore(cross)

	return &containsQuery{
		innerCells: inner,
		crossCells: cross,
		shape:      shape,
		bBox:       shape.BoundingBox(),
		score:      score,
	}
}

func (cq *containsQuery) Evaluate(geoData segment.GeoCellData) *util.Bitset {
	numDocs := int(geoData.NumDocs())
	exclude := geoData.Exclude()

	hits := util.NewBitset(numDocs, exclude)
	maybeHits := util.NewBitset(numDocs, exclude)

	innerScores := make([]uint64, numDocs)
	crossScores := make([]uint64, numDocs)

	evaluator := NewQueryEvaluator(cq, geoData)

	evaluator.rangeScanInner(innerScores, crossScores)
	evaluator.rangeScanCross(innerScores, crossScores)

	for i := 0; i < numDocs; i++ {
		if innerScores[i] == cq.score {
			hits.Add(i)
		} else if innerScores[i]+crossScores[i] == cq.score {
			maybeHits.Add(i)
		}
	}

	var reader *bytes.Reader

	boxFilter := func(docNum int) {
		docBBoxBytes, err := geoData.BoundingBox(uint64(docNum))
		if docBBoxBytes == nil || err != nil {
			return
		}

		docBBox, err := geojson.ExtractShapesFromBytes(docBBoxBytes, &reader, nil)
		if err != nil {
			return
		}

		if ok, err := docBBox.Contains(cq.bBox); err == nil && !ok {
			maybeHits.Remove(docNum)
		}
	}

	maybeHits.Iterate(boxFilter)

	shapeFilter := func(docNum int) {
		docShapeBytes, err := geoData.Shape(uint64(docNum))
		if docShapeBytes == nil || err != nil {
			return
		}

		docShape, err := geojson.ExtractShapesFromBytes(docShapeBytes, &reader, nil)
		if err != nil {
			return
		}

		if ok, err := docShape.Contains(cq.shape); err == nil && ok {
			hits.Add(docNum)
		}
	}

	maybeHits.Iterate(shapeFilter)

	return hits
}

func (cq *containsQuery) InnerCells() []uint64 {
	return cq.innerCells
}

func (cq *containsQuery) CrossCells() []uint64 {
	return cq.crossCells
}
