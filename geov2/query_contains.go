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
}

func NewContainsQuery(shape index.GeoJSON) Query {
	inner, cross := shape.Cells()

	return &containsQuery{
		innerCells: inner,
		crossCells: cross,
		shape:      shape,
		bBox:       shape.BoundingBox(),
	}
}

func (cq *containsQuery) Evaluate(geoData segment.GeoCellData) *util.Bitset {
	// startTime := time.Now()
	numDocs := int(geoData.NumDocs())
	exclude := geoData.Exclude()

	hits := util.NewBitset(numDocs, exclude)
	maybeHits := util.NewBitset(numDocs, exclude)

	innerScores := make([]uint64, numDocs)
	crossScores := make([]uint64, numDocs)

	docScores := geoData.DocScores()

	evaluator := NewQueryEvaluator(cq, geoData)

	evaluator.rangeScanInner(innerScores, crossScores)
	for i := 0; i < numDocs; i++ {
		if innerScores[i]+crossScores[i] == docScores[i] {
			hits.Add(i)
		}
	}

	evaluator.rangeScanCross(innerScores, crossScores)
	for i := 0; i < numDocs; i++ {
		if !hits.Contains(i) && innerScores[i]+crossScores[i] == docScores[i] {
			maybeHits.Add(i)
		}
	}

	// fmt.Printf("Contains query range scan took %v\n", time.Since(startTime))
	// fmt.Printf("After range scan: Hits count = %d, MaybeHits count = %d\n", hits.Count(), maybeHits.Count())
	// startTime = time.Now()
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

		if ok, err := cq.bBox.Contains(docBBox); err == nil && !ok {
			maybeHits.Remove(docNum)
		}
	}

	maybeHits.Iterate(boxFilter)
	// fmt.Printf("Box filter took %v\n", time.Since(startTime))
	// fmt.Printf("After bounding box filter: Hits count = %d, MaybeHits count = %d\n", hits.Count(), maybeHits.Count())
	// startTime = time.Now()

	shapeFilter := func(docNum int) {
		docShapeBytes, err := geoData.Shape(uint64(docNum))
		if docShapeBytes == nil || err != nil {
			return
		}

		docShape, err := geojson.ExtractShapesFromBytes(docShapeBytes, &reader, nil)
		if err != nil {
			return
		}

		if ok, err := cq.shape.Contains(docShape); err == nil && ok {
			hits.Add(docNum)
		}
	}

	maybeHits.Iterate(shapeFilter)
	// fmt.Printf("Shape filter took %v\n", time.Since(startTime))
	// fmt.Printf("After shape filter: Hits count = %d, time elapsed = %v\n", hits.Count(), time.Since(startTime))

	return hits
}

func (cq *containsQuery) InnerCells() []uint64 {
	return cq.innerCells
}

func (cq *containsQuery) CrossCells() []uint64 {
	return cq.crossCells
}
