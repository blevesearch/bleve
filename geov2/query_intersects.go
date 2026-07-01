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

type intersectsQuery struct {
	innerCells []uint64
	crossCells []uint64

	shape index.GeoJSON
	bBox  index.GeoJSON
}

func NewIntersectsQuery(shape index.GeoJSON) Query {
	inner, cross := shape.Cells()

	return &intersectsQuery{
		innerCells: inner,
		crossCells: cross,
		shape:      shape,
		bBox:       shape.BoundingBox(),
	}
}

func (iq *intersectsQuery) Evaluate(geoData segment.GeoShapeV2Data) *util.Bitset {
	numDocs := int(geoData.NumDocs())
	exclude := geoData.Exclude()

	// create bitsets for hits and maybeHits providing exclude to the bitset
	// which will make it impossible to set those bits
	hits := util.NewBitset(numDocs, exclude)
	maybeHits := util.NewBitset(numDocs, exclude)

	innerScores := make([]uint64, numDocs)
	crossScores := make([]uint64, numDocs)

	// create an evaluator instance to scan the query cells against the index cells
	evaluator := NewQueryEvaluator(iq, geoData)

	// scan and score the overlap of query inner cells with all index cells
	evaluator.rangeScanInner(innerScores, crossScores)

	// if there is any overlap of query inner cells with any of the index cells
	// then we have a quaranteed hit. Reset scores to reuse score arrays for the
	// next step
	for i := 0; i < numDocs; i++ {
		if innerScores[i] > 0 || crossScores[i] > 0 {
			hits.Add(i)
			innerScores[i] = 0
			crossScores[i] = 0
		}
	}

	// scan and score the overlap of query cross cells with all index cells
	evaluator.rangeScanCross(innerScores, crossScores)

	// if there is any overlap of query cross cells with any of the index inner
	// cells then we have a quaranteed hit, if there is any overlap of query cross
	// cells with any of the index cross cells then we have a maybe hit, otherwise
	// we have no hit.
	for i := 0; i < numDocs; i++ {
		if innerScores[i] > 0 && !hits.Contains(i) {
			hits.Add(i)
		} else if crossScores[i] > 0 && !hits.Contains(i) {
			maybeHits.Add(i)
		}
	}

	var reader *bytes.Reader

	// filter out any maybeHits that do not have a bounding box that
	// intersects the query bounding box
	boxFilter := func(docNum int) {
		docBBoxBytes, err := geoData.BoundingBox(uint64(docNum))
		if docBBoxBytes == nil || err != nil {
			return
		}

		docBBox, err := geojson.ExtractShapesFromBytes(docBBoxBytes, &reader, nil)
		if err != nil {
			return
		}

		if ok, err := docBBox.Intersects(iq.bBox); err == nil && !ok {
			maybeHits.Remove(docNum)
		}
	}

	maybeHits.Iterate(boxFilter)

	// filter out any maybeHits that do not have a shape that
	// intersects the query shape
	shapeFilter := func(docNum int) {
		docShapeBytes, err := geoData.Shape(uint64(docNum))
		if docShapeBytes == nil || err != nil {
			return
		}

		docShape, err := geojson.ExtractShapesFromBytes(docShapeBytes, &reader, nil)
		if err != nil {
			return
		}

		if ok, err := docShape.Intersects(iq.shape); err == nil && ok {
			hits.Add(docNum)
		}
	}

	maybeHits.Iterate(shapeFilter)

	return hits
}

func (iq *intersectsQuery) InnerCells() []uint64 {
	return iq.innerCells
}

func (iq *intersectsQuery) CrossCells() []uint64 {
	return iq.crossCells
}
