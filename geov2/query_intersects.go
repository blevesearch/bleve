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
	inner, cross := shape.QueryCells()

	return &intersectsQuery{
		innerCells: inner,
		crossCells: cross,
		shape:      shape,
		bBox:       shape.BoundingBox(),
	}
}

func (iq *intersectsQuery) Evaluate(geoData segment.GeoShapeV2Data) *util.Bitset {
	numDocs := int(geoData.NumDocs())
	exclude := geoData.Excluded()

	// create bitsets for hits and maybeHits providing exclude to the bitset
	// which will make it impossible to set those bits
	hits := util.NewBitset(numDocs, exclude)
	maybeHits := util.NewBitset(numDocs, exclude)

	// obtain zeroed score arrays from the segment-level pool and return
	// them once the evaluation is done
	innerScores := geoData.GetScoreMap()
	crossScores := geoData.GetScoreMap()
	defer geoData.PutScoreMap(innerScores)
	defer geoData.PutScoreMap(crossScores)

	// create an evaluator instance to scan the query cells against the index cells
	evaluator := NewQueryEvaluator(iq, geoData)

	// scan and score the overlap of query inner cells with all index cells
	evaluator.rangeScanInner(innerScores, crossScores)

	// if there is any overlap of query inner cells with any of the index cells
	// then we have a guaranteed hit. Reset scores to reuse score maps for the
	// next step
	forEachScoredDoc(innerScores, crossScores, func(id uint32, inner, cross uint64) {
		if inner > 0 || cross > 0 {
			hits.Add(int(id))
		}
	})
	clear(innerScores)
	clear(crossScores)

	// scan and score the overlap of query cross cells with all index cells
	evaluator.rangeScanCross(innerScores, crossScores)

	// if there is any overlap of query cross cells with any of the index inner
	// cells then we have a guaranteed hit, if there is any overlap of query cross
	// cells with any of the index cross cells then we have a maybe hit, otherwise
	// we have no hit.
	forEachScoredDoc(innerScores, crossScores, func(id uint32, inner, cross uint64) {
		docNum := int(id)
		if inner > 0 && !hits.Contains(docNum) {
			hits.Add(docNum)
		} else if cross > 0 && !hits.Contains(docNum) {
			maybeHits.Add(docNum)
		}
	})

	var reader *bytes.Reader

	// filter out any maybeHits that do not have a bounding box that
	// intersects the query bounding box
	boxFilter := func(docNum int) {
		docBBoxBytes, err := geoData.BoundingBox(uint32(docNum))
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
		docShapeBytes, err := geoData.Shape(uint32(docNum))
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
