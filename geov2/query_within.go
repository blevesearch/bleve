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

type withinQuery struct {
	innerCells []uint64
	crossCells []uint64

	shape index.GeoJSON
	bBox  index.GeoJSON
}

func NewWithinQuery(shape index.GeoJSON) Query {
	inner, cross := shape.QueryCells()

	return &withinQuery{
		innerCells: inner,
		crossCells: cross,
		shape:      shape,
		bBox:       shape.BoundingBox(),
	}
}

func (wq *withinQuery) Evaluate(geoData segment.GeoShapeV2Data) *util.Bitset {
	numDocs := int(geoData.NumDocs())
	exclude := geoData.Excluded()

	// create bitsets for hits and maybeHits providing exclude to the bitset
	// which will make it impossible to set those bits
	hits := util.NewBitset(numDocs, exclude)
	maybeHits := util.NewBitset(numDocs, exclude)

	// obtain zeroed score arrays from the segment-level pool and return
	// them once the evaluation is done
	innerScores := geoData.GetScoreArray()
	crossScores := geoData.GetScoreArray()
	defer geoData.PutScoreArray(innerScores)
	defer geoData.PutScoreArray(crossScores)

	docScoresInner, docScoresCross := geoData.DocScores()

	// create an evaluator instance to scan the query cells against the index cells
	evaluator := NewQueryEvaluator(wq, geoData)

	// scan and score the overlap of query inner cells with all index cells
	evaluator.rangeScanInner(innerScores, crossScores)

	// if all of the index cells are contained within the query inner cells,
	// then we have a guaranteed hit. Only consider documents with non zero
	// total scores as hits
	for i := 0; i < numDocs; i++ {
		if innerScores[i]+crossScores[i] == docScoresInner[i]+docScoresCross[i] &&
			innerScores[i]+crossScores[i] != 0 {
			hits.Add(i)
		}
	}

	// scan and score the overlap of query cross cells with all index cells
	evaluator.rangeScanCross(innerScores, crossScores)

	// A document is a maybe-hit once the accumulated score reaches at least
	// its own inner-cell score. Complete overlap of index and cross scores
	// may not always be possible due to the nature of region coverer being
	// non exhaustive in the sense that the boundaries are not always only
	// covered by the smallest possible cells.
	for i := 0; i < numDocs; i++ {
		if !hits.Contains(i) && innerScores[i]+crossScores[i] >= docScoresInner[i] && innerScores[i]+crossScores[i] != 0 {
			maybeHits.Add(i)
		}
	}

	var reader *bytes.Reader

	// filter out any maybeHits that do not have a bounding box that
	// is within the query bounding box
	boxFilter := func(docNum int) {
		docBBoxBytes, err := geoData.BoundingBox(uint64(docNum))
		if docBBoxBytes == nil || err != nil {
			return
		}

		docBBox, err := geojson.ExtractShapesFromBytes(docBBoxBytes, &reader, nil)
		if err != nil {
			return
		}

		if ok, err := wq.bBox.Contains(docBBox); err == nil && !ok {
			maybeHits.Remove(docNum)
		}
	}

	maybeHits.Iterate(boxFilter)

	// filter out any maybeHits that do not have a shape that
	// is within the query shape
	shapeFilter := func(docNum int) {
		docShapeBytes, err := geoData.Shape(uint64(docNum))
		if docShapeBytes == nil || err != nil {
			return
		}

		docShape, err := geojson.ExtractShapesFromBytes(docShapeBytes, &reader, nil)
		if err != nil {
			return
		}

		if ok, err := wq.shape.Contains(docShape); err == nil && ok {
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
