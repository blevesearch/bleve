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
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

type disjointQuery struct {
	innerCells []uint64
	crossCells []uint64

	shape index.GeoJSON
	bBox  index.GeoJSON
}

func NewDisjointQuery(shape index.GeoJSON) Query {
	inner, cross := shape.Cells()

	return &disjointQuery{
		innerCells: inner,
		crossCells: cross,
		shape:      shape,
		bBox:       shape.BoundingBox(),
	}
}

func (dq *disjointQuery) Evaluate(geoData segment.GeoCellData) *util.Bitset {
	intersectsQuery := &intersectsQuery{
		innerCells: dq.innerCells,
		crossCells: dq.crossCells,
		shape:      dq.shape,
		bBox:       dq.bBox,
	}

	notHits := intersectsQuery.Evaluate(geoData)

	numDocs := int(geoData.NumDocs())
	exclude := geoData.Exclude()

	hits := util.NewBitset(numDocs, exclude)

	for i := 0; i < numDocs; i++ {
		if !notHits.Contains(i) {
			hits.Add(i)
		}
	}

	return hits
}

func (dq *disjointQuery) InnerCells() []uint64 {
	return dq.innerCells
}

func (dq *disjointQuery) CrossCells() []uint64 {
	return dq.crossCells
}
