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

type Query interface {
	Evaluate(geoData segment.GeoCellData) *util.Bitset
	InnerCells() []uint64
	CrossCells() []uint64
}

func NewQuery(shape index.GeoJSON, relation string) Query {
	switch relation {
	case "contains":
		return NewContainsQuery(shape)
	case "intersects":
		return NewIntersectsQuery(shape)
	case "within":
		return NewWithinQuery(shape)
	default:
		return nil
	}
}
