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

package query

import (
	"context"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

type GeoShapeV2Query struct {
	GeometryV2 Geometry `json:"geometry_v2,omitempty"`
	FieldVal   string   `json:"field,omitempty"`
	BoostVal   *Boost   `json:"boost,omitempty"`
}

func (q *GeoShapeV2Query) Searcher(ctx context.Context, i index.IndexReader,
	m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}

	ctx = context.WithValue(ctx, search.QueryTypeKey, search.Geo)

	return searcher.NewGeoShapeV2Searcher(ctx, i, q.GeometryV2.Shape,
		q.GeometryV2.Relation, field, q.BoostVal.Value(), options)
}

func (q *GeoShapeV2Query) Field() string {
	return q.FieldVal
}

func (q *GeoShapeV2Query) SetField(f string) {
	q.FieldVal = f
}
