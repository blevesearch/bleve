//  Copyright (c) 2022 Couchbase, Inc.
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
	"encoding/json"

	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

type Geometry struct {
	Shape    index.GeoJSON `json:"shape"`
	Relation string        `json:"relation"`
}

type GeoShapeQuery struct {
	Geometry Geometry `json:"geometry"`
	FieldVal string   `json:"field,omitempty"`
	BoostVal *Boost   `json:"boost,omitempty"`
}

func NewGeoShapeQuery(coordinates [][][][]float64, typ, relation string) *GeoShapeQuery {
	s, _, err := geo.NewGeoJsonShape(coordinates, typ)
	if err != nil {
		return nil
	}

	return &GeoShapeQuery{Geometry: Geometry{Shape: s, Relation: relation}}
}

func (q *GeoShapeQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *GeoShapeQuery) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *GeoShapeQuery) SetField(f string) {
	q.FieldVal = f
}

func (q *GeoShapeQuery) Field() string {
	return q.FieldVal
}

func (q *GeoShapeQuery) Searcher(i index.IndexReader,
	m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}

	return searcher.NewGeoShapeSearcher(i, q.Geometry.Shape, q.Geometry.Relation, field, q.BoostVal.Value(), options)
}

func (q *GeoShapeQuery) Validate() error {
	return nil
}

func (q *Geometry) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Shape    json.RawMessage `json:"shape"`
		Relation string          `json:"relation"`
	}{}

	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}

	q.Shape, err = geo.ParseGeoJSONShape(tmp.Shape)
	if err != nil {
		return err
	}
	q.Relation = tmp.Relation
	return nil
}
