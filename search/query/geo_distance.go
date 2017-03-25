//  Copyright (c) 2017 Couchbase, Inc.
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
	"github.com/blevesearch/bleve/geo"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searcher"
)

type GeoDistanceQuery struct {
	Location *GeoPoint `json:"location,omitempty"`
	Distance string    `json:"distance,omitempty"`
	FieldVal string    `json:"field,omitempty"`
	BoostVal *Boost    `json:"boost,omitempty"`
}

func NewGeoDistanceQuery(lon, lat float64, distance string) *GeoDistanceQuery {
	return &GeoDistanceQuery{
		Location: &GeoPoint{
			Lon: lon,
			Lat: lat,
		},
		Distance: distance,
	}
}

func (q *GeoDistanceQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *GeoDistanceQuery) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *GeoDistanceQuery) SetField(f string) {
	q.FieldVal = f
}

func (q *GeoDistanceQuery) Field() string {
	return q.FieldVal
}

func (q *GeoDistanceQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}

	dist, err := geo.ParseDistance(q.Distance)
	if err != nil {
		return nil, err
	}

	return searcher.NewGeoPointDistanceSearcher(i, q.Location.Lon, q.Location.Lat, dist, field, q.BoostVal.Value(), options)
}

func (q *GeoDistanceQuery) Validate() error {
	return nil
}
