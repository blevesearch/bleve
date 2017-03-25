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
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searcher"
)

type GeoPoint struct {
	Lon float64 `json:"lon,omitempty"`
	Lat float64 `json:"lat,omitempty"`
}

type GeoBoundingBoxQuery struct {
	TopLeft     *GeoPoint `json:"top_left,omitempty"`
	BottomRight *GeoPoint `json:"bottom_right,omitempty"`
	FieldVal    string    `json:"field,omitempty"`
	BoostVal    *Boost    `json:"boost,omitempty"`
}

func NewGeoBoundingBoxQuery(topLeftLon, topLeftLat, bottomRightLon, bottomRightLat float64) *GeoBoundingBoxQuery {
	return &GeoBoundingBoxQuery{
		TopLeft: &GeoPoint{
			Lon: topLeftLon,
			Lat: topLeftLat,
		},
		BottomRight: &GeoPoint{
			Lon: bottomRightLon,
			Lat: bottomRightLat,
		},
	}
}

func (q *GeoBoundingBoxQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *GeoBoundingBoxQuery) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *GeoBoundingBoxQuery) SetField(f string) {
	q.FieldVal = f
}

func (q *GeoBoundingBoxQuery) Field() string {
	return q.FieldVal
}

func (q *GeoBoundingBoxQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}

	if q.BottomRight.Lon < q.TopLeft.Lon {
		// cross date line, rewrite as two parts

		leftSearcher, err := searcher.NewGeoBoundingBoxSearcher(i, -180, q.BottomRight.Lat, q.BottomRight.Lon, q.TopLeft.Lat, field, q.BoostVal.Value(), options)
		if err != nil {
			return nil, err
		}
		rightSearcher, err := searcher.NewGeoBoundingBoxSearcher(i, q.TopLeft.Lon, q.BottomRight.Lat, 180, q.TopLeft.Lat, field, q.BoostVal.Value(), options)
		if err != nil {
			_ = leftSearcher.Close()
			return nil, err
		}

		return searcher.NewDisjunctionSearcher(i, []search.Searcher{leftSearcher, rightSearcher}, 0, options)
	}

	return searcher.NewGeoBoundingBoxSearcher(i, q.TopLeft.Lon, q.BottomRight.Lat, q.BottomRight.Lon, q.TopLeft.Lat, field, q.BoostVal.Value(), options)
}

func (q *GeoBoundingBoxQuery) Validate() error {
	return nil
}
