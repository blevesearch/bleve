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

package searcher

import (
	"github.com/blevesearch/bleve/geo"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/numeric"
	"github.com/blevesearch/bleve/search"
)

type GeoPointDistanceSearcher struct {
	indexReader index.IndexReader
	field       string

	centerLon float64
	centerLat float64
	dist      float64

	options search.SearcherOptions

	searcher *FilteringSearcher
}

func NewGeoPointDistanceSearcher(indexReader index.IndexReader, centerLon, centerLat, dist float64, field string, boost float64, options search.SearcherOptions) (*GeoPointDistanceSearcher, error) {
	rv := &GeoPointDistanceSearcher{
		indexReader: indexReader,
		centerLon:   centerLon,
		centerLat:   centerLat,
		dist:        dist,
		field:       field,
		options:     options,
	}

	// compute bounding box containing the circle
	topLeftLon, topLeftLat, bottomRightLon, bottomRightLat := geo.ComputeBoundingBox(centerLon, centerLat, dist)

	var boxSearcher search.Searcher
	if bottomRightLon < topLeftLon {
		// cross date line, rewrite as two parts

		leftSearcher, err := NewGeoBoundingBoxSearcher(indexReader, -180, bottomRightLat, bottomRightLon, topLeftLat, field, boost, options, false)
		if err != nil {
			return nil, err
		}
		rightSearcher, err := NewGeoBoundingBoxSearcher(indexReader, topLeftLon, bottomRightLat, 180, topLeftLat, field, boost, options, false)
		if err != nil {
			_ = leftSearcher.Close()
			return nil, err
		}

		boxSearcher, err = NewDisjunctionSearcher(indexReader, []search.Searcher{leftSearcher, rightSearcher}, 0, options)
		if err != nil {
			_ = leftSearcher.Close()
			_ = rightSearcher.Close()
			return nil, err
		}
	} else {

		// build geoboundinggox searcher for that bounding box
		var err error
		boxSearcher, err = NewGeoBoundingBoxSearcher(indexReader, topLeftLon, bottomRightLat, bottomRightLon, topLeftLat, field, boost, options, false)
		if err != nil {
			return nil, err
		}
	}

	// wrap it in a filtering searcher which checks the actual distance
	rv.searcher = NewFilteringSearcher(boxSearcher, func(d *search.DocumentMatch) bool {
		var lon, lat float64
		var found bool
		err := indexReader.DocumentVisitFieldTerms(d.IndexInternalID, []string{field}, func(field string, term []byte) {
			// only consider the values which are shifted 0
			prefixCoded := numeric.PrefixCoded(term)
			shift, err := prefixCoded.Shift()
			if err == nil && shift == 0 {
				i64, err := prefixCoded.Int64()
				if err == nil {
					lon = geo.MortonUnhashLon(uint64(i64))
					lat = geo.MortonUnhashLat(uint64(i64))
					found = true
				}
			}
		})
		if err == nil && found {
			dist := geo.Haversin(lon, lat, rv.centerLon, rv.centerLat)
			if dist <= rv.dist/1000 {
				return true
			}
		}
		return false
	})

	return rv, nil
}

func (s *GeoPointDistanceSearcher) Count() uint64 {
	return s.searcher.Count()
}

func (s *GeoPointDistanceSearcher) Weight() float64 {
	return s.searcher.Weight()
}

func (s *GeoPointDistanceSearcher) SetQueryNorm(qnorm float64) {
	s.searcher.SetQueryNorm(qnorm)
}

func (s *GeoPointDistanceSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	return s.searcher.Next(ctx)
}

func (s *GeoPointDistanceSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	return s.searcher.Advance(ctx, ID)
}

func (s *GeoPointDistanceSearcher) Close() error {
	return s.searcher.Close()
}

func (s *GeoPointDistanceSearcher) Min() int {
	return 0
}

func (s *GeoPointDistanceSearcher) DocumentMatchPoolSize() int {
	return s.searcher.DocumentMatchPoolSize()
}
