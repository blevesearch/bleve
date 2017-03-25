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
	"bytes"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/geo"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/numeric"
	"github.com/blevesearch/bleve/search"
)

type GeoBoundingBoxSearcher struct {
	indexReader index.IndexReader
	field       string
	minLon      float64
	minLat      float64
	maxLon      float64
	maxLat      float64
	options     search.SearcherOptions

	rangeBounds []*geoRange

	searcher *DisjunctionSearcher
}

func NewGeoBoundingBoxSearcher(indexReader index.IndexReader, minLon, minLat, maxLon, maxLat float64, field string, boost float64, options search.SearcherOptions) (*GeoBoundingBoxSearcher, error) {
	rv := &GeoBoundingBoxSearcher{
		indexReader: indexReader,
		minLon:      minLon,
		minLat:      minLat,
		maxLon:      maxLon,
		maxLat:      maxLat,
		field:       field,
		options:     options,
	}
	rv.computeRange(0, (geo.GeoBits<<1)-1)

	var termsOnBoundary []search.Searcher
	var termsNotOnBoundary []search.Searcher
	for _, r := range rv.rangeBounds {
		ts, err := NewTermSearcher(indexReader, string(r.cell), field, 1.0, options)
		if err != nil {
			for _, s := range termsOnBoundary {
				_ = s.Close()
			}
			for _, s := range termsNotOnBoundary {
				_ = s.Close()
			}
			return nil, err
		}
		if r.boundary {
			termsOnBoundary = append(termsOnBoundary, ts)
		} else {
			termsNotOnBoundary = append(termsNotOnBoundary, ts)
		}
	}
	onBoundarySearcher, err := NewDisjunctionSearcher(indexReader, termsOnBoundary, 0, options)
	if err != nil {
		for _, s := range termsOnBoundary {
			_ = s.Close()
		}
		for _, s := range termsNotOnBoundary {
			_ = s.Close()
		}
		return nil, err
	}
	filterOnBoundarySearcher := NewFilteringSearcher(onBoundarySearcher, func(d *search.DocumentMatch) bool {
		var lon, lat float64
		var found bool
		err = indexReader.DocumentVisitFieldTerms(d.IndexInternalID, []string{field}, func(field string, term []byte) {
			// only consider the values which are shifted 0
			prefixCoded := numeric.PrefixCoded(term)
			var shift uint
			shift, err = prefixCoded.Shift()
			if err == nil && shift == 0 {
				var i64 int64
				i64, err = prefixCoded.Int64()
				if err == nil {
					lon = geo.MortonUnhashLon(uint64(i64))
					lat = geo.MortonUnhashLat(uint64(i64))
					found = true
				}
			}
		})
		if err == nil && found {
			return geo.BoundingBoxContains(lon, lat, minLon, minLat, maxLon, maxLat)
		}
		return false
	})
	notOnBoundarySearcher, err := NewDisjunctionSearcher(indexReader, termsNotOnBoundary, 0, options)
	if err != nil {
		for _, s := range termsOnBoundary {
			_ = s.Close()
		}
		for _, s := range termsNotOnBoundary {
			_ = s.Close()
		}
		_ = filterOnBoundarySearcher.Close()
		return nil, err
	}

	rv.searcher, err = NewDisjunctionSearcher(indexReader, []search.Searcher{filterOnBoundarySearcher, notOnBoundarySearcher}, 0, options)
	if err != nil {
		for _, s := range termsOnBoundary {
			_ = s.Close()
		}
		for _, s := range termsNotOnBoundary {
			_ = s.Close()
		}
		_ = filterOnBoundarySearcher.Close()
		_ = notOnBoundarySearcher.Close()
		return nil, err
	}
	return rv, nil
}

func (s *GeoBoundingBoxSearcher) Count() uint64 {
	return s.searcher.Count()
}

func (s *GeoBoundingBoxSearcher) Weight() float64 {
	return s.searcher.Weight()
}

func (s *GeoBoundingBoxSearcher) SetQueryNorm(qnorm float64) {
	s.searcher.SetQueryNorm(qnorm)
}

func (s *GeoBoundingBoxSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	return s.searcher.Next(ctx)
}

func (s *GeoBoundingBoxSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	return s.searcher.Advance(ctx, ID)
}

func (s *GeoBoundingBoxSearcher) Close() error {
	return s.searcher.Close()
}

func (s *GeoBoundingBoxSearcher) Min() int {
	return 0
}

func (s *GeoBoundingBoxSearcher) DocumentMatchPoolSize() int {
	return s.searcher.DocumentMatchPoolSize()
}

var geoMaxShift = document.GeoPrecisionStep * 4
var geoDetailLevel = ((geo.GeoBits << 1) - geoMaxShift) / 2

func (s *GeoBoundingBoxSearcher) computeRange(term uint64, shift uint) {
	split := term | uint64(0x1)<<shift
	var upperMax uint64
	if shift < 63 {
		upperMax = term | ((uint64(1) << (shift + 1)) - 1)
	} else {
		upperMax = 0xffffffffffffffff
	}
	lowerMax := split - 1
	s.relateAndRecurse(term, lowerMax, shift)
	s.relateAndRecurse(split, upperMax, shift)
}

func (s *GeoBoundingBoxSearcher) relateAndRecurse(start, end uint64, res uint) {
	minLon := geo.MortonUnhashLon(start)
	minLat := geo.MortonUnhashLat(start)
	maxLon := geo.MortonUnhashLon(end)
	maxLat := geo.MortonUnhashLat(end)

	level := ((geo.GeoBits << 1) - res) >> 1

	within := res%document.GeoPrecisionStep == 0 && s.cellWithin(minLon, minLat, maxLon, maxLat)
	if within || (level == geoDetailLevel && s.cellIntersectShape(minLon, minLat, maxLon, maxLat)) {
		s.rangeBounds = append(s.rangeBounds, newGeoRange(start, res, level, !within))
	} else if level < geoDetailLevel && s.cellIntersectsMBR(minLon, minLat, maxLon, maxLat) {
		s.computeRange(start, res-1)
	}
}

func (s *GeoBoundingBoxSearcher) cellWithin(minLon, minLat, maxLon, maxLat float64) bool {
	return geo.RectWithin(minLon, minLat, maxLon, maxLat, s.minLon, s.minLat, s.maxLon, s.maxLat)
}

func (s *GeoBoundingBoxSearcher) cellIntersectShape(minLon, minLat, maxLon, maxLat float64) bool {
	return s.cellIntersectsMBR(minLon, minLat, maxLon, maxLat)
}

func (s *GeoBoundingBoxSearcher) cellIntersectsMBR(minLon, minLat, maxLon, maxLat float64) bool {
	return geo.RectIntersects(minLon, minLat, maxLon, maxLat, s.minLon, s.minLat, s.maxLon, s.maxLat)
}

type geoRange struct {
	cell     []byte
	level    uint
	boundary bool
}

func newGeoRange(lower uint64, res uint, level uint, boundary bool) *geoRange {
	return &geoRange{
		level:    level,
		boundary: boundary,
		cell:     numeric.MustNewPrefixCodedInt64(int64(lower), res),
	}
}

func (r *geoRange) Compare(other *geoRange) int {
	return bytes.Compare(r.cell, other.cell)
}
