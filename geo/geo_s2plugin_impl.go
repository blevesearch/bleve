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

package geo

import (
	"sync"

	index "github.com/blevesearch/bleve_index_api"

	"github.com/blevesearch/geo/s2"
)

// spatialPluginsMap is spatial plugin cache.
var (
	spatialPluginsMap = make(map[string]index.SpatialAnalyzerPlugin)
	pluginsMapLock    = sync.RWMutex{}
)

// RegisterSpatialAnalyzerPlugin registers the given plugin implementation.
func RegisterSpatialAnalyzerPlugin(plugin index.SpatialAnalyzerPlugin) {
	pluginsMapLock.Lock()
	spatialPluginsMap[plugin.Type()] = plugin
	pluginsMapLock.Unlock()
}

// GetSpatialAnalyzerPlugin retrieves the given implementation type.
func GetSpatialAnalyzerPlugin(typ string) index.SpatialAnalyzerPlugin {
	pluginsMapLock.RLock()
	rv := spatialPluginsMap[typ]
	pluginsMapLock.RUnlock()
	return rv
}

func init() {
	registerS2RegionTermIndexer()
}

func registerS2RegionTermIndexer() {
	// refer for detailed commentary on s2 options here
	// https://github.com/blevesearch/geo/blob/5734244d948a0ccf9a6b00e02d1a05f94cbc8849/s2/region_term_indexer.go#L92
	options := &s2.Options{}

	// maxLevel control the maximum size of the
	// S2Cells used to approximate regions.
	options.SetMaxLevel(16)

	// minLevel control the minimum size of the
	// S2Cells used to approximate regions.
	options.SetMinLevel(4)

	// levelMod value greater than 1 increases the effective branching
	// factor of the S2Cell hierarchy by skipping some levels.
	options.SetLevelMod(2)

	// maxCells controls the maximum number of cells
	// when approximating each s2 region.
	options.SetMaxCells(8)

	spatialPlugin := S2SpatialAnalyzerPlugin{
		s2Indexer: s2.NewRegionTermIndexerWithOptions(*options)}

	RegisterSpatialAnalyzerPlugin(&spatialPlugin)
}

// S2SpatialAnalyzerPlugin is an implementation of
// the index.SpatialAnalyzerPlugin interface.
type S2SpatialAnalyzerPlugin struct {
	s2Indexer *s2.RegionTermIndexer
}

func (s *S2SpatialAnalyzerPlugin) Type() string {
	return "s2"
}

func (s *S2SpatialAnalyzerPlugin) GetIndexTokens(queryShape index.GeoJSON) []string {
	var rv []string
	shapes := []index.GeoJSON{queryShape}
	if gc, ok := queryShape.(*geometryCollection); ok {
		shapes = gc.Shapes
	}

	for _, shape := range shapes {
		if s2t, ok := shape.(s2Tokenizable); ok {
			rv = append(rv, s2t.IndexTokens(s)...)
		}
	}

	return stripCoveringTerms(rv)
}

func (s *S2SpatialAnalyzerPlugin) GetQueryTokens(queryShape index.GeoJSON) []string {
	var rv []string
	shapes := []index.GeoJSON{queryShape}
	if gc, ok := queryShape.(*geometryCollection); ok {
		shapes = gc.Shapes
	}

	for _, shape := range shapes {
		if s2t, ok := shape.(s2Tokenizable); ok {
			rv = append(rv, s2t.QueryTokens(s)...)
		}
	}

	return stripCoveringTerms(rv)
}

// ------------------------------------------------------------------------
// s2Tokenizable is an optional interface for shapes that support
// the generation of s2 based tokens that can be used for both
// indexing and querying.

type s2Tokenizable interface {
	// IndexTokens returns the tokens for indexing.
	IndexTokens(*S2SpatialAnalyzerPlugin) []string

	// QueryTokens returns the tokens for searching.
	QueryTokens(*S2SpatialAnalyzerPlugin) []string
}

// ------------------------------------------------------------------------

func (p *point) IndexTokens(s *S2SpatialAnalyzerPlugin) []string {
	ll := s2.LatLngFromDegrees(p.Vertices[1], p.Vertices[0])
	point := s2.PointFromLatLng(ll)
	return s.s2Indexer.GetIndexTermsForPoint(point, "")
}

func (p *point) QueryTokens(s *S2SpatialAnalyzerPlugin) []string {
	ll := s2.LatLngFromDegrees(p.Vertices[1], p.Vertices[0])
	point := s2.PointFromLatLng(ll)
	return s.s2Indexer.GetQueryTermsForPoint(point, "")
}

// ------------------------------------------------------------------------

func (mp *multipoint) IndexTokens(s *S2SpatialAnalyzerPlugin) []string {
	var rv []string
	for _, point := range mp.Vertices {
		terms := s.s2Indexer.GetIndexTermsForPoint(s2.PointFromLatLng(
			s2.LatLngFromDegrees(point[1], point[0])), "")
		rv = append(rv, terms...)
	}
	return deduplicateTerms(rv)
}

func (mp *multipoint) QueryTokens(s *S2SpatialAnalyzerPlugin) []string {
	var rv []string
	for _, point := range mp.Vertices {
		terms := s.s2Indexer.GetQueryTermsForPoint(s2.PointFromLatLng(
			s2.LatLngFromDegrees(point[1], point[0])), "")
		rv = append(rv, terms...)
	}

	return deduplicateTerms(rv)
}

// ------------------------------------------------------------------------

func (ls *linestring) IndexTokens(s *S2SpatialAnalyzerPlugin) []string {
	pls := s2PolylinesFromCoordinates([][][]float64{ls.Vertices})
	if len(pls) == 1 {
		return s.s2Indexer.GetIndexTermsForRegion(pls[0].CapBound(), "")
	}
	return nil
}

func (ls *linestring) QueryTokens(s *S2SpatialAnalyzerPlugin) []string {
	pls := s2PolylinesFromCoordinates([][][]float64{ls.Vertices})
	if len(pls) == 1 {
		return s.s2Indexer.GetQueryTermsForRegion(pls[0].CapBound(), "")
	}
	return nil
}

// ------------------------------------------------------------------------

func (mls *multilinestring) IndexTokens(s *S2SpatialAnalyzerPlugin) []string {
	var rv []string
	polylines := s2PolylinesFromCoordinates(mls.Vertices)

	for _, pline := range polylines {
		terms := s.s2Indexer.GetIndexTermsForRegion(pline.CapBound(), "")
		rv = append(rv, terms...)
	}

	return deduplicateTerms(rv)
}

func (mls *multilinestring) QueryTokens(s *S2SpatialAnalyzerPlugin) []string {
	var rv []string
	polylines := s2PolylinesFromCoordinates(mls.Vertices)

	for _, pline := range polylines {
		terms := s.s2Indexer.GetQueryTermsForRegion(pline.CapBound(), "")
		rv = append(rv, terms...)
	}

	return deduplicateTerms(rv)
}

// ------------------------------------------------------------------------

func (mp *multipolygon) IndexTokens(s *S2SpatialAnalyzerPlugin) []string {
	var rv []string
	for _, loops := range mp.Coordinates() {
		s2polygon := s2PolygonFromCoordinates(loops)
		terms := s.s2Indexer.GetIndexTermsForRegion(s2polygon.CapBound(), "")
		rv = append(rv, terms...)
	}

	return rv
}

func (mp *multipolygon) QueryTokens(s *S2SpatialAnalyzerPlugin) []string {
	var rv []string
	for _, coords := range mp.Coordinates() {
		s2polygon := s2PolygonFromCoordinates(coords)
		terms := s.s2Indexer.GetQueryTermsForRegion(s2polygon.CapBound(), "")
		rv = append(rv, terms...)
	}

	return rv
}

// ------------------------------------------------------------------------

func (pgn *polygon) IndexTokens(s *S2SpatialAnalyzerPlugin) []string {
	s2polygon := s2PolygonFromCoordinates(pgn.Coordinates())
	return s.s2Indexer.GetIndexTermsForRegion(
		s2polygon.CapBound(), "")
}

func (pgn *polygon) QueryTokens(s *S2SpatialAnalyzerPlugin) []string {
	s2polygon := s2PolygonFromCoordinates(pgn.Coordinates())
	return s.s2Indexer.GetQueryTermsForRegion(
		s2polygon.CapBound(), "")
}

// ------------------------------------------------------------------------

func (c *circle) IndexTokens(s *S2SpatialAnalyzerPlugin) []string {
	cp := s2.PointFromLatLng(s2.LatLngFromDegrees(c.Vertices[1], c.Vertices[0]))
	angle := radiusInMetersToS1Angle(float64(c.RadiusInMeters))
	cap := s2.CapFromCenterAngle(cp, angle)

	return s.s2Indexer.GetIndexTermsForRegion(cap.CapBound(), "")
}

func (c *circle) QueryTokens(s *S2SpatialAnalyzerPlugin) []string {
	cp := s2.PointFromLatLng(s2.LatLngFromDegrees(c.Vertices[1], c.Vertices[0]))
	angle := radiusInMetersToS1Angle(float64(c.RadiusInMeters))
	cap := s2.CapFromCenterAngle(cp, angle)

	return s.s2Indexer.GetQueryTermsForRegion(cap.CapBound(), "")
}

// ------------------------------------------------------------------------

func (e *envelope) IndexTokens(s *S2SpatialAnalyzerPlugin) []string {
	s2rect := s2RectFromBounds(e.Vertices[0], e.Vertices[1])
	return s.s2Indexer.GetIndexTermsForRegion(s2rect.CapBound(), "")
}

func (e *envelope) QueryTokens(s *S2SpatialAnalyzerPlugin) []string {
	s2rect := s2RectFromBounds(e.Vertices[0], e.Vertices[1])
	return s.s2Indexer.GetQueryTermsForRegion(s2rect.CapBound(), "")
}

// ------------------------------------------------------------------------

func (p *Point) IndexTokens(s *S2SpatialAnalyzerPlugin) []string {
	return s.s2Indexer.GetIndexTermsForPoint(s2.PointFromLatLng(
		s2.LatLngFromDegrees(p.Lat, p.Lon)), "")
}

func (p *Point) QueryTokens(s *S2SpatialAnalyzerPlugin) []string {
	return nil
}

// ------------------------------------------------------------------------

func (pd *pointDistance) IndexTokens(s *S2SpatialAnalyzerPlugin) []string {
	return nil
}

func (pd *pointDistance) QueryTokens(s *S2SpatialAnalyzerPlugin) []string {
	// obtain the covering query region from the given points.
	queryRegion := s2.CapFromCenterAndRadius(pd.centerLat,
		pd.centerLon, pd.dist)

	// obtain the query terms for the query region.
	terms := s.s2Indexer.GetQueryTermsForRegion(queryRegion, "")

	return s2.FilterOutCoveringTerms(terms)
}

// ------------------------------------------------------------------------

func (bp *boundedPolygon) IndexTokens(s *S2SpatialAnalyzerPlugin) []string {
	return nil
}

func (bp *boundedPolygon) QueryTokens(s *S2SpatialAnalyzerPlugin) []string {
	vertices := make([]s2.Point, len(bp.coordinates))
	for i, point := range bp.coordinates {
		vertices[i] = s2.PointFromLatLng(
			s2.LatLngFromDegrees(point.Lat, point.Lon))
	}
	s2polygon := s2.PolygonFromOrientedLoops([]*s2.Loop{s2.LoopFromPoints(vertices)})

	// obtain the terms to be searched for the given polygon.
	terms := s.s2Indexer.GetQueryTermsForRegion(
		s2polygon.CapBound(), "")

	return s2.FilterOutCoveringTerms(terms)
}

// ------------------------------------------------------------------------

func (br *boundedRectangle) IndexTokens(s *S2SpatialAnalyzerPlugin) []string {
	return nil
}

func (br *boundedRectangle) QueryTokens(s *S2SpatialAnalyzerPlugin) []string {
	rect := s2.RectFromDegrees(br.minLat, br.minLon, br.maxLat, br.maxLon)

	// obtain the terms to be searched for the given bounding box.
	terms := s.s2Indexer.GetQueryTermsForRegion(rect, "")

	return s2.FilterOutCoveringTerms(terms)
}
