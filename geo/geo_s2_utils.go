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
	// https://github.com/sreekanth-cb/geo/blob/806f1c56fffb418d53d2ea6ce6aabaa376355d67/s2/region_term_indexer.go#L92
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

	// If the index will only contain points (rather than regions), be sure
	// to set this flag.  This will generate smaller and faster queries that
	// are specialized for the points-only case.
	options.SetPointsOnly(true)

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

func (s *S2SpatialAnalyzerPlugin) GetIndexTokens(shape index.GeoJSON) []string {
	if shape.Type() == "point" {
		if point, ok := shape.(*Point); ok {
			// generate the tokens for indexing.
			return s.s2Indexer.GetIndexTermsForPoint(s2.PointFromLatLng(
				s2.LatLngFromDegrees(point.Lat, point.Lon)), "")
		}
	}
	return nil
}

func (s *S2SpatialAnalyzerPlugin) GetQueryTokens(shape index.GeoJSON) []string {
	if pd, ok := shape.(*pointDistance); ok {
		// obtain the covering query region from the given points.
		queryRegion := s2.CapFromCenterAndRadius(pd.centerLat, pd.centerLon, pd.dist)

		// obtain the query terms for the query region.
		terms := s.s2Indexer.GetQueryTermsForRegion(queryRegion, "")

		// since we index only one dimensional points, let's filter out
		// or prune our search time terms. This needs to be removed once
		// we start indexing 2 or higher dimensional shapes.
		return s2.FilterOutCoveringTerms(terms)
	}

	if br, ok := shape.(*boundedRectangle); ok {
		rect := s2.RectFromDegrees(br.minLat, br.minLon, br.maxLat, br.maxLon)

		// obtain the terms to be searched for the given bounding box.
		terms := s.s2Indexer.GetQueryTermsForRegion(rect, "")

		// since we index only one dimensional points, let's filter out
		// or prune our search time terms. This needs to be removed once
		// we start indexing 2 or higher dimensional shapes.
		return s2.FilterOutCoveringTerms(terms)
	}

	if bg, ok := shape.(*boundedPolygon); ok {
		coordinates := bg.coordinates
		vertices := make([]s2.Point, len(coordinates))
		for i, point := range coordinates {
			vertices[i] = s2.PointFromLatLng(s2.LatLngFromDegrees(point.Lat, point.Lon))
		}
		polygon := s2.PolygonFromLoops([]*s2.Loop{s2.LoopFromPoints(vertices)})

		// obtain the terms to be searched for the given polygon.
		terms := s.s2Indexer.GetQueryTermsForRegion(
			polygon.CapBound(), "")

		// since we index only one dimensional points, let's filter out
		// or prune our search time terms. This needs to be removed once
		// we start indexing 2 or higher dimensional shapes.
		return s2.FilterOutCoveringTerms(terms)
	}

	return nil
}

type boundedRectangle struct {
	minLat float64
	maxLat float64
	minLon float64
	maxLon float64
}

func (br *boundedRectangle) Type() string {
	return "boundedRectangle"
}

func NewBoundedRectangle(minLat, minLon, maxLat, maxLon float64) *boundedRectangle {
	return &boundedRectangle{minLat: minLat, maxLat: maxLat,
		minLon: minLon, maxLon: maxLon}
}

type boundedPolygon struct {
	coordinates []Point
}

func (bp *boundedPolygon) Type() string {
	return "boundedPolygon"
}

func NewBoundedPolygon(coordinates []Point) *boundedPolygon {
	return &boundedPolygon{coordinates: coordinates}
}

type pointDistance struct {
	dist      float64
	centerLat float64
	centerLon float64
}

func (p *pointDistance) Type() string {
	return "pointDistance"
}

func NewPointDistance(centerLat, centerLon, dist float64) *pointDistance {
	return &pointDistance{centerLat: centerLat,
		centerLon: centerLon, dist: dist}
}

func (p *Point) Type() string {
	return "point"
}
