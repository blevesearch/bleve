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
	"encoding/json"
	"fmt"
	"strings"

	index "github.com/blevesearch/bleve_index_api"

	"github.com/blevesearch/geo/s2"
)

const (
	PointType              = "point"
	MultiPointType         = "multipoint"
	LineStringType         = "linestring"
	MultiLineStringType    = "multilinestring"
	PolygonType            = "polygon"
	MultiPolygonType       = "multipolygon"
	GeometryCollectionType = "geometrycollection"
	CircleType             = "circle"
	EnvelopeType           = "envelope"
)

// compositeShape is an optional interface for the
// composite geoJSON shapes which is composed of
// multiple spatial shapes within it. Composite shapes
// like multipoint, multilinestring, multipolygon and
// geometrycollection shapes are supposed to implement
// this interface.
type compositeShape interface {
	// Members implementation returns the
	// geoJSON shapes composed within the shape.
	Members() []index.GeoJSON
}

//--------------------------------------------------------
// point represents the geoJSON point type and it
// implements the index.GeoJSON interface.
type point struct {
	Typ      string    `json:"type"`
	Vertices []float64 `json:"coordinates"`
	s2cell   *s2.Cell
}

func (p *point) Type() string {
	return strings.ToLower(p.Typ)
}

func NewGeoJsonPoint(v []float64) index.GeoJSON {
	return &point{Typ: PointType, Vertices: v}
}

func (p *point) Intersects(other index.GeoJSON) (bool, error) {
	if p.s2cell == nil {
		s2point := s2.PointFromLatLng(s2.LatLngFromDegrees(
			p.Vertices[1], p.Vertices[0]))
		cell := s2.CellFromPoint(s2point)
		p.s2cell = &cell
	}

	return checkCellIntersectsShape(p.s2cell, p, other)
}

func (p *point) Contains(other index.GeoJSON) (bool, error) {
	if p.s2cell == nil {
		s2point := s2.PointFromLatLng(s2.LatLngFromDegrees(
			p.Vertices[1], p.Vertices[0]))
		cell := s2.CellFromPoint(s2point)
		p.s2cell = &cell
	}

	return checkCellContainsShape([]*s2.Cell{p.s2cell}, other)
}

func (p *point) Coordinates() []float64 {
	return p.Vertices
}

//--------------------------------------------------------
// multipoint represents the geoJSON multipoint type and it
// implements the index.GeoJSON interface as well as the
// compositeShap interface.
type multipoint struct {
	Typ      string      `json:"type"`
	Vertices [][]float64 `json:"coordinates"`
	s2cells  []*s2.Cell
}

func NewGeoJsonMultiPoint(v [][]float64) index.GeoJSON {
	return &multipoint{Typ: MultiPointType, Vertices: v}
}

func (p *multipoint) Type() string {
	return strings.ToLower(p.Typ)
}

func (p *multipoint) Intersects(other index.GeoJSON) (bool, error) {
	if p.s2cells == nil {
		p.s2cells = make([]*s2.Cell, len(p.Vertices))

		for i, point := range p.Vertices {
			s2point := s2.PointFromLatLng(s2.LatLngFromDegrees(
				point[1], point[0]))
			cell := s2.CellFromPoint(s2point)
			p.s2cells[i] = &cell
		}
	}

	for _, cell := range p.s2cells {
		rv, err := checkCellIntersectsShape(cell, p, other)
		if rv && err == nil {
			return rv, nil
		}
	}

	return false, nil
}

func (p *multipoint) Contains(other index.GeoJSON) (bool, error) {
	if p.s2cells == nil {
		p.s2cells = make([]*s2.Cell, len(p.Vertices))

		for i, point := range p.Vertices {
			s2point := s2.PointFromLatLng(s2.LatLngFromDegrees(
				point[1], point[0]))
			cell := s2.CellFromPoint(s2point)
			p.s2cells[i] = &cell
		}
	}

	return checkCellContainsShape(p.s2cells, other)
}

func (p *multipoint) Coordinates() [][]float64 {
	return p.Vertices
}

func (p *multipoint) Members() []index.GeoJSON {
	points := make([]index.GeoJSON, len(p.Vertices))
	for pos, vertices := range p.Vertices {
		points[pos] = NewGeoJsonPoint(vertices)
	}
	return points
}

//--------------------------------------------------------
// linestring represents the geoJSON linestring type and it
// implements the index.GeoJSON interface.
type linestring struct {
	Typ      string      `json:"type"`
	Vertices [][]float64 `json:"coordinates"`
	pl       *s2.Polyline
}

func NewGeoJsonLinestring(points [][]float64) index.GeoJSON {
	return &linestring{Typ: LineStringType, Vertices: points}
}

func (p *linestring) Type() string {
	return strings.ToLower(p.Typ)
}

func (p *linestring) Intersects(other index.GeoJSON) (bool, error) {
	if p.pl == nil {
		latlngs := make([]s2.LatLng, 2)
		latlngs[0] = s2.LatLngFromDegrees(p.Vertices[0][1], p.Vertices[0][0])
		latlngs[1] = s2.LatLngFromDegrees(p.Vertices[1][1], p.Vertices[1][0])

		p.pl = s2.PolylineFromLatLngs(latlngs)
	}

	return checkLineStringsIntersectsShape([]*s2.Polyline{p.pl}, p, other)
}

func (p *linestring) Contains(other index.GeoJSON) (bool, error) {
	return checkLineStringsContainsShape([][][]float64{p.Vertices}, other)
}

func (p *linestring) Coordinates() [][]float64 {
	return p.Vertices
}

//--------------------------------------------------------
// multilinestring represents the geoJSON multilinestring type
// and it implements the index.GeoJSON interface as well as the
// compositeShap interface.
type multilinestring struct {
	Typ      string        `json:"type"`
	Vertices [][][]float64 `json:"coordinates"`
	pls      []*s2.Polyline
}

func NewGeoJsonMultilinestring(points [][][]float64) index.GeoJSON {
	return &multilinestring{Typ: MultiLineStringType, Vertices: points}
}

func (p *multilinestring) Type() string {
	return strings.ToLower(p.Typ)
}

func (p *multilinestring) Intersects(other index.GeoJSON) (bool, error) {
	if p.pls == nil {
		p.pls = s2PolylinesFromCoordinates(p.Vertices)
	}

	return checkLineStringsIntersectsShape(p.pls, p, other)
}

func (p *multilinestring) Contains(other index.GeoJSON) (bool, error) {
	return checkLineStringsContainsShape(p.Vertices, other)
}

func (p *multilinestring) Coordinates() [][][]float64 {
	return p.Vertices
}

func (p *multilinestring) Members() []index.GeoJSON {
	lines := make([]index.GeoJSON, len(p.Vertices))
	for pos, vertices := range p.Vertices {
		lines[pos] = NewGeoJsonLinestring(vertices)
	}
	return lines
}

//--------------------------------------------------------
// polygon represents the geoJSON polygon type
// and it implements the index.GeoJSON interface.
type polygon struct {
	Typ      string        `json:"type"`
	Vertices [][][]float64 `json:"coordinates"`
	s2pgn    *s2.Polygon
}

func NewGeoJsonPolygon(points [][][]float64) index.GeoJSON {
	return &polygon{Typ: PolygonType, Vertices: points}
}

func (p *polygon) Type() string {
	return strings.ToLower(p.Typ)
}

func (p *polygon) Intersects(other index.GeoJSON) (bool, error) {
	// make an s2polygon for reuse.
	if p.s2pgn == nil {
		p.s2pgn = s2PolygonFromCoordinates(p.Vertices)
	}

	return checkPolygonIntersectsShape(p.s2pgn, p.Vertices, p, other)
}

func (p *polygon) Contains(other index.GeoJSON) (bool, error) {
	// make an s2polygon for reuse.
	if p.s2pgn == nil {
		p.s2pgn = s2PolygonFromCoordinates(p.Vertices)
	}

	return checkMultiPolygonContainsShape([]*s2.Polygon{p.s2pgn}, p, other)
}

func (p *polygon) Coordinates() [][][]float64 {
	return p.Vertices
}

//--------------------------------------------------------
// multipolygon represents the geoJSON multipolygon type
// and it implements the index.GeoJSON interface as well as the
// compositeShap interface.
type multipolygon struct {
	Typ      string          `json:"type"`
	Vertices [][][][]float64 `json:"coordinates"`
	s2pgns   []*s2.Polygon
}

func NewGeoJsonMultiPolygon(points [][][][]float64) index.GeoJSON {
	return &multipolygon{Typ: MultiPolygonType, Vertices: points}
}

func (p *multipolygon) Type() string {
	return strings.ToLower(p.Typ)
}

func (p *multipolygon) Intersects(other index.GeoJSON) (bool, error) {
	if p.s2pgns == nil {
		p.s2pgns = make([]*s2.Polygon, len(p.Vertices))
		for i, vertices := range p.Vertices {
			pgn := s2PolygonFromCoordinates(vertices)
			p.s2pgns[i] = pgn
		}
	}

	for i, pgn := range p.s2pgns {
		rv, err := checkPolygonIntersectsShape(pgn, p.Vertices[i], p, other)
		if rv && err == nil {
			return true, nil
		}
	}

	return false, nil
}

func (p *multipolygon) Contains(other index.GeoJSON) (bool, error) {
	if p.s2pgns == nil {
		p.s2pgns = make([]*s2.Polygon, len(p.Vertices))
		for i, vertices := range p.Vertices {
			pgn := s2PolygonFromCoordinates(vertices)
			p.s2pgns[i] = pgn
		}
	}

	return checkMultiPolygonContainsShape(p.s2pgns, p, other)
}

func (p *multipolygon) Coordinates() [][][][]float64 {
	return p.Vertices
}

func (p *multipolygon) Members() []index.GeoJSON {
	polygons := make([]index.GeoJSON, len(p.Vertices))
	for pos, vertices := range p.Vertices {
		polygons[pos] = NewGeoJsonPolygon(vertices)
	}
	return polygons
}

//--------------------------------------------------------
// geometryCollection represents the geoJSON geometryCollection type
// and it implements the index.GeoJSON interface as well as the
// compositeShap interface.
type geometryCollection struct {
	Typ    string          `json:"type"`
	Shapes []index.GeoJSON `json:"geometries"`
}

func (gc *geometryCollection) Type() string {
	return strings.ToLower(gc.Typ)
}

func (gc *geometryCollection) Members() []index.GeoJSON {
	shapes := make([]index.GeoJSON, 0, len(gc.Shapes))
	for _, shape := range gc.Shapes {
		if cs, ok := shape.(compositeShape); ok {
			shapes = append(shapes, cs.Members()...)
		} else {
			shapes = append(shapes, shape)
		}
	}
	return shapes
}

func (gc *geometryCollection) Intersects(other index.GeoJSON) (bool, error) {
	for _, shape := range gc.Members() {

		intersects, err := shape.Intersects(other)
		if intersects && err == nil {
			return true, nil
		}
	}
	return false, nil
}

func (gc *geometryCollection) Contains(other index.GeoJSON) (bool, error) {
	// handle composite target shapes explicitly
	if cs, ok := other.(compositeShape); ok {
		otherShapes := cs.Members()
		shapesFoundWithIn := make(map[int]struct{})

	nextShape:
		for pos, shapeInDoc := range otherShapes {
			for _, shape := range gc.Members() {
				within, err := shape.Contains(shapeInDoc)
				if within && err == nil {
					shapesFoundWithIn[pos] = struct{}{}
					continue nextShape
				}
			}
		}

		return len(shapesFoundWithIn) == len(otherShapes), nil
	}

	for _, shape := range gc.Members() {
		within, err := shape.Contains(other)
		if within && err == nil {
			return true, nil
		}
	}

	return false, nil
}

func (gc *geometryCollection) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Typ    string            `json:"type"`
		Shapes []json.RawMessage `json:"geometries"`
	}{}

	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	gc.Typ = tmp.Typ

	for _, shape := range tmp.Shapes {
		var t map[string]interface{}
		err := json.Unmarshal(shape, &t)
		if err != nil {
			return err
		}

		var typ string

		if val, ok := t["type"]; ok {
			typ = strings.ToLower(val.(string))
		} else {
			continue
		}

		switch typ {
		case PointType:
			var p point
			err := json.Unmarshal(shape, &p)
			if err != nil {
				return err
			}
			gc.Shapes = append(gc.Shapes, &p)

		case MultiPointType:
			var mp multipoint
			err := json.Unmarshal(shape, &mp)
			if err != nil {
				return err
			}
			gc.Shapes = append(gc.Shapes, &mp)

		case LineStringType:
			var ls linestring
			err := json.Unmarshal(shape, &ls)
			if err != nil {
				return err
			}
			gc.Shapes = append(gc.Shapes, &ls)

		case MultiLineStringType:
			var mls multilinestring
			err := json.Unmarshal(shape, &mls)
			if err != nil {
				return err
			}
			gc.Shapes = append(gc.Shapes, &mls)

		case PolygonType:
			var pgn polygon
			err := json.Unmarshal(shape, &pgn)
			if err != nil {
				return err
			}
			gc.Shapes = append(gc.Shapes, &pgn)

		case MultiPolygonType:
			var pgn multipolygon
			err := json.Unmarshal(shape, &pgn)
			if err != nil {
				return err
			}
			gc.Shapes = append(gc.Shapes, &pgn)
		}
	}

	return nil
}

//--------------------------------------------------------
// circle represents a custom circle type and it
// implements the index.GeoJSON interface.
type circle struct {
	Typ            string    `json:"type"`
	Vertices       []float64 `json:"coordinates"`
	RadiusInMeters float64   `json:"radiusInMeters"`
	s2cap          *s2.Cap
}

func NewGeoCircle(points []float64,
	radiusInMeter float64) index.GeoJSON {
	return &circle{Typ: CircleType,
		Vertices:       points,
		RadiusInMeters: radiusInMeter}
}

func (c *circle) Type() string {
	return strings.ToLower(c.Typ)
}

func (c *circle) Intersects(other index.GeoJSON) (bool, error) {
	if c.s2cap == nil {
		c.s2cap = s2Cap(c.Vertices, c.RadiusInMeters)
	}

	return checkCircleIntersectsShape(c.s2cap, c, other)
}

func (c *circle) Contains(other index.GeoJSON) (bool, error) {
	if c.s2cap == nil {
		c.s2cap = s2Cap(c.Vertices, c.RadiusInMeters)
	}

	return checkCircleContainsShape(c.s2cap, c, other)
}

func (c *circle) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Typ            string    `json:"type"`
		Vertices       []float64 `json:"coordinates"`
		Radius         string    `json:"radius"`
		RadiusInMeters float64   `json:"radiusInMeters"`
	}{}

	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	c.Typ = tmp.Typ
	c.Vertices = tmp.Vertices
	c.RadiusInMeters = tmp.RadiusInMeters
	if tmp.Radius != "" {
		c.RadiusInMeters, err = ParseDistance(tmp.Radius)
	}

	return err
}

//--------------------------------------------------------
// envelope represents the  envelope/bounding box type and it
// implements the index.GeoJSON interface.
type envelope struct {
	Typ      string      `json:"type"`
	Vertices [][]float64 `json:"coordinates"`
	r        *s2.Rect
}

func NewGeoEnvelope(points [][]float64) index.GeoJSON {
	return &envelope{Vertices: points, Typ: EnvelopeType}
}

func (e *envelope) Type() string {
	return strings.ToLower(e.Typ)
}

func (e *envelope) Intersects(other index.GeoJSON) (bool, error) {
	if e.r == nil {
		e.r = s2RectFromBounds(e.Vertices[0], e.Vertices[1])
	}

	return checkEnvelopeIntersectsShape(e.r, e, other)
}

func (e *envelope) Contains(other index.GeoJSON) (bool, error) {
	if e.r == nil {
		e.r = s2RectFromBounds(e.Vertices[0], e.Vertices[1])
	}

	return checkEnvelopeContainsShape(e.r, e, other)
}

//--------------------------------------------------------

type boundedRectangle struct {
	minLat float64
	maxLat float64
	minLon float64
	maxLon float64
}

func NewBoundedRectangle(minLat, minLon, maxLat,
	maxLon float64) *boundedRectangle {
	return &boundedRectangle{minLat: minLat,
		maxLat: maxLat, minLon: minLon, maxLon: maxLon}
}

func (br *boundedRectangle) Type() string {
	// placeholder implementation
	return "boundedRectangle"
}

func (p *boundedRectangle) Intersects(s index.GeoJSON) (bool, error) {
	// placeholder implementation
	return false, nil
}

func (p *boundedRectangle) Contains(s index.GeoJSON) (bool, error) {
	// placeholder implementation
	return false, nil
}

//--------------------------------------------------------

type boundedPolygon struct {
	coordinates []Point
}

func NewBoundedPolygon(coordinates []Point) *boundedPolygon {
	return &boundedPolygon{coordinates: coordinates}
}

func (bp *boundedPolygon) Type() string {
	// placeholder implementation
	return "boundedPolygon"
}

func (p *boundedPolygon) Intersects(s index.GeoJSON) (bool, error) {
	// placeholder implementation
	return false, nil
}

func (p *boundedPolygon) Contains(s index.GeoJSON) (bool, error) {
	// placeholder implementation
	return false, nil
}

//--------------------------------------------------------

type pointDistance struct {
	dist      float64
	centerLat float64
	centerLon float64
}

func (p *pointDistance) Type() string {
	// placeholder implementation
	return "pointDistance"
}

func NewPointDistance(centerLat, centerLon,
	dist float64) *pointDistance {
	return &pointDistance{centerLat: centerLat,
		centerLon: centerLon, dist: dist}
}

func (p *pointDistance) Intersects(s index.GeoJSON) (bool, error) {
	// placeholder implementation
	return false, nil
}

func (p *pointDistance) Contains(s index.GeoJSON) (bool, error) {
	// placeholder implementation
	return false, nil
}

//--------------------------------------------------------

func (p *Point) Type() string {
	return PointType
}

func (p *Point) Intersects(s index.GeoJSON) (bool, error) {
	// placeholder implementation
	return false, nil
}

func (p *Point) Contains(s index.GeoJSON) (bool, error) {
	// placeholder implementation
	return false, nil
}

//--------------------------------------------------------

// checkCellIntersectsShape checks for intersection between
// the s2cell and the shape in the document.
func checkCellIntersectsShape(cell *s2.Cell, shapeIn,
	other index.GeoJSON) (bool, error) {
	// check if the other shape is a point.
	if p2, ok := other.(*point); ok {
		s2cell := s2.CellFromLatLng(s2.LatLngFromDegrees(
			p2.Vertices[1], p2.Vertices[0]))

		if cell.IntersectsCell(s2cell) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multipoint.
	if p2, ok := other.(*multipoint); ok {
		// check the intersection for any point in the array.
		for _, point := range p2.Vertices {
			s2cell := s2.CellFromLatLng(s2.LatLngFromDegrees(
				point[1], point[0]))

			if cell.IntersectsCell(s2cell) {
				return true, nil
			}
		}

		return false, nil
	}

	// check if the other shape is a polygon.
	if p2, ok := other.(*polygon); ok {
		s2pgn2 := s2PolygonFromCoordinates(p2.Coordinates())

		if s2pgn2.IntersectsCell(*cell) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multipolygon.
	if p2, ok := other.(*multipolygon); ok {
		// check the intersection for any polygon in the collection.
		for _, coordinates := range p2.Vertices {
			s2pgn2 := s2PolygonFromCoordinates(coordinates)

			if s2pgn2.IntersectsCell(*cell) {
				return true, nil
			}
		}

		return false, nil
	}

	// check if the other shape is a linestring.
	if p2, ok := other.(*linestring); ok {
		for _, point := range p2.Vertices {
			s2cell := s2.CellFromLatLng(s2.LatLngFromDegrees(
				point[1], point[0]))
			if cell.IntersectsCell(s2cell) {
				return true, nil
			}
		}

		return false, nil
	}

	// check if the other shape is a multilinestring.
	if p2, ok := other.(*multilinestring); ok {
		// check the intersection for any linestring in the array.
		for _, linestrings := range p2.Vertices {
			for _, point := range linestrings {
				s2cell := s2.CellFromLatLng(s2.LatLngFromDegrees(
					point[1], point[0]))
				if cell.IntersectsCell(s2cell) {
					return true, nil
				}
			}
		}

		return false, nil
	}

	// check if the other shape is a geometrycollection.
	if gc, ok := other.(*geometryCollection); ok {
		// check for intersection across every member shape.
		if geometryCollectionIntersectsShape(gc, shapeIn) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a circle.
	if c, ok := other.(*circle); ok {
		s2cap := s2Cap(c.Vertices, c.RadiusInMeters)
		if s2cap.IntersectsCell(*cell) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is an envelope.
	if e, ok := other.(*envelope); ok {
		s2rectInDoc := s2RectFromBounds(e.Vertices[0],
			e.Vertices[1])

		if s2rectInDoc.IntersectsCell(*cell) {
			return true, nil
		}

		return false, nil
	}

	return false, fmt.Errorf("unknown geojson type: %s "+
		" found in document", other.Type())
}

// checkCellContainsShape checks whether the given shape in
// in the document is contained with the s2cell.
func checkCellContainsShape(cells []*s2.Cell,
	other index.GeoJSON) (bool, error) {
	// check if the other shape is a point.
	if p2, ok := other.(*point); ok {
		for _, cell := range cells {
			s2point := s2.PointFromLatLng(s2.LatLngFromDegrees(
				p2.Vertices[1], p2.Vertices[0]))

			if cell.ContainsPoint(s2point) {
				return true, nil
			}
		}

		return false, nil
	}

	// check if the other shape is a multipoint, if so containment is
	// checked for every point in the multipoint with every given cells.
	if p2, ok := other.(*multipoint); ok {
		// check the containment for every point in the collection.
		lookup := make(map[int]struct{})
		for _, cell := range cells {
			for pos, point := range p2.Vertices {
				if _, done := lookup[pos]; done {
					continue
				}
				// already processed all the points in the multipoint.
				if len(lookup) == len(p2.Vertices) {
					return true, nil
				}

				s2point := s2.PointFromLatLng(s2.LatLngFromDegrees(point[1],
					point[0]))

				if cell.ContainsPoint(s2point) {
					lookup[pos] = struct{}{}
				}
			}
		}

		return len(lookup) == len(p2.Vertices), nil
	}

	// as point is a non closed shape, containment isn't feasible
	// for other higher dimensions.
	return false, nil
}

// ------------------------------------------------------------------------

// checkLineStringsIntersectsShape checks whether the given linestrings
// intersects with the shape in the document.
func checkLineStringsIntersectsShape(pls []*s2.Polyline, shapeIn,
	other index.GeoJSON) (bool, error) {
	// check if the other shape is a point.
	if p2, ok := other.(*point); ok {

		if polylineIntersectsPoint(pls, p2.Vertices) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multipoint.
	if p2, ok := other.(*multipoint); ok {
		// check the intersection for any point in the collection.
		for _, point := range p2.Vertices {

			if polylineIntersectsPoint(pls, point) {
				return true, nil
			}
		}

		return false, nil
	}

	// check if the other shape is a polygon.
	if p2, ok := other.(*polygon); ok {

		if polylineIntersectsPolygons(pls,
			[][][][]float64{p2.Coordinates()}) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multipolygon.
	if p2, ok := other.(*multipolygon); ok {
		// check the intersection for any polygon in the collection.
		if polylineIntersectsPolygons(pls, p2.Coordinates()) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a linestring.
	if ls, ok := other.(*linestring); ok {

		if polylineIntersectsPolylines(pls, [][][]float64{ls.Vertices}) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multilinestring.
	if mls, ok := other.(*multilinestring); ok {

		if polylineIntersectsPolylines(pls, mls.Vertices) {
			return true, nil
		}

		return false, nil
	}

	if gc, ok := other.(*geometryCollection); ok {
		// check whether the linestring intersects with any of the
		// shapes Contains a geometrycollection.
		if geometryCollectionIntersectsShape(gc, shapeIn) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a circle.
	if c, ok := other.(*circle); ok {
		centre := s2.PointFromLatLng(
			s2.LatLngFromDegrees(c.Vertices[1], c.Vertices[0]))

		for _, pl := range pls {
			edge := pl.Edge(0)
			distance := s2.DistanceFromSegment(centre, edge.V0, edge.V1)
			r := radiusInMetersToS1Angle(float64(c.RadiusInMeters))
			return distance <= r, nil
		}

		return false, nil
	}

	// check if the other shape is a envelope.
	if e, ok := other.(*envelope); ok {
		s2rectInDoc := s2RectFromBounds(e.Vertices[0], e.Vertices[1])

		for _, pl := range pls {
			edge := pl.Edge(0)
			latlng1 := s2.LatLngFromPoint(edge.V0)
			latlng2 := s2.LatLngFromPoint(edge.V1)
			a := []float64{latlng1.Lng.Degrees(), latlng1.Lat.Degrees()}
			b := []float64{latlng2.Lng.Degrees(), latlng2.Lat.Degrees()}
			for j := 0; j < 4; j++ {
				v1 := s2rectInDoc.Vertex(j)
				v2 := s2rectInDoc.Vertex((j + 1) % 4)
				c := []float64{v1.Lng.Degrees(), v1.Lat.Degrees()}
				d := []float64{v2.Lng.Degrees(), v2.Lat.Degrees()}
				if doIntersect(a, b, c, d) {
					return true, nil
				}
			}
		}

		return false, nil
	}

	return false, fmt.Errorf("unknown geojson type: %s "+
		"found in document", other.Type())
}

// checkLineStringsContainsShape checks the containment for
// points and multipoints for the linestring vertices.
func checkLineStringsContainsShape(vertices [][][]float64,
	other index.GeoJSON) (bool, error) {
	// check if the other shape is a point.
	if p, ok := other.(*point); ok {

		if polylinesContainPoints(vertices, [][]float64{p.Vertices}) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multipoint.
	if mp, ok := other.(*multipoint); ok {

		if polylinesContainPoints(vertices, mp.Vertices) {
			return true, nil
		}
	}

	return false, nil
}

// ------------------------------------------------------------------------

// checkPolygonIntersectsShape checks the intersection between the
// s2 polygon and the other shapes in the documents.
func checkPolygonIntersectsShape(s2pgn *s2.Polygon, vertices [][][]float64,
	shapeIn, other index.GeoJSON) (bool, error) {
	// check if the other shape is a point.
	if p2, ok := other.(*point); ok {
		s2cell := s2.CellFromLatLng(s2.LatLngFromDegrees(p2.Vertices[1],
			p2.Vertices[0]))

		if s2pgn.IntersectsCell(s2cell) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multipoint.
	if p2, ok := other.(*multipoint); ok {
		// check the intersection for any point in the collection.
		for _, point := range p2.Vertices {
			s2cell := s2.CellFromLatLng(s2.LatLngFromDegrees(point[1],
				point[0]))

			if s2pgn.IntersectsCell(s2cell) {
				return true, nil
			}
		}

		return false, nil
	}

	// check if the other shape is a polygon.
	if p2, ok := other.(*polygon); ok {
		s2p2 := s2PolygonFromCoordinates(p2.Coordinates())

		if s2pgn.Intersects(s2p2) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multipolygon.
	if p2, ok := other.(*multipolygon); ok {
		// check the intersection for any polygon in the collection.
		for _, coordinates := range p2.Vertices {
			s2p2 := s2PolygonFromCoordinates(coordinates)

			if s2pgn.Intersects(s2p2) {
				return true, nil
			}
		}

		return false, nil
	}

	// check if the other shape is a linestring.
	if ls, ok := other.(*linestring); ok {

		if polygonsIntersectsLinestrings(s2pgn,
			[][][]float64{ls.Vertices}) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multilinestring.
	if mls, ok := other.(*multilinestring); ok {

		if polygonsIntersectsLinestrings(s2pgn, mls.Vertices) {
			return true, nil
		}

		return false, nil
	}

	if gc, ok := other.(*geometryCollection); ok {
		// check whether the polygon intersects with any of the
		// member shapes of the geometry collection.
		if geometryCollectionIntersectsShape(gc, shapeIn) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a circle.
	if c, ok := other.(*circle); ok {

		cp := s2.PointFromLatLng(s2.LatLngFromDegrees(
			c.Vertices[1], c.Vertices[0]))
		radius := radiusInMetersToS1Angle(float64(c.RadiusInMeters))
		projected := s2pgn.Project(&cp)
		distance := projected.Distance(cp)

		return distance <= radius, nil
	}

	// check if the other shape is a envelope.
	if e, ok := other.(*envelope); ok {

		s2rectInDoc := s2RectFromBounds(e.Vertices[0], e.Vertices[1])
		s2pgnInDoc := s2PolygonFromS2Rectangle(s2rectInDoc)
		if s2pgn.Intersects(s2pgnInDoc) {
			return true, nil
		}
		return false, nil
	}

	return false, fmt.Errorf("unknown geojson type: %s "+
		" found in document", other.Type())
}

// checkMultiPolygonContainsShape checks whether the given polygons
// collectively contains the shape in the document.
func checkMultiPolygonContainsShape(s2pgns []*s2.Polygon,
	shapeIn, other index.GeoJSON) (bool, error) {
	// check if the other shape is a point.
	if p2, ok := other.(*point); ok {
		s2point := s2.PointFromLatLng(
			s2.LatLngFromDegrees(p2.Vertices[1], p2.Vertices[0]))

		for _, s2pgn := range s2pgns {
			if s2pgn.ContainsPoint(s2point) {
				return true, nil
			}
		}

		return false, nil
	}

	// check if the other shape is a multipoint.
	if p2, ok := other.(*multipoint); ok {
		// check the containment for every point in the collection.
		pointsWithIn := make(map[int]struct{})
	nextPoint:
		for pointIndex, point := range p2.Vertices {
			s2point := s2.PointFromLatLng(s2.LatLngFromDegrees(point[1],
				point[0]))

			for _, s2pgn := range s2pgns {
				if s2pgn.ContainsPoint(s2point) {
					pointsWithIn[pointIndex] = struct{}{}
					continue nextPoint
				}
			}
		}

		return len(p2.Vertices) == len(pointsWithIn), nil
	}

	// check if the other shape is a polygon.
	if p2, ok := other.(*polygon); ok {
		s2p2 := s2PolygonFromCoordinates(p2.Coordinates())

		for _, s2pgn := range s2pgns {
			if s2pgn.Contains(s2p2) {
				return true, nil
			}
		}

		return false, nil
	}

	// check if the other shape is a multipolygon.
	if p2, ok := other.(*multipolygon); ok {
		// check the intersection for every polygon in the collection.
		polygonsWithIn := make(map[int]struct{})
	nextPolygon:
		for pgnIndex, coordinates := range p2.Vertices {
			s2p2 := s2PolygonFromCoordinates(coordinates)

			for _, s2pgn := range s2pgns {
				if s2pgn.Contains(s2p2) {
					polygonsWithIn[pgnIndex] = struct{}{}
					continue nextPolygon
				}
			}
		}

		return len(p2.Vertices) == len(polygonsWithIn), nil
	}

	// check if the other shape is a linestring.
	if ls, ok := other.(*linestring); ok {

		if polygonsContainsLineStrings(s2pgns,
			[][][]float64{ls.Vertices}) {
			return true, nil
		}
		return false, nil
	}

	// check if the other shape is a multilinestring.
	if mls, ok := other.(*multilinestring); ok {
		// check whether any of the linestring is inside the polygon.
		if polygonsContainsLineStrings(s2pgns, mls.Vertices) {
			return true, nil
		}

		return false, nil
	}

	if gc, ok := other.(*geometryCollection); ok {
		shapesWithIn := make(map[int]struct{})
	nextShape:
		for pos, shape := range gc.Members() {
			for _, s2pgn := range s2pgns {
				contains, err := checkMultiPolygonContainsShape(
					[]*s2.Polygon{s2pgn}, shapeIn, shape)
				if err == nil && contains {
					shapesWithIn[pos] = struct{}{}
					continue nextShape
				}
			}
		}
		return len(shapesWithIn) == len(gc.Members()), nil
	}

	// check if the other shape is a circle.
	if c, ok := other.(*circle); ok {
		cp := s2.PointFromLatLng(
			s2.LatLngFromDegrees(c.Vertices[1], c.Vertices[0]))
		radius := radiusInMetersToS1Angle(float64(c.RadiusInMeters))

		for _, s2pgn := range s2pgns {

			if s2pgn.ContainsPoint(cp) {
				projected := s2pgn.ProjectToBoundary(&cp)
				distance := projected.Distance(cp)
				if distance >= radius {
					return true, nil
				}
			}
		}

		return false, nil
	}

	// check if the other shape is a envelope.
	if e, ok := other.(*envelope); ok {
		s2rectInDoc := s2RectFromBounds(e.Vertices[0], e.Vertices[1])
		// create a polygon from the rectangle and checks the containment.
		s2pgnInDoc := s2PolygonFromS2Rectangle(s2rectInDoc)
		for _, s2pgn := range s2pgns {
			if s2pgn.Contains(s2pgnInDoc) {
				return true, nil
			}
		}

		return false, nil
	}

	return false, fmt.Errorf("unknown geojson type: %s"+
		" found in document", other.Type())
}

// ------------------------------------------------------------------------

// checkCircleIntersectsShape checks for intersection of the
// shape in the document with the circle.
func checkCircleIntersectsShape(s2cap *s2.Cap, shapeIn,
	other index.GeoJSON) (bool, error) {
	// check if the other shape is a point.
	if p2, ok := other.(*point); ok {
		s2cell := s2.CellFromLatLng(s2.LatLngFromDegrees(p2.Vertices[1],
			p2.Vertices[0]))

		if s2cap.IntersectsCell(s2cell) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multipoint.
	if p2, ok := other.(*multipoint); ok {
		// check the intersection for any point in the collection.
		for _, point := range p2.Vertices {
			s2cell := s2.CellFromLatLng(s2.LatLngFromDegrees(point[1],
				point[0]))

			if s2cap.IntersectsCell(s2cell) {
				return true, nil
			}
		}

		return false, nil
	}

	// check if the other shape is a polygon.
	if p2, ok := other.(*polygon); ok {
		s2pgn := s2PolygonFromCoordinates(p2.Coordinates())
		centerPoint := s2cap.Center()
		projected := s2pgn.Project(&centerPoint)
		distance := projected.Distance(centerPoint)
		return distance <= s2cap.Radius(), nil
	}

	// check if the other shape is a multipolygon.
	if p2, ok := other.(*multipolygon); ok {
		// check the intersection for any polygon in the collection.
		for _, coordinates := range p2.Vertices {
			s2pgn := s2PolygonFromCoordinates(coordinates)
			centerPoint := s2cap.Center()
			projected := s2pgn.Project(&centerPoint)
			distance := projected.Distance(centerPoint)
			return distance <= s2cap.Radius(), nil
		}

		return false, nil
	}

	// check if the other shape is a linestring.
	if p2, ok := other.(*linestring); ok {
		start := s2.LatLngFromDegrees(p2.Vertices[0][1], p2.Vertices[0][0])
		end := s2.LatLngFromDegrees(p2.Vertices[1][1], p2.Vertices[1][0])
		pl := s2.PolylineFromLatLngs([]s2.LatLng{start, end})
		projected, _ := pl.Project(s2cap.Center())
		distance := projected.Distance(s2cap.Center())
		return distance <= s2cap.Radius(), nil
	}

	// check if the other shape is a multilinestring.
	if p2, ok := other.(*multilinestring); ok {
		polylines := s2PolylinesFromCoordinates(p2.Vertices)
		for _, pl := range polylines {
			projected, _ := pl.Project(s2cap.Center())
			distance := projected.Distance(s2cap.Center())
			if distance <= s2cap.Radius() {
				return true, nil
			}
		}

		return false, nil
	}

	if gc, ok := other.(*geometryCollection); ok {
		// check whether the circle intersects with any of the
		// member shapes Contains the geometrycollection.
		if geometryCollectionIntersectsShape(gc, shapeIn) {
			return true, nil
		}
		return false, nil
	}

	// check if the other shape is a circle.
	if c, ok := other.(*circle); ok {

		s2capInDoc := s2Cap(c.Vertices, c.RadiusInMeters)

		if s2cap.Intersects(*s2capInDoc) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a envelope.
	if e, ok := other.(*envelope); ok {
		s2rectInDoc := s2RectFromBounds(e.Vertices[0], e.Vertices[1])

		if s2rectInDoc.ContainsPoint(s2cap.Center()) {
			return true, nil
		}

		latlngs := []s2.LatLng{s2rectInDoc.Vertex(0), s2rectInDoc.Vertex(1),
			s2rectInDoc.Vertex(2), s2rectInDoc.Vertex(3), s2rectInDoc.Vertex(0)}
		pl := s2.PolylineFromLatLngs(latlngs)
		projected, _ := pl.Project(s2cap.Center())
		distance := projected.Distance(s2cap.Center())
		if distance <= s2cap.Radius() {
			return true, nil
		}

		return false, nil
	}

	return false, fmt.Errorf("unknown geojson type: %s"+
		" found in document", other.Type())
}

// checkCircleContainsShape checks for containment of the
// shape in the document with the circle.
func checkCircleContainsShape(s2cap *s2.Cap,
	shapeIn, other index.GeoJSON) (bool, error) {
	// check if the other shape is a point.
	if p2, ok := other.(*point); ok {
		s2point := s2.PointFromLatLng(s2.LatLngFromDegrees(
			p2.Vertices[1], p2.Vertices[0]))

		if s2cap.ContainsPoint(s2point) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multipoint.
	if p2, ok := other.(*multipoint); ok {
		// check the intersection for every point in the collection.
		for _, point := range p2.Vertices {
			s2point := s2.PointFromLatLng(s2.LatLngFromDegrees(
				point[1], point[0]))

			if !s2cap.ContainsPoint(s2point) {
				return false, nil
			}
		}

		return true, nil
	}

	// check if the other shape is a polygon.
	if p2, ok := other.(*polygon); ok {
		for _, vertex := range p2.Vertices {
			for _, v := range vertex {
				if !s2cap.ContainsPoint(s2.PointFromLatLng(
					s2.LatLngFromDegrees(v[1], v[0]))) {
					return false, nil
				}
			}
		}

		return true, nil
	}

	// check if the other shape is a multipolygon.
	if p2, ok := other.(*multipolygon); ok {
		// check the containment for every polygon in the collection.
		for _, coordinates := range p2.Vertices {
			for _, vertex := range coordinates {
				for _, v := range vertex {
					if !s2cap.ContainsPoint(s2.PointFromLatLng(
						s2.LatLngFromDegrees(v[1], v[0]))) {
						return false, nil
					}
				}
			}
		}

		return true, nil
	}

	// check if the other shape is a linestring.
	if p2, ok := other.(*linestring); ok {
		start := s2.PointFromLatLng(s2.LatLngFromDegrees(
			p2.Vertices[0][1], p2.Vertices[0][0]))
		end := s2.PointFromLatLng(s2.LatLngFromDegrees(
			p2.Vertices[1][1], p2.Vertices[1][0]))
		// check whether both the end vertices are inside the circle.
		if s2cap.ContainsPoint(start) && s2cap.ContainsPoint(end) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multilinestring.
	if p2, ok := other.(*multilinestring); ok {
		for _, lines := range p2.Vertices {
			start := s2.PointFromLatLng(s2.LatLngFromDegrees(
				lines[0][1], lines[0][0]))
			end := s2.PointFromLatLng(s2.LatLngFromDegrees(
				lines[1][1], lines[1][0]))
			// check whether both the end vertices are inside the circle.
			if !(s2cap.ContainsPoint(start) && s2cap.ContainsPoint(end)) {
				return false, nil
			}
		}

		return true, nil
	}

	if gc, ok := other.(*geometryCollection); ok {
		for _, shape := range gc.Members() {
			contains, err := shapeIn.Contains(shape)
			if err == nil && !contains {
				return false, nil
			}
		}
		return true, nil
	}

	// check if the other shape is a circle.
	if c, ok := other.(*circle); ok {
		s2capInDoc := s2Cap(c.Vertices, c.RadiusInMeters)

		if s2cap.Contains(*s2capInDoc) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a envelope.
	if e, ok := other.(*envelope); ok {
		s2rectInDoc := s2RectFromBounds(e.Vertices[0], e.Vertices[1])

		for i := 0; i < 4; i++ {
			if !s2cap.ContainsPoint(
				s2.PointFromLatLng(s2rectInDoc.Vertex(i))) {
				return false, nil
			}
		}

		return true, nil
	}

	return false, fmt.Errorf("unknown geojson type: %s"+
		" found in document", other.Type())
}

// ------------------------------------------------------------------------

// checkEnvelopeIntersectsShape checks whether the given shape in
// the document is intersecting Contains the envelope/rectangle.
func checkEnvelopeIntersectsShape(s2rect *s2.Rect, shapeIn,
	other index.GeoJSON) (bool, error) {
	// check if the other shape is a point.
	if p2, ok := other.(*point); ok {
		s2cell := s2.CellFromLatLng(s2.LatLngFromDegrees(p2.Vertices[1],
			p2.Vertices[0]))

		if s2rect.IntersectsCell(s2cell) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multipoint.
	if p2, ok := other.(*multipoint); ok {
		// check the intersection for any point in the collection.
		for _, point := range p2.Vertices {
			s2cell := s2.CellFromLatLng(s2.LatLngFromDegrees(point[1],
				point[0]))

			if s2rect.IntersectsCell(s2cell) {
				return true, nil
			}
		}

		return false, nil
	}

	// check if the other shape is a polygon.
	if pgn, ok := other.(*polygon); ok {

		if rectangleIntersectsWithPolygons(s2rect,
			[][][][]float64{pgn.Vertices}) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multipolygon.
	if mpgn, ok := other.(*multipolygon); ok {
		// check the intersection for any polygon in the collection.
		if rectangleIntersectsWithPolygons(s2rect, mpgn.Vertices) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a linestring.
	if ls, ok := other.(*linestring); ok {

		if rectangleIntersectsWithLineStrings(s2rect,
			[][][]float64{ls.Vertices}) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multilinestring.
	if mls, ok := other.(*multilinestring); ok {

		if rectangleIntersectsWithLineStrings(s2rect, mls.Vertices) {
			return true, nil
		}

		return false, nil
	}

	if gc, ok := other.(*geometryCollection); ok {
		// check for the intersection of every member shape
		// within the geometrycollection.
		if geometryCollectionIntersectsShape(gc, shapeIn) {
			return true, nil
		}
		return false, nil
	}

	// check if the other shape is a circle.
	if c, ok := other.(*circle); ok {
		s2capInDoc := s2Cap(c.Vertices, c.RadiusInMeters)
		s2pgn := s2PolygonFromS2Rectangle(s2rect)
		cp := s2capInDoc.Center()
		projected := s2pgn.Project(&cp)
		distance := projected.Distance(s2capInDoc.Center())
		return distance <= s2capInDoc.Radius(), nil
	}

	// check if the other shape is a envelope.
	if e, ok := other.(*envelope); ok {
		s2rectInDoc := s2RectFromBounds(e.Vertices[0], e.Vertices[1])

		if s2rect.Intersects(*s2rectInDoc) {
			return true, nil
		}

		return false, nil
	}

	return false, fmt.Errorf("unknown geojson type: %s"+
		" found in document", other.Type())
}

// checkEnvelopeContainsShape checks whether the given shape in
// the document is contained Contains the envelope/rectangle.
func checkEnvelopeContainsShape(s2rect *s2.Rect, shapeIn,
	other index.GeoJSON) (bool, error) {
	// check if the other shape is a point.
	if p2, ok := other.(*point); ok {
		s2LatLng := s2.LatLngFromDegrees(p2.Vertices[1], p2.Vertices[0])

		if s2rect.ContainsLatLng(s2LatLng) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a multipoint.
	if p2, ok := other.(*multipoint); ok {
		// check the intersection for any point in the collection.
		for _, point := range p2.Vertices {
			s2LatLng := s2.LatLngFromDegrees(point[1], point[0])

			if !s2rect.ContainsLatLng(s2LatLng) {
				return false, nil
			}
		}

		return true, nil
	}

	// check if the other shape is a polygon.
	if p2, ok := other.(*polygon); ok {
		for _, points := range p2.Vertices {
			for _, point := range points {
				if !s2rect.ContainsLatLng(s2.LatLngFromDegrees(point[1],
					point[0])) {
					return false, nil
				}
			}
		}

		return true, nil
	}

	// check if the other shape is a multipolygon.
	if p2, ok := other.(*multipolygon); ok {
		// check the intersection for any polygon in the collection.
		for _, coordinates := range p2.Vertices {
			for _, points := range coordinates {
				for _, point := range points {
					if !s2rect.ContainsLatLng(s2.LatLngFromDegrees(point[1],
						point[0])) {
						return false, nil
					}
				}
			}
		}

		return true, nil
	}

	// check if the other shape is a linestring.
	if p2, ok := other.(*linestring); ok {
		for _, point := range p2.Vertices {
			s2LatLng := s2.LatLngFromDegrees(point[1], point[0])
			if !s2rect.ContainsLatLng(s2LatLng) {
				return false, nil
			}
		}
		return true, nil
	}

	// check if the other shape is a multilinestring.
	if p2, ok := other.(*multilinestring); ok {
		for _, points := range p2.Vertices {
			for _, point := range points {
				s2LatLng := s2.LatLngFromDegrees(point[1], point[0])
				if !s2rect.ContainsLatLng(s2LatLng) {
					return false, nil
				}
			}
		}
		return true, nil
	}

	if gc, ok := other.(*geometryCollection); ok {
		for _, shape := range gc.Members() {
			contains, err := shapeIn.Contains(shape)
			if err == nil && !contains {
				return false, nil
			}
		}
		return true, nil
	}

	// check if the other shape is a circle.
	if c, ok := other.(*circle); ok {
		cp := s2.PointFromLatLng(s2.LatLngFromDegrees(c.Vertices[1],
			c.Vertices[0]))
		angle := radiusInMetersToS1Angle(float64(c.RadiusInMeters))
		s2capInDoc := s2.CapFromCenterAngle(cp, angle)

		if s2rect.Contains(s2capInDoc.RectBound()) {
			return true, nil
		}

		return false, nil
	}

	// check if the other shape is a envelope.
	if e, ok := other.(*envelope); ok {
		s2rectInDoc := s2RectFromBounds(e.Vertices[0], e.Vertices[1])

		if s2rect.Contains(*s2rectInDoc) {
			return true, nil
		}

		return false, nil
	}

	return false, fmt.Errorf("unknown geojson type: %s"+
		" found in document", other.Type())
}
