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
	"strings"

	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/geo/s2"
	"github.com/golang/geo/s1"
)

// ------------------------------------------------------------------------

func polylineIntersectsPoint(pls []*s2.Polyline,
	point []float64) bool {
	s2cell := s2.CellFromLatLng(s2.LatLngFromDegrees(
		point[1], point[0]))

	for _, pl := range pls {
		if pl.IntersectsCell(s2cell) {
			return true
		}
	}

	return false
}

func polylineIntersectsPolygons(pls []*s2.Polyline,
	coordinates [][][][]float64) bool {
	for _, pl := range pls {
		for _, vertices := range coordinates {
			s2pgn := s2PolygonFromCoordinates(vertices)
			for i := 0; i < pl.NumEdges(); i++ {
				edge := pl.Edge(i)
				a := []float64{edge.V0.X, edge.V0.Y}
				b := []float64{edge.V1.X, edge.V1.Y}

				for i := 0; i < s2pgn.NumEdges(); i++ {
					edgeB := s2pgn.Edge(i)

					c := []float64{edgeB.V0.X, edgeB.V0.Y}
					d := []float64{edgeB.V1.X, edgeB.V1.Y}

					if doIntersect(a, b, c, d) {
						return true
					}
				}
			}
		}
	}

	return false
}

func polylineIntersectsPolylines(pls []*s2.Polyline,
	coordinates [][][]float64) bool {
	var plsInDoc []*s2.Polyline
	for _, lines := range coordinates {
		latlngs := make([]s2.LatLng, 0, len(lines))
		for _, line := range lines {
			latlngs = append(latlngs, s2.LatLngFromDegrees(line[1], line[0]))
		}
		plsInDoc = append(plsInDoc, s2.PolylineFromLatLngs(latlngs))
	}

	for _, pl := range pls {
		for _, pl2 := range plsInDoc {
			if pl.Intersects(pl2) {
				return true
			}
		}
	}

	return false
}

func geometryCollectionIntersectsShape(gc *geometryCollection,
	shapeIn index.GeoJSON) bool {
	for _, shape := range gc.Members() {
		intersects, err := shapeIn.Intersects(shape)
		if err == nil && intersects {
			return true
		}
	}
	return false
}

func polylinesContainPoints(lineVertices [][][]float64,
	coordinates [][]float64) bool {
	// check the intersection for every point in the array.
	lookup := make(map[int]struct{})
NextPoint:
	for pos, points := range coordinates {
		s2point := s2.PointFromLatLng(
			s2.LatLngFromDegrees(points[1], points[0]))

		for _, lines := range lineVertices {
			for _, point := range lines {

				linePoint := s2.PointFromLatLng(
					s2.LatLngFromDegrees(point[1], point[0]))

				if linePoint.ContainsPoint(s2point) {
					lookup[pos] = struct{}{}
					continue NextPoint
				}
			}
		}
	}

	return len(coordinates) == len(lookup)
}

func polygonsIntersectsLinestrings(s2pgn *s2.Polygon,
	lineVertices [][][]float64) bool {
	for _, vertices := range lineVertices {
		t1 := s2.PointFromLatLng(s2.LatLngFromDegrees(
			vertices[0][1], vertices[0][0]))
		t2 := s2.PointFromLatLng(s2.LatLngFromDegrees(
			vertices[1][1], vertices[1][0]))
		a := []float64{t1.X, t1.Y}
		b := []float64{t2.X, t2.Y}

		for i := 0; i < s2pgn.NumEdges(); i++ {
			edgeB := s2pgn.Edge(i)

			c := []float64{edgeB.V0.X, edgeB.V0.Y}
			d := []float64{edgeB.V1.X, edgeB.V1.Y}

			if doIntersect(a, b, c, d) {
				return true
			}
		}
	}
	return false
}

func polygonsContainsLineStrings(s2pgns []*s2.Polygon,
	lineVertices [][][]float64) bool {
	linesWithIn := make(map[int]struct{})
nextLine:
	for lineIndex, points := range lineVertices {
		start := s2.PointFromLatLng(s2.LatLngFromDegrees(
			points[0][1], points[0][0]))
		end := s2.PointFromLatLng(s2.LatLngFromDegrees(
			points[1][1], points[1][0]))

		// check whether both the end vertices are inside the polygon.
		for _, s2pgn := range s2pgns {
			if s2pgn.ContainsPoint(start) && s2pgn.ContainsPoint(end) {
				// if both endpoints lie within the polygon then check
				// for any edge intersections to confirm the containment.
				for i := 0; i < s2pgn.NumEdges(); i++ {
					edgeA := s2pgn.Edge(i)
					a := []float64{edgeA.V0.X, edgeA.V0.Y}
					b := []float64{edgeA.V1.X, edgeA.V1.Y}
					c := []float64{start.X, start.Y}
					d := []float64{end.X, end.Y}
					if doIntersect(a, b, c, d) {
						continue nextLine
					}
				}
				linesWithIn[lineIndex] = struct{}{}
				continue nextLine
			}
		}
	}

	return len(lineVertices) == len(linesWithIn)
}

func rectangleIntersectsWithPolygons(s2rect *s2.Rect,
	coordinates [][][][]float64) bool {
	s2pgnFromRect := s2PolygonFromS2Rectangle(s2rect)
	for _, pgnVertices := range coordinates {
		s2pgn := s2PolygonFromCoordinates(pgnVertices)
		if s2pgn.Intersects(s2pgnFromRect) {
			return true
		}
	}

	return false
}

func rectangleIntersectsWithLineStrings(s2rect *s2.Rect,
	coordinates [][][]float64) bool {

	var polylines []*s2.Polyline

	for _, lines := range coordinates {
		var latlngs []s2.LatLng
		for _, line := range lines {
			v := s2.LatLngFromDegrees(line[1], line[0])
			latlngs = append(latlngs, v)
		}
		pl := s2.PolylineFromLatLngs(latlngs)
		polylines = append(polylines, pl)
	}

	for _, pl := range polylines {
		for i := 0; i < pl.NumEdges(); i++ {
			edgeA := pl.Edge(i)
			a := []float64{edgeA.V0.X, edgeA.V0.Y}
			b := []float64{edgeA.V1.X, edgeA.V1.Y}

			for j := 0; j < 4; j++ {
				v1 := s2.PointFromLatLng(s2rect.Vertex(j))
				v2 := s2.PointFromLatLng(s2rect.Vertex((j + 1) % 4))

				c := []float64{v1.X, v1.Y}
				d := []float64{v2.X, v2.Y}

				if doIntersect(a, b, c, d) {
					return true
				}
			}
		}
	}

	return false
}

func s2PolygonFromCoordinates(coordinates [][][]float64) *s2.Polygon {
	loops := make([]*s2.Loop, 0, len(coordinates))
	for _, loop := range coordinates {
		var points []s2.Point
		for _, point := range loop {
			p := s2.PointFromLatLng(s2.LatLngFromDegrees(point[1], point[0]))
			points = append(points, p)
		}
		loops = append(loops, s2.LoopFromPoints(points))
	}

	return s2.PolygonFromOrientedLoops(loops)
}

func s2PolygonFromS2Rectangle(s2rect *s2.Rect) *s2.Polygon {
	loops := make([]*s2.Loop, 0, 1)
	var points []s2.Point
	for j := 0; j <= 4; j++ {
		points = append(points, s2.PointFromLatLng(s2rect.Vertex(j%4)))
	}

	loops = append(loops, s2.LoopFromPoints(points))
	return s2.PolygonFromLoops(loops)
}

func deduplicateTerms(terms []string) []string {
	var rv []string
	hash := make(map[string]struct{}, len(terms))
	for _, term := range terms {
		if _, exists := hash[term]; !exists {
			rv = append(rv, term)
			hash[term] = struct{}{}
		}
	}

	return rv
}

//----------------------------------------------------------------------

var earthRadiusInMeter = 6378137.0

func radiusInMetersToS1Angle(radius float64) s1.Angle {
	return s1.Angle(radius / earthRadiusInMeter)
}

func s2PolylinesFromCoordinates(coordinates [][][]float64) []*s2.Polyline {
	var polylines []*s2.Polyline
	for _, lines := range coordinates {
		var latlngs []s2.LatLng
		for _, line := range lines {
			v := s2.LatLngFromDegrees(line[1], line[0])
			latlngs = append(latlngs, v)
		}
		polylines = append(polylines, s2.PolylineFromLatLngs(latlngs))
	}
	return polylines
}

func s2RectFromBounds(topLeft, bottomRight []float64) *s2.Rect {
	rect := s2.EmptyRect()
	rect = rect.AddPoint(s2.LatLngFromDegrees(topLeft[1], topLeft[0]))
	rect = rect.AddPoint(s2.LatLngFromDegrees(bottomRight[1], bottomRight[0]))
	return &rect
}

func s2Cap(vertices []float64, radiusInMeter float64) *s2.Cap {
	cp := s2.PointFromLatLng(s2.LatLngFromDegrees(vertices[1], vertices[0]))
	angle := radiusInMetersToS1Angle(float64(radiusInMeter))
	cap := s2.CapFromCenterAngle(cp, angle)
	return &cap
}

func max(a, b float64) float64 {
	if a >= b {
		return a
	}
	return b
}

func min(a, b float64) float64 {
	if a >= b {
		return b
	}
	return a
}

func onsegment(p, q, r []float64) bool {
	if q[0] <= max(p[0], r[0]) && q[0] >= min(p[0], r[0]) &&
		q[1] <= max(p[1], r[1]) && q[1] >= min(p[1], r[1]) {
		return true
	}

	return false
}

func doIntersect(p1, q1, p2, q2 []float64) bool {
	o1 := orientation(p1, q1, p2)
	o2 := orientation(p1, q1, q2)
	o3 := orientation(p2, q2, p1)
	o4 := orientation(p2, q2, q1)

	if o1 != o2 && o3 != o4 {
		return true
	}

	if o1 == 0 && onsegment(p1, p2, q1) {
		return true
	}

	if o2 == 0 && onsegment(p1, q2, q1) {
		return true
	}

	if o3 == 0 && onsegment(p2, p1, q2) {
		return true
	}

	if o4 == 0 && onsegment(p2, q1, q2) {
		return true
	}

	return false
}

func orientation(p, q, r []float64) int {
	val := (q[1]-p[1])*(r[0]-q[0]) - (q[0]-p[0])*(r[1]-q[1])
	if val == 0 {
		return 0
	}
	if val > 0 {
		return 1
	}
	return 2
}

func stripCoveringTerms(terms []string) []string {
	rv := make([]string, 0, len(terms))
	for _, term := range terms {
		if strings.HasPrefix(term, "$") {
			rv = append(rv, term[1:])
			continue
		}
		rv = append(rv, term)
	}
	return deduplicateTerms(rv)
}
