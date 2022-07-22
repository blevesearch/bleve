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

package geo

import (
	"reflect"
	"strconv"
	"strings"
)

// ExtractGeoPoint takes an arbitrary interface{} and tries it's best to
// interpret it is as geo point.  Supported formats:
// Container:
// slice length 2 (GeoJSON)
//  first element lon, second element lat
// string (coordinates separated by comma, or a geohash)
//  first element lat, second element lon
// map[string]interface{}
//  exact keys lat and lon or lng
// struct
//  w/exported fields case-insensitive match on lat and lon or lng
// struct
//  satisfying Later and Loner or Lnger interfaces
//
// in all cases values must be some sort of numeric-like thing: int/uint/float
func ExtractGeoPoint(thing interface{}) (lon, lat float64, success bool) {
	var foundLon, foundLat bool

	thingVal := reflect.ValueOf(thing)
	if !thingVal.IsValid() {
		return lon, lat, false
	}

	thingTyp := thingVal.Type()

	// is it a slice
	if thingVal.Kind() == reflect.Slice {
		// must be length 2
		if thingVal.Len() == 2 {
			first := thingVal.Index(0)
			if first.CanInterface() {
				firstVal := first.Interface()
				lon, foundLon = extractNumericVal(firstVal)
			}
			second := thingVal.Index(1)
			if second.CanInterface() {
				secondVal := second.Interface()
				lat, foundLat = extractNumericVal(secondVal)
			}
		}
	}

	// is it a string
	if thingVal.Kind() == reflect.String {
		geoStr := thingVal.Interface().(string)
		if strings.Contains(geoStr, ",") {
			// geo point with coordinates split by comma
			points := strings.Split(geoStr, ",")
			for i, point := range points {
				// trim any leading or trailing white spaces
				points[i] = strings.TrimSpace(point)
			}
			if len(points) == 2 {
				var err error
				lat, err = strconv.ParseFloat(points[0], 64)
				if err == nil {
					foundLat = true
				}
				lon, err = strconv.ParseFloat(points[1], 64)
				if err == nil {
					foundLon = true
				}
			}
		} else {
			// geohash
			if len(geoStr) <= geoHashMaxLength {
				lat, lon = DecodeGeoHash(geoStr)
				foundLat = true
				foundLon = true
			}
		}
	}

	// is it a map
	if l, ok := thing.(map[string]interface{}); ok {
		if lval, ok := l["lon"]; ok {
			lon, foundLon = extractNumericVal(lval)
		} else if lval, ok := l["lng"]; ok {
			lon, foundLon = extractNumericVal(lval)
		}
		if lval, ok := l["lat"]; ok {
			lat, foundLat = extractNumericVal(lval)
		}
	}

	// now try reflection on struct fields
	if thingVal.Kind() == reflect.Struct {
		for i := 0; i < thingVal.NumField(); i++ {
			fieldName := thingTyp.Field(i).Name
			if strings.HasPrefix(strings.ToLower(fieldName), "lon") {
				if thingVal.Field(i).CanInterface() {
					fieldVal := thingVal.Field(i).Interface()
					lon, foundLon = extractNumericVal(fieldVal)
				}
			}
			if strings.HasPrefix(strings.ToLower(fieldName), "lng") {
				if thingVal.Field(i).CanInterface() {
					fieldVal := thingVal.Field(i).Interface()
					lon, foundLon = extractNumericVal(fieldVal)
				}
			}
			if strings.HasPrefix(strings.ToLower(fieldName), "lat") {
				if thingVal.Field(i).CanInterface() {
					fieldVal := thingVal.Field(i).Interface()
					lat, foundLat = extractNumericVal(fieldVal)
				}
			}
		}
	}

	// last hope, some interfaces
	// lon
	if l, ok := thing.(loner); ok {
		lon = l.Lon()
		foundLon = true
	} else if l, ok := thing.(lnger); ok {
		lon = l.Lng()
		foundLon = true
	}
	// lat
	if l, ok := thing.(later); ok {
		lat = l.Lat()
		foundLat = true
	}

	return lon, lat, foundLon && foundLat
}

// extract numeric value (if possible) and returns a float64
func extractNumericVal(v interface{}) (float64, bool) {
	val := reflect.ValueOf(v)
	if !val.IsValid() {
		return 0, false
	}
	typ := val.Type()
	switch typ.Kind() {
	case reflect.Float32, reflect.Float64:
		return val.Float(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(val.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(val.Uint()), true
	}

	return 0, false
}

// various support interfaces which can be used to find lat/lon
type loner interface {
	Lon() float64
}

type later interface {
	Lat() float64
}

type lnger interface {
	Lng() float64
}

// GlueBytes primarily for quicker filtering of docvalues
// during the filtering phase.
var GlueBytes = []byte("##")

var GlueBytesOffset = len(GlueBytes)

func extractCoordinates(thing interface{}) []float64 {
	thingVal := reflect.ValueOf(thing)
	if !thingVal.IsValid() {
		return nil
	}

	if thingVal.Kind() == reflect.Slice {
		// must be length 2
		if thingVal.Len() == 2 {
			var foundLon, foundLat bool
			var lon, lat float64
			first := thingVal.Index(0)
			if first.CanInterface() {
				firstVal := first.Interface()
				lon, foundLon = extractNumericVal(firstVal)
			}
			second := thingVal.Index(1)
			if second.CanInterface() {
				secondVal := second.Interface()
				lat, foundLat = extractNumericVal(secondVal)
			}

			if !foundLon || !foundLat {
				return nil
			}

			return []float64{lon, lat}
		}
	}
	return nil
}

func extract2DCoordinates(thing interface{}) [][]float64 {
	thingVal := reflect.ValueOf(thing)
	if !thingVal.IsValid() {
		return nil
	}

	rv := make([][]float64, 0, 8)
	if thingVal.Kind() == reflect.Slice {
		for j := 0; j < thingVal.Len(); j++ {
			edges := thingVal.Index(j).Interface()
			if es, ok := edges.([]interface{}); ok {
				v := extractCoordinates(es)
				if len(v) == 2 {
					rv = append(rv, v)
				}
			}
		}

		return rv
	}

	return nil
}

func extract3DCoordinates(thing interface{}) (c [][][]float64) {
	coords := reflect.ValueOf(thing)
	for i := 0; i < coords.Len(); i++ {
		vals := coords.Index(i)

		edges := vals.Interface()
		if es, ok := edges.([]interface{}); ok {
			loop := extract2DCoordinates(es)
			if len(loop) > 0 {
				c = append(c, loop)
			}
		}
	}

	return c
}

func extract4DCoordinates(thing interface{}) (rv [][][][]float64) {
	thingVal := reflect.ValueOf(thing)
	if !thingVal.IsValid() {
		return nil
	}

	if thingVal.Kind() == reflect.Slice {
		for j := 0; j < thingVal.Len(); j++ {
			c := extract3DCoordinates(thingVal.Index(j).Interface())
			rv = append(rv, c)
		}
	}

	return rv
}

func ParseGeoShapeField(thing interface{}) (interface{}, string, error) {
	thingVal := reflect.ValueOf(thing)
	if !thingVal.IsValid() {
		return nil, "", nil
	}

	var shape string
	var coordValue interface{}

	if thingVal.Kind() == reflect.Map {
		iter := thingVal.MapRange()
		for iter.Next() {
			if iter.Key().String() == "type" {
				shape = iter.Value().Interface().(string)
				continue
			}

			if iter.Key().String() == "coordinates" {
				coordValue = iter.Value().Interface()
			}
		}
	}

	return coordValue, strings.ToLower(shape), nil
}

func extractGeoShape(thing interface{}) ([][][][]float64, string, bool) {
	coordValue, typ, err := ParseGeoShapeField(thing)
	if err != nil {
		return nil, "", false
	}

	return ExtractGeoShapeCoordinates(coordValue, typ)
}

// ExtractGeometryCollection takes an interface{} and tries it's best to
// interpret all the member geojson shapes within it.
func ExtractGeometryCollection(thing interface{}) ([][][][][]float64, []string, bool) {
	thingVal := reflect.ValueOf(thing)
	if !thingVal.IsValid() {
		return nil, nil, false
	}
	var rv [][][][][]float64
	var types []string
	var f bool

	if thingVal.Kind() == reflect.Map {
		iter := thingVal.MapRange()
		for iter.Next() {

			if iter.Key().String() == "type" {
				continue
			}

			if iter.Key().String() == "geometries" {
				collection := iter.Value().Interface()
				items := reflect.ValueOf(collection)

				for j := 0; j < items.Len(); j++ {
					coords, shape, found := extractGeoShape(items.Index(j).Interface())
					if found {
						f = found
						rv = append(rv, coords)
						types = append(types, shape)
					}
				}
			}
		}
	}

	return rv, types, f
}

// ExtractCircle takes an interface{} and tries it's best to
// interpret the center point coordinates and the radius for a
// given circle shape.
func ExtractCircle(thing interface{}) ([]float64, string, bool) {
	thingVal := reflect.ValueOf(thing)
	if !thingVal.IsValid() {
		return nil, "", false
	}
	var rv []float64
	var radiusStr string

	if thingVal.Kind() == reflect.Map {
		iter := thingVal.MapRange()
		for iter.Next() {

			if iter.Key().String() == "radius" {
				radiusStr = iter.Value().Interface().(string)
				continue
			}

			if iter.Key().String() == "coordinates" {
				lng, lat, found := ExtractGeoPoint(iter.Value().Interface())
				if !found {
					return nil, radiusStr, false
				}
				rv = append(rv, lng)
				rv = append(rv, lat)
			}
		}
	}

	return rv, radiusStr, true
}

// ExtractGeoShapeCoordinates takes an interface{} and tries it's best to
// interpret the coordinates for any of the given geoshape typ like
// a point, multipoint, linestring, multilinestring, polygon, multipolygon,
func ExtractGeoShapeCoordinates(coordValue interface{},
	typ string) ([][][][]float64, string, bool) {
	var rv [][][][]float64
	if typ == PointType {
		point := extractCoordinates(coordValue)

		// ignore the contents with invalid entry.
		if len(point) < 2 {
			return nil, typ, false
		}

		rv = [][][][]float64{{{point}}}
		return rv, typ, true
	}

	if typ == MultiPointType || typ == LineStringType ||
		typ == EnvelopeType {
		coords := extract2DCoordinates(coordValue)

		// ignore the contents with invalid entry.
		if len(coords) == 0 {
			return nil, typ, false
		}

		if typ == EnvelopeType && len(coords) != 2 {
			return nil, typ, false
		}

		if typ == LineStringType && len(coords) < 2 {
			return nil, typ, false
		}

		rv = [][][][]float64{{coords}}
		return rv, typ, true
	}

	if typ == PolygonType || typ == MultiLineStringType {
		coords := extract3DCoordinates(coordValue)

		// ignore the contents with invalid entry.
		if len(coords) == 0 {
			return nil, typ, false
		}

		if typ == PolygonType && len(coords[0]) < 3 ||
			typ == MultiLineStringType && len(coords[0]) < 2 {
			return nil, typ, false
		}

		rv = [][][][]float64{coords}
		return rv, typ, true
	}

	if typ == MultiPolygonType {
		rv = extract4DCoordinates(coordValue)

		// ignore the contents with invalid entry.
		if len(rv) == 0 || len(rv[0]) == 0 {
			return nil, typ, false

		}

		if len(rv[0][0]) < 3 {
			return nil, typ, false
		}

		return rv, typ, true
	}

	return rv, typ, false
}
