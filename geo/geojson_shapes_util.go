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
)

// FilterGeoShapesOnRelation extracts the shapes in the document, apply
// the `relation` filter and confirms whether the shape in the document
//  satisfies the given relation.
func FilterGeoShapesOnRelation(shape index.GeoJSON, targetShapeBytes []byte,
	relation string) (bool, error) {

	shapeInDoc, err := extractShapesFromBytes(targetShapeBytes)
	if err != nil {
		return false, err
	}

	return filterShapes(shape, shapeInDoc, relation)
}

// extractShapesFromBytes unmarshal the bytes to retrieve the
// embedded geojson shape.
func extractShapesFromBytes(targetShapeBytes []byte) (
	index.GeoJSON, error) {
	meta := struct {
		Typ string `json:"type"`
	}{}

	err := json.Unmarshal(targetShapeBytes, &meta)
	if err != nil {
		return nil, err
	}

	var shapeInDoc index.GeoJSON

	switch strings.ToLower(meta.Typ) {
	case PolygonType:
		shapeInDoc = &polygon{Typ: meta.Typ}
		err := json.Unmarshal(targetShapeBytes, shapeInDoc)
		if err != nil {
			return nil, err
		}
		return shapeInDoc, nil

	case MultiPolygonType:
		shapeInDoc = &multipolygon{Typ: meta.Typ}
		err := json.Unmarshal(targetShapeBytes, shapeInDoc)
		if err != nil {
			return nil, err
		}
		return shapeInDoc, nil

	case PointType:
		shapeInDoc = &point{Typ: meta.Typ}
		err := json.Unmarshal(targetShapeBytes, shapeInDoc)
		if err != nil {
			return nil, err
		}
		return shapeInDoc, nil

	case MultiPointType:
		shapeInDoc = &multipoint{Typ: meta.Typ}
		err := json.Unmarshal(targetShapeBytes, shapeInDoc)
		if err != nil {
			return nil, err
		}
		return shapeInDoc, nil

	case LineStringType:
		shapeInDoc = &linestring{Typ: meta.Typ}
		err := json.Unmarshal(targetShapeBytes, shapeInDoc)
		if err != nil {
			return nil, err
		}
		return shapeInDoc, nil

	case MultiLineStringType:
		shapeInDoc = &multilinestring{Typ: meta.Typ}
		err := json.Unmarshal(targetShapeBytes, shapeInDoc)
		if err != nil {
			return nil, err
		}
		return shapeInDoc, nil

	case GeometryCollectionType:
		shapeInDoc = &geometryCollection{Typ: meta.Typ}
		err := json.Unmarshal(targetShapeBytes, shapeInDoc)
		if err != nil {
			return nil, err
		}
		return shapeInDoc, nil

	case CircleType:
		shapeInDoc = &circle{Typ: meta.Typ}
		err := json.Unmarshal(targetShapeBytes, shapeInDoc)
		if err != nil {
			return nil, err
		}
		return shapeInDoc, nil

	case EnvelopeType:
		shapeInDoc = &envelope{Typ: meta.Typ}
		err := json.Unmarshal(targetShapeBytes, shapeInDoc)
		if err != nil {
			return nil, err
		}
		return shapeInDoc, nil

	}

	return nil, nil
}

// filterShapes applies the given relation between the query shape
// and the shape in the document.
func filterShapes(shape index.GeoJSON,
	shapeInDoc index.GeoJSON, relation string) (bool, error) {

	if relation == "intersects" {
		return shape.Intersects(shapeInDoc)
	}

	if relation == "contains" {
		return shapeInDoc.Contains(shape)
	}

	if relation == "within" {
		return shape.Contains(shapeInDoc)
	}

	return false, fmt.Errorf("unknown relation: %s", relation)
}

// ParseGeoJSONShape unmarshals the geojson/circle/envelope shape
// embedded in the given bytes.
func ParseGeoJSONShape(input []byte) (index.GeoJSON, error) {
	var sType string
	var tmp struct {
		Typ string `json:"type"`
	}
	err := json.Unmarshal(input, &tmp)
	if err != nil {
		return nil, err
	}

	sType = strings.ToLower(tmp.Typ)

	switch sType {
	case PolygonType:
		var rv polygon
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil

	case MultiPolygonType:
		var rv multipolygon
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil

	case PointType:
		var rv point
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil

	case MultiPointType:
		var rv multipoint
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil

	case LineStringType:
		var rv linestring
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil

	case MultiLineStringType:
		var rv multilinestring
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil

	case GeometryCollectionType:
		var rv geometryCollection
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil

	case CircleType:
		var rv circle
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil

	case EnvelopeType:
		var rv envelope
		err := json.Unmarshal(input, &rv)
		if err != nil {
			return nil, err
		}
		return &rv, nil

	default:
		return nil, fmt.Errorf("unknown shape type: %s", sType)
	}

	return nil, err
}

// NewGeoJsonShape instantiate a geojson shape/circle or
// an envelope from the given coordinates and type.
func NewGeoJsonShape(coordinates [][][][]float64, typ string) (
	rv index.GeoJSON, value []byte, err error) {
	if len(coordinates) == 0 {
		return nil, nil, fmt.Errorf("missing coordinates")
	}

	typ = strings.ToLower(typ)

	switch typ {
	case PointType:
		rv = &point{Typ: typ, Vertices: coordinates[0][0][0]}
		value, err = json.Marshal(rv)
		if err != nil {
			return rv, nil, err
		}

	case MultiPointType:
		rv = &multipoint{Typ: typ, Vertices: coordinates[0][0]}
		value, err = json.Marshal(rv)
		if err != nil {
			return rv, nil, err
		}

	case LineStringType:
		rv = &linestring{Typ: typ, Vertices: coordinates[0][0]}
		value, err = json.Marshal(rv)
		if err != nil {
			return rv, nil, err
		}

	case MultiLineStringType:
		rv = &multilinestring{Typ: typ, Vertices: coordinates[0]}
		value, err = json.Marshal(rv)
		if err != nil {
			return rv, nil, err
		}

	case PolygonType:
		rv = &polygon{Typ: typ, Vertices: coordinates[0]}
		value, err = json.Marshal(rv)
		if err != nil {
			return rv, nil, err
		}

	case MultiPolygonType:
		rv = &multipolygon{Typ: typ, Vertices: coordinates}
		value, err = json.Marshal(rv)
		if err != nil {
			return rv, nil, err
		}

	case EnvelopeType:
		rv = &envelope{Typ: typ, Vertices: coordinates[0][0]}
		value, err = json.Marshal(rv)
		if err != nil {
			return rv, nil, err
		}

	default:
		return rv, nil, fmt.Errorf("unknown shape type: %s", typ)
	}

	value = append(GlueBytes, value...)
	return rv, value, nil
}

// NewGeometryCollection instantiate a geometrycollection
// and prefix the byte contents with certain glue bytes that
// can be used later while filering the doc values.
func NewGeometryCollection(coordinates [][][][][]float64,
	typs []string) (index.GeoJSON, []byte, error) {
	shapes := make([]index.GeoJSON, 0, len(coordinates))
	for i, vertices := range coordinates {
		s, _, err := NewGeoJsonShape(vertices, typs[i])
		if err != nil {
			continue
		}
		shapes = append(shapes, s)
	}

	var gc geometryCollection
	gc.Typ = GeometryCollectionType
	gc.Shapes = shapes
	vbytes, err := json.Marshal(&gc)
	if err != nil {
		return nil, nil, err
	}

	vbytes = append(GlueBytes, vbytes...)
	return &gc, vbytes, nil
}

// NewGeoCircleShape instantiate a circle shape and
// prefix the byte contents with certain glue bytes that
// can be used later while filering the doc values.
func NewGeoCircleShape(cp []float64,
	radiusInMeter float64) (*circle, []byte, error) {
	rv := &circle{Typ: CircleType, Vertices: cp,
		RadiusInMeters: radiusInMeter}
	vbytes, err := json.Marshal(rv)
	if err != nil {
		return nil, nil, err
	}

	vbytes = append(GlueBytes, vbytes...)
	return rv, vbytes, nil
}
