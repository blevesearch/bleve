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
	"encoding/json"
	"reflect"
	"testing"
)

func TestExtractGeoPoint(t *testing.T) {
	tests := []struct {
		in      interface{}
		lon     float64
		lat     float64
		success bool
	}{
		// values are ints
		{
			in: map[string]interface{}{
				"lat": 5,
				"lon": 5,
			},
			lon:     5,
			lat:     5,
			success: true,
		},
		// values are uints
		{
			in: map[string]interface{}{
				"lat": uint(5),
				"lon": uint(5),
			},
			lon:     5,
			lat:     5,
			success: true,
		},
		// values float64 as with parsed JSON
		{
			in: map[string]interface{}{
				"lat": 5.0,
				"lon": 5.0,
			},
			lon:     5,
			lat:     5,
			success: true,
		},
		// values are bool (not supported)
		{
			in: map[string]interface{}{
				"lat": true,
				"lon": false,
			},
			lon:     0,
			lat:     0,
			success: false,
		},
		// using lng variant of lon
		{
			in: map[string]interface{}{
				"lat": 5.0,
				"lng": 5.0,
			},
			lon:     5,
			lat:     5,
			success: true,
		},
		// using struct
		{
			in: struct {
				Lon float64
				Lat float64
			}{
				Lon: 3.0,
				Lat: 7.5,
			},
			lon:     3.0,
			lat:     7.5,
			success: true,
		},
		// struct with lng alternate
		{
			in: struct {
				Lng float64
				Lat float64
			}{
				Lng: 3.0,
				Lat: 7.5,
			},
			lon:     3.0,
			lat:     7.5,
			success: true,
		},
		// test going throug interface
		{
			in: &s11{
				lon: 4.0,
				lat: 6.9,
			},
			lon:     4.0,
			lat:     6.9,
			success: true,
		},
		// test going throug interface with lng variant
		{
			in: &s12{
				lng: 4.0,
				lat: 6.9,
			},
			lon:     4.0,
			lat:     6.9,
			success: true,
		},
		// try GeoJSON slice
		{
			in:      []interface{}{3.4, 5.9},
			lon:     3.4,
			lat:     5.9,
			success: true,
		},
		// try GeoJSON slice too long
		{
			in:      []interface{}{3.4, 5.9, 9.4},
			lon:     0,
			lat:     0,
			success: false,
		},
		// slice of floats
		{
			in:      []float64{3.4, 5.9},
			lon:     3.4,
			lat:     5.9,
			success: true,
		},
		// values are nil (not supported)
		{
			in: map[string]interface{}{
				"lat": nil,
				"lon": nil,
			},
			lon:     0,
			lat:     0,
			success: false,
		},
		// input is nil
		{
			in:      nil,
			lon:     0,
			lat:     0,
			success: false,
		},
	}

	for _, test := range tests {
		lon, lat, success := ExtractGeoPoint(test.in)
		if success != test.success {
			t.Errorf("expected extract geo point %t, got %t for %v", test.success, success, test.in)
		}
		if lon != test.lon {
			t.Errorf("expected lon %f, got %f for %v", test.lon, lon, test.in)
		}
		if lat != test.lat {
			t.Errorf("expected lat %f, got %f for %v", test.lat, lat, test.in)
		}
	}
}

type s11 struct {
	lon float64
	lat float64
}

func (s *s11) Lon() float64 {
	return s.lon
}

func (s *s11) Lat() float64 {
	return s.lat
}

type s12 struct {
	lng float64
	lat float64
}

func (s *s12) Lng() float64 {
	return s.lng
}

func (s *s12) Lat() float64 {
	return s.lat
}

func TestExtractGeoShape(t *testing.T) {
	tests := []struct {
		in      interface{}
		resTyp  string
		result  [][][][]float64
		success bool
	}{
		// valid point slice
		{
			in: map[string]interface{}{
				"coordinates": []interface{}{3.4, 5.9},
				"type":        "Point",
			},
			resTyp:  "point",
			result:  [][][][]float64{{{{3.4, 5.9}}}},
			success: true,
		},
		// invalid point slice
		{
			in: map[string]interface{}{
				"coordinates": []interface{}{3.4},
				"type":        "point"},

			resTyp:  "point",
			result:  nil,
			success: false,
		},
		// valid multipoint slice containing single point
		{
			in: map[string]interface{}{
				"coordinates": [][]interface{}{{3.4, 5.9}},
				"type":        "multipoint"},
			resTyp:  "multipoint",
			result:  [][][][]float64{{{{3.4, 5.9}}}},
			success: true,
		},
		// valid multipoint slice
		{
			in: map[string]interface{}{
				"coordinates": [][]interface{}{{3.4, 5.9}, {6.7, 9.8}},
				"type":        "multipoint"},
			resTyp:  "multipoint",
			result:  [][][][]float64{{{{3.4, 5.9}, {6.7, 9.8}}}},
			success: true,
		},
		// valid multipoint slice containing one invalid entry
		{
			in: map[string]interface{}{
				"coordinates": [][]interface{}{{3.4, 5.9}, {6.7}},
				"type":        "multipoint"},
			resTyp:  "multipoint",
			result:  [][][][]float64{{{{3.4, 5.9}}}},
			success: true,
		},
		// invalid multipoint slice
		{
			in: map[string]interface{}{
				"coordinates": [][]interface{}{{3.4}},
				"type":        "multipoint"},
			resTyp:  "multipoint",
			result:  nil,
			success: false,
		},
		// valid linestring slice
		{
			in: map[string]interface{}{
				"coordinates": [][]interface{}{{3.4, 4.4}, {8.4, 9.4}},
				"type":        "linestring"},
			resTyp:  "linestring",
			result:  [][][][]float64{{{{3.4, 4.4}, {8.4, 9.4}}}},
			success: true,
		},
		// valid linestring slice
		{
			in: map[string]interface{}{
				"coordinates": [][]interface{}{{3.4, 4.4}, {8.4, 9.4}, {10.1, 12.3}},
				"type":        "linestring"},
			resTyp:  "linestring",
			result:  [][][][]float64{{{{3.4, 4.4}, {8.4, 9.4}, {10.1, 12.3}}}},
			success: true,
		},
		// invalid linestring slice with single entry
		{
			in: map[string]interface{}{
				"coordinates": [][]interface{}{{3.4, 4.4}},
				"type":        "linestring"},
			resTyp:  "linestring",
			result:  nil,
			success: false,
		},
		// invalid linestring slice with wrong paranthesis
		{
			in: map[string]interface{}{
				"coordinates": [][][]interface{}{{{3.4, 4.4}, {8.4, 9.4}}},
				"type":        "linestring"},
			resTyp:  "linestring",
			result:  nil,
			success: false,
		},
		// valid envelope
		{
			in: map[string]interface{}{
				"coordinates": [][]interface{}{{3.4, 4.4}, {8.4, 9.4}},
				"type":        "envelope"},
			resTyp:  "envelope",
			result:  [][][][]float64{{{{3.4, 4.4}, {8.4, 9.4}}}},
			success: true,
		},
		// invalid envelope
		{
			in: map[string]interface{}{
				"coordinates": [][]interface{}{{3.4, 4.4}},
				"type":        "envelope"},
			resTyp:  "envelope",
			result:  nil,
			success: false,
		},
		// invalid envelope
		{
			in: map[string]interface{}{
				"coordinates": [][][]interface{}{{{3.4, 4.4}, {8.4, 9.4}}},
				"type":        "envelope"},
			resTyp:  "envelope",
			result:  nil,
			success: false,
		},
		// invalid envelope with >2 vertices
		{
			in: map[string]interface{}{
				"coordinates": [][]interface{}{{3.4, 4.4}, {5.6, 6.4}, {7.4, 7.4}},
				"type":        "envelope"},
			resTyp:  "envelope",
			result:  nil,
			success: false,
		},
	}

	for _, test := range tests {
		result, shapeType, success := extractGeoShape(test.in)
		if success != test.success {
			t.Errorf("expected extract geo point: %t, got: %t for: %v", test.success, success, test.in)
		}
		if shapeType != test.resTyp {
			t.Errorf("expected shape type: %v, got: %v for input: %v", test.resTyp, shapeType, test.in)
		}
		if !reflect.DeepEqual(test.result, result) {
			t.Errorf("expected result %+v, got %+v for %v", test.result, result, test.in)
		}
	}
}

func TestExtractGeoShapeCoordinates(t *testing.T) {
	tests := []struct {
		x        []byte
		typ      string
		expectOK bool
	}{
		{
			x: []byte(`[
				[
					[77.58894681930542,12.976498523818783],
					[77.58677959442139,12.974533005048169],
					[77.58894681930542,12.976498523818783]
				]
			]`),
			typ:      PolygonType,
			expectOK: true,
		},
		{ // Invalid construct, but handled
			x: []byte(`[
				[
					{"lon":77.58894681930542,"lat":12.976498523818783},
					{"lon":77.58677959442139,"lat":12.974533005048169},
					{"lon":77.58894681930542,"lat":12.976498523818783}
				]
			]`),
			typ:      PolygonType,
			expectOK: false,
		},
		{ // Invalid construct causes panic (within extract3DCoordinates), fix MB-65807
			x: []byte(`{
				"coordinates": [
					[77.58894681930542,12.976498523818783],
					[77.58677959442139,12.974533005048169],
					[77.58894681930542,12.976498523818783]
				]
			}`),
			typ:      PolygonType,
			expectOK: false,
		},
		{
			x: []byte(`[
				[
					[
						[-0.163421630859375,51.531600743186644],
						[-0.15277862548828125,51.52455221546295],
						[-0.15895843505859375,51.53693981046689],
						[-0.163421630859375,51.531600743186644]
					]
				],
				[
					[
						[-0.1902008056640625,51.5091698216777],
						[-0.1599884033203125,51.51322956905176],
						[-0.1902008056640625,51.5091698216777]
					]
				]
			]`),
			typ:      MultiPolygonType,
			expectOK: true,
		},
		{ // Invalid construct causes panic (within extract3DCoordinates), fix MB-65807
			x: []byte(`[
				{
					"coordinates": [
						[-0.163421630859375,51.531600743186644],
						[-0.15277862548828125,51.52455221546295],
						[-0.15895843505859375,51.53693981046689],
						[-0.163421630859375,51.531600743186644]
					]
				},
				{
					"coordinates": [
						[-0.1902008056640625,51.5091698216777],
						[-0.1599884033203125,51.51322956905176],
						[-0.1902008056640625,51.5091698216777]
					]
				}
			]`),
			typ:      MultiPolygonType,
			expectOK: false,
		},
	}

	for i := range tests {
		var x interface{}
		if err := json.Unmarshal(tests[i].x, &x); err != nil {
			t.Fatalf("[%d] JSON err: %v", i+1, err)
		}

		_, typ, ok := ExtractGeoShapeCoordinates(x, tests[i].typ)
		if ok != tests[i].expectOK {
			t.Errorf("[%d] expected ok %t, got %t", i+1, tests[i].expectOK, ok)
		}

		if ok && typ != tests[i].typ {
			t.Errorf("[%d] expected type %s, got %s", i+1, tests[i].typ, typ)
		}
	}
}
