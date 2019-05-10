//  Copyright (c) 2019 Couchbase, Inc.
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
	"testing"
)

func TestDecodeGeoHash(t *testing.T) {
	tests := []struct {
		hash string
		lon  float64
		lat  float64
	}{
		{"d3hn3", -73.059082, 6.745605},     // -73.05908203, 6.74560547 as per http://geohash.co/
		{"u4pru", 10.393066, 57.634277},     // 10.39306641, 57.63427734
		{"u4pruy", 10.409546, 57.648010},    // 10.40954590, 57.64801025
		{"u4pruyd", 10.407486, 57.648697},   // 10.40748596, 57.64869690
		{"u4pruydqqvj", 10.40744, 57.64911}, // 10.40743969, 57.64911063
	}

	for _, test := range tests {
		lat, lon := DecodeGeoHash(test.hash)

		if compareGeo(test.lon, lon) != 0 {
			t.Errorf("expected lon %f, got %f, hash %s", test.lon, lon, test.hash)
		}
		if compareGeo(test.lat, lat) != 0 {
			t.Errorf("expected lat %f, got %f, hash %s", test.lat, lat, test.hash)
		}
	}
}

func TestEncodeGeoHash(t *testing.T) {
	tests := []struct {
		lon  float64
		lat  float64
		hash string
	}{
		{2.29449034, 48.85841131, "u09tunquc"},
		{76.491540, 10.060349, "t9y3hx7my0fp"},
	}

	for _, test := range tests {
		hash := EncodeGeoHash(test.lat, test.lon)

		if !strings.HasPrefix(hash, test.hash) {
			t.Errorf("expected hash %s, got %s", test.hash, hash)
		}
	}
}
