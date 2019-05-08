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
	"testing"
)

func TestGeoHash(t *testing.T) {
	tests := []struct {
		hash string
		lon  float64
		lat  float64
	}{
		{"d3hn3", -73.080000, 6.730000},     // -73.05908203, 6.74560547 as per http://geohash.co/
		{"u4pru", 10.380000, 57.620000},     // 10.39306641, 57.63427734
		{"u4pruy", 10.410000, 57.646000},    // 10.40954590, 57.64801025
		{"u4pruyd", 10.407000, 57.649000},   // 10.40748596, 57.64869690
		{"u4pruydqqvj", 10.40744, 57.64911}, // 10.40743969, 57.64911063
	}

	for _, test := range tests {
		lat, lon := GeoHashDecode(test.hash)

		if compareGeo(test.lon, lon) != 0 {
			t.Errorf("expected lon %f, got %f, hash %s", test.lon, lon, test.hash)
		}
		if compareGeo(test.lat, lat) != 0 {
			t.Errorf("expected lat %f, got %f, hash %s", test.lat, lat, test.hash)
		}
	}
}
