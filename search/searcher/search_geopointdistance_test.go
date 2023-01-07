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
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

func TestGeoPointDistanceSearcher(t *testing.T) {

	tests := []struct {
		centerLon float64
		centerLat float64
		dist      float64
		field     string
		want      []string
	}{
		// approx 110567m per degree at equator
		{0.0, 0.0, 0, "loc", nil},
		{0.0, 0.0, 110567, "loc", []string{"a"}},
		{0.0, 0.0, 2 * 110567, "loc", []string{"a", "b"}},
		// stretching our approximation here
		{0.0, 0.0, 15 * 110567, "loc", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}},
	}

	i := setupGeo(t)
	indexReader, err := i.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	for _, test := range tests {
		got, err := testGeoPointDistanceSearch(indexReader, test.centerLon, test.centerLat, test.dist, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("expected %v, got %v for %f %f %f %s", test.want, got, test.centerLon, test.centerLat, test.dist, test.field)
		}

	}
}

func testGeoPointDistanceSearch(i index.IndexReader, centerLon, centerLat, dist float64, field string) ([]string, error) {
	var rv []string
	gds, err := NewGeoPointDistanceSearcher(nil, i, centerLon, centerLat, dist, field, 1.0, search.SearcherOptions{})
	if err != nil {
		return nil, err
	}
	ctx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(gds.DocumentMatchPoolSize(), 0),
	}
	docMatch, err := gds.Next(ctx)
	for docMatch != nil && err == nil {
		rv = append(rv, string(docMatch.IndexInternalID))
		docMatch, err = gds.Next(ctx)
	}
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func TestGeoPointDistanceCompare(t *testing.T) {
	tests := []struct {
		docLat, docLon       float64
		centerLat, centerLon float64
		distance             string
	}{
		// Data points originally from MB-33454.
		{
			docLat:    33.718,
			docLon:    -116.8293,
			centerLat: 39.59000587,
			centerLon: -119.22998428,
			distance:  "10000mi",
		},
		{
			docLat:    41.1305,
			docLon:    -121.6587,
			centerLat: 61.28,
			centerLon: -149.34,
			distance:  "10000mi",
		},
	}

	for testi, test := range tests {
		// compares the results from ComputeGeoRange with original, non-optimized version
		compare := func(desc string,
			minLon, minLat, maxLon, maxLat float64, checkBoundaries bool) {
			// do math to produce list of terms needed for this search
			onBoundaryRes, offBoundaryRes, err := ComputeGeoRange(nil, 0, GeoBitsShift1Minus1,
				minLon, minLat, maxLon, maxLat, checkBoundaries, nil, "")
			if err != nil {
				t.Fatal(err)
			}

			onBROrig, offBROrig := origComputeGeoRange(0, GeoBitsShift1Minus1,
				minLon, minLat, maxLon, maxLat, checkBoundaries)
			if !reflect.DeepEqual(onBoundaryRes, onBROrig) {
				t.Fatalf("testi: %d, test: %+v, desc: %s, onBoundaryRes != onBROrig,\n onBoundaryRes:%v,\n onBROrig: %v",
					testi, test, desc, onBoundaryRes, onBROrig)
			}
			if !reflect.DeepEqual(offBoundaryRes, offBROrig) {
				t.Fatalf("testi: %d, test: %+v, desc: %s, offBoundaryRes, offBROrig,\n offBoundaryRes: %v,\n offBROrig: %v",
					testi, test, desc, offBoundaryRes, offBROrig)
			}
		}

		// follow the general approach of the GeoPointDistanceSearcher...
		dist, err := geo.ParseDistance(test.distance)
		if err != nil {
			t.Fatal(err)
		}

		topLeftLon, topLeftLat, bottomRightLon, bottomRightLat, err :=
			geo.RectFromPointDistance(test.centerLon, test.centerLat, dist)
		if err != nil {
			t.Fatal(err)
		}

		if bottomRightLon < topLeftLon {
			// crosses date line, rewrite as two parts
			compare("-180/f", -180, bottomRightLat, bottomRightLon, topLeftLat, false)
			compare("-180/t", -180, bottomRightLat, bottomRightLon, topLeftLat, true)

			compare("180/f", topLeftLon, bottomRightLat, 180, topLeftLat, false)
			compare("180/t", topLeftLon, bottomRightLat, 180, topLeftLat, true)
		} else {
			compare("reg/f", topLeftLon, bottomRightLat, bottomRightLon, topLeftLat, false)
			compare("reg/t", topLeftLon, bottomRightLat, bottomRightLon, topLeftLat, true)
		}
	}
}
