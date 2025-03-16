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
	"context"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/index/upsidedown"
	"github.com/blevesearch/bleve/v2/index/upsidedown/store/gtreap"
	"github.com/blevesearch/bleve/v2/numeric"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

func TestGeoBoundingBox(t *testing.T) {
	tests := []struct {
		minLon float64
		minLat float64
		maxLon float64
		maxLat float64
		field  string
		want   []string
	}{
		{10.001, 10.001, 20.002, 20.002, "loc", nil},
		{0.001, 0.001, 0.002, 0.002, "loc", []string{"a"}},
		{0.001, 0.001, 1.002, 1.002, "loc", []string{"a", "b"}},
		{0.001, 0.001, 9.002, 9.002, "loc", []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}},
		// same upper-left, bottom-right point
		{25, 25, 25, 25, "loc", nil},
		// box that would return points, but points reversed
		{0.002, 0.002, 0.001, 0.001, "loc", nil},
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
		got, err := testGeoBoundingBoxSearch(indexReader, test.minLon, test.minLat, test.maxLon, test.maxLat, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("expected %v, got %v for %f %f %f %f %s", test.want, got, test.minLon, test.minLat, test.maxLon, test.maxLat, test.field)
		}

	}
}

func testGeoBoundingBoxSearch(i index.IndexReader, minLon, minLat, maxLon, maxLat float64, field string) ([]string, error) {
	var rv []string
	gbs, err := NewGeoBoundingBoxSearcher(context.TODO(), i, minLon, minLat, maxLon, maxLat, field, 1.0, search.SearcherOptions{}, true)
	if err != nil {
		return nil, err
	}
	ctx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(gbs.DocumentMatchPoolSize(), 0),
	}
	docMatch, err := gbs.Next(ctx)
	for docMatch != nil && err == nil {
		rv = append(rv, string(docMatch.IndexInternalID))
		docMatch, err = gbs.Next(ctx)
	}
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func setupGeo(t *testing.T) index.Index {
	analysisQueue := index.NewAnalysisQueue(1)
	i, err := upsidedown.NewUpsideDownCouch(
		gtreap.Name,
		map[string]interface{}{
			"path": "",
		},
		analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = i.Open()
	if err != nil {
		t.Fatal(err)
	}
	doc := document.NewDocument("a")
	doc.AddField(document.NewGeoPointField("loc", []uint64{}, 0.0015, 0.0015))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("b")
	doc.AddField(document.NewGeoPointField("loc", []uint64{}, 1.0015, 1.0015))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("c")
	doc.AddField(document.NewGeoPointField("loc", []uint64{}, 2.0015, 2.0015))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("d")
	doc.AddField(document.NewGeoPointField("loc", []uint64{}, 3.0015, 3.0015))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("e")
	doc.AddField(document.NewGeoPointField("loc", []uint64{}, 4.0015, 4.0015))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("f")
	doc.AddField(document.NewGeoPointField("loc", []uint64{}, 5.0015, 5.0015))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("g")
	doc.AddField(document.NewGeoPointField("loc", []uint64{}, 6.0015, 6.0015))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("h")
	doc.AddField(document.NewGeoPointField("loc", []uint64{}, 7.0015, 7.0015))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("i")
	doc.AddField(document.NewGeoPointField("loc", []uint64{}, 8.0015, 8.0015))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("j")
	doc.AddField(document.NewGeoPointField("loc", []uint64{}, 9.0015, 9.0015))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	return i
}

func TestComputeGeoRange(t *testing.T) {
	tests := []struct {
		degs        float64
		onBoundary  int
		offBoundary int
		err         string
	}{
		{0.01, 4, 0, ""},
		{0.1, 56, 144, ""},
		{100.0, 32768, 258560, ""},
	}

	for testi, test := range tests {
		onBoundaryRes, offBoundaryRes, err := ComputeGeoRange(context.TODO(), 0, GeoBitsShift1Minus1,
			-1.0*test.degs, -1.0*test.degs, test.degs, test.degs, true, nil, "")
		if (err != nil) != (test.err != "") {
			t.Errorf("test: %+v, err: %v", test, err)
		}
		if len(onBoundaryRes) != test.onBoundary {
			t.Errorf("test: %+v, onBoundaryRes: %v", test, len(onBoundaryRes))
		}
		if len(offBoundaryRes) != test.offBoundary {
			t.Errorf("test: %+v, offBoundaryRes: %v", test, len(offBoundaryRes))
		}

		onBROrig, offBROrig := origComputeGeoRange(0, GeoBitsShift1Minus1,
			-1.0*test.degs, -1.0*test.degs, test.degs, test.degs, true)
		if !reflect.DeepEqual(onBoundaryRes, onBROrig) {
			t.Errorf("testi: %d, test: %+v, onBoundaryRes != onBROrig,\n onBoundaryRes:%v,\n onBROrig: %v",
				testi, test, onBoundaryRes, onBROrig)
		}
		if !reflect.DeepEqual(offBoundaryRes, offBROrig) {
			t.Errorf("testi: %d, test: %+v, offBoundaryRes, offBROrig,\n offBoundaryRes: %v,\n offBROrig: %v",
				testi, test, offBoundaryRes, offBROrig)
		}
	}
}

// --------------------------------------------------------------------

func BenchmarkComputeGeoRangePt01(b *testing.B) {
	onBoundary := 4
	offBoundary := 0
	benchmarkComputeGeoRange(b, -0.01, -0.01, 0.01, 0.01, onBoundary, offBoundary)
}

func BenchmarkComputeGeoRangePt1(b *testing.B) {
	onBoundary := 56
	offBoundary := 144
	benchmarkComputeGeoRange(b, -0.1, -0.1, 0.1, 0.1, onBoundary, offBoundary)
}

func BenchmarkComputeGeoRange10(b *testing.B) {
	onBoundary := 5464
	offBoundary := 53704
	benchmarkComputeGeoRange(b, -10.0, -10.0, 10.0, 10.0, onBoundary, offBoundary)
}

func BenchmarkComputeGeoRange100(b *testing.B) {
	onBoundary := 32768
	offBoundary := 258560
	benchmarkComputeGeoRange(b, -100.0, -100.0, 100.0, 100.0, onBoundary, offBoundary)
}

// --------------------------------------------------------------------

func benchmarkComputeGeoRange(b *testing.B,
	minLon, minLat, maxLon, maxLat float64, onBoundary, offBoundary int,
) {
	checkBoundaries := true

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		onBoundaryRes, offBoundaryRes, err := ComputeGeoRange(context.TODO(), 0, GeoBitsShift1Minus1, minLon, minLat, maxLon, maxLat, checkBoundaries, nil, "")
		if err != nil {
			b.Fatalf("expected no err")
		}
		if len(onBoundaryRes) != onBoundary || len(offBoundaryRes) != offBoundary {
			b.Fatalf("boundaries not matching")
		}
	}
}

// --------------------------------------------------------------------

// original, non-optimized implementation of ComputeGeoRange
func origComputeGeoRange(term uint64, shift uint,
	sminLon, sminLat, smaxLon, smaxLat float64,
	checkBoundaries bool) (
	onBoundary [][]byte, notOnBoundary [][]byte,
) {
	split := term | uint64(0x1)<<shift
	var upperMax uint64
	if shift < 63 {
		upperMax = term | ((uint64(1) << (shift + 1)) - 1)
	} else {
		upperMax = 0xffffffffffffffff
	}
	lowerMax := split - 1
	onBoundary, notOnBoundary = origRelateAndRecurse(term, lowerMax, shift,
		sminLon, sminLat, smaxLon, smaxLat, checkBoundaries)
	plusOnBoundary, plusNotOnBoundary := origRelateAndRecurse(split, upperMax, shift,
		sminLon, sminLat, smaxLon, smaxLat, checkBoundaries)
	onBoundary = append(onBoundary, plusOnBoundary...)
	notOnBoundary = append(notOnBoundary, plusNotOnBoundary...)
	return
}

// original, non-optimized implementation of relateAndRecurse
func origRelateAndRecurse(start, end uint64, res uint,
	sminLon, sminLat, smaxLon, smaxLat float64,
	checkBoundaries bool) (
	onBoundary [][]byte, notOnBoundary [][]byte,
) {
	minLon := geo.MortonUnhashLon(start)
	minLat := geo.MortonUnhashLat(start)
	maxLon := geo.MortonUnhashLon(end)
	maxLat := geo.MortonUnhashLat(end)

	level := ((geo.GeoBits << 1) - res) >> 1

	within := res%document.GeoPrecisionStep == 0 &&
		geo.RectWithin(minLon, minLat, maxLon, maxLat,
			sminLon, sminLat, smaxLon, smaxLat)
	if within || (level == geoDetailLevel &&
		geo.RectIntersects(minLon, minLat, maxLon, maxLat,
			sminLon, sminLat, smaxLon, smaxLat)) {
		if !within && checkBoundaries {
			return [][]byte{
				numeric.MustNewPrefixCodedInt64(int64(start), res),
			}, nil
		}
		return nil,
			[][]byte{
				numeric.MustNewPrefixCodedInt64(int64(start), res),
			}
	} else if level < geoDetailLevel &&
		geo.RectIntersects(minLon, minLat, maxLon, maxLat,
			sminLon, sminLat, smaxLon, smaxLat) {
		return origComputeGeoRange(start, res-1, sminLon, sminLat, smaxLon, smaxLat,
			checkBoundaries)
	}
	return nil, nil
}
