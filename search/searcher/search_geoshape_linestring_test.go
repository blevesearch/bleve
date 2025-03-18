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

package searcher

import (
	"context"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/index/upsidedown/store/gtreap"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

func TestGeoJsonLinestringIntersectsQuery(t *testing.T) {
	tests := []struct {
		line  [][]float64
		field string
		want  []string
	}{
		// test intersecting linestring query for polygon1.
		{
			[][]float64{
				{74.85860824584961, 22.407219759334023},
				{74.8663330078125, 22.382936446589863},
			},
			"geometry",
			[]string{"polygon1"},
		},

		// test intersecting linestring query for polygon1 and polygon2.
		{
			[][]float64{
				{74.82461929321289, 22.393729553598526},
				{74.93671417236328, 22.356743809494784},
			},
			"geometry",
			[]string{"polygon1", "polygon2"},
		},

		// test intersecting linestring query for envelope1.
		{
			[][]float64{
				{74.83938217163086, 22.325782524687973},
				{74.8692512512207, 22.311172762889516},
			},
			"geometry",
			[]string{"envelope1"},
		},

		// test intersecting linestring query for circle.
		{
			[][]float64{
				{74.94546890258789, 22.310815439776572},
				{74.93276596069336, 22.303708490145645},
			},
			"geometry",
			[]string{"circle1"},
		},

		// test intersecting linestring query for linestring1.
		{
			[][]float64{
				{74.938645362854, 22.321614134448936},
				{74.94070529937744, 22.320224643365446},
			},
			"geometry",
			[]string{"linestring1"},
		},

		// test intersecting linestring query for multilinestring1.
		{
			[][]float64{
				{74.9241828918457, 22.307996525380194},
				{74.94100570678711, 22.293781977618558},
			},
			"geometry",
			[]string{"multilinestring1"},
		},

		// test intersecting linestring query for multipolygon1.
		{
			[][]float64{
				{36.22072219848633, 50.007132228568786},
				{36.22218132019043, 49.99791917183082},
			},
			"geometry",
			[]string{"multipolygon1"},
		},

		// test intersecting linestring query for envelope2, circle2,
		// multipolygon1 and gc_polygonInGc_multipolygonInGc.
		{
			[][]float64{
				{36.19840621948242, 50.03834418692451},
				{36.25720024108887, 50.02136210283289},
			},
			"geometry",
			[]string{"envelope2", "circle2", "multipolygon1", "gc_polygonInGc_multipolygonInGc"},
		},
	}
	i := setupGeoJsonShapesIndexForLinestringQuery(t)
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

	for n, test := range tests {
		got, err := runGeoShapeLinestringQueryWithRelation("intersects",
			indexReader, test.line, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.line)
		}
	}
}

func TestGeoJsonLinestringContainsQuery(t *testing.T) {
	tests := []struct {
		line  [][]float64
		field string
		want  []string
	}{
		// test a linestring query for multipolygon1.
		{
			[][]float64{
				{36.21668815612793, 50.040494087443996},
				{36.226301193237305, 50.03861982057644},
			},
			"geometry",
			[]string{"multipolygon1"},
		},

		// test a linestring query with endspoints on two
		// different polygons in a multipolygon.
		{
			[][]float64{
				{36.19746208190918, 50.038564693972646},
				{36.21565818786621, 50.03718650830641},
			},
			"geometry", nil,
		},

		// test a linestring query for envelope2.
		{
			[][]float64{
				{36.25290870666503, 50.03018471417061},
				{36.23110771179199, 50.01854955486945},
			},
			"geometry",
			[]string{"envelope2"},
		},

		// test a linestring query for circle2.
		{
			[][]float64{
				{36.220550537109375, 50.02930252595981},
				{36.224327087402344, 50.02847545979485},
			},
			"geometry",
			[]string{"circle2"},
		},

		// test a linestring query for polygonWithHole2.
		{
			[][]float64{
				{36.27367973327637, 49.89883638369706},
				{36.27445220947265, 49.89596137883285},
			},
			"geometry",
			[]string{"polygonWithHole2"},
		},

		// test a linestring query within the hole of polygonWithHole2.
		{[][]float64{
			{36.261234283447266, 49.89540847364305},
			{36.26243591308594, 49.89087441212101},
		}, "geometry", nil},
	}
	i := setupGeoJsonShapesIndexForLinestringQuery(t)
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

	for n, test := range tests {
		got, err := runGeoShapeLinestringQueryWithRelation("contains",
			indexReader, test.line, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.line)
		}
	}
}

func TestGeoJsonMultiLinestringContainsQuery(t *testing.T) {
	tests := []struct {
		line  [][][]float64
		field string
		want  []string
	}{
		// test a multilinestring query for multipolygon1.
		{
			[][][]float64{
				{
					{36.21668815612793, 50.040494087443996},
					{36.226301193237305, 50.03861982057644},
				},
				{
					{36.226816177368164, 49.999463999158},
					{36.234025955200195, 50.00271900853649},
				},
			},
			"geometry",
			[]string{"multipolygon1"},
		},

		// test a multilinestring query that is covered by the geometryCollection.
		{
			[][][]float64{{
				{36.28664016723633, 49.96574238290487},
				{36.30251884460449, 49.96369956194569},
			}, {
				{36.19179725646973, 50.03983258984584},
				{36.19420051574707, 50.03801342445342},
			}},
			"geometry",
			[]string{"gc_polygonInGc_multipolygonInGc"},
		},

		// test a multilinestring query for envelope2.
		{
			[][][]float64{
				{
					{36.23213768005371, 50.02913711386621},
					{36.25187873840332, 50.02902683882067},
				},
				{
					{36.231794357299805, 50.018935600613254},
					{36.2314510345459, 50.025883893582055},
				},
			},
			"geometry",
			[]string{"envelope2"},
		},

		// test a multilinestring query with one linestring outside of envelope2.
		{
			[][][]float64{
				{
					{36.23213768005371, 50.02913711386621},
					{36.25187873840332, 50.02902683882067},
				},
				{{36.231794357299805, 50.018935600613254}, {36.2314510345459, 50.025883893582055}},
				{{36.25659942626953, 50.024284772330844}, {36.24406814575195, 50.01518531066489}},
			},
			"geometry", nil,
		},

		// test a multilinestring query with one linestring
		// inside the whole of a polygonWithHole2.
		{
			[][][]float64{
				{
					{36.27367973327637, 49.89883638369706},
					{36.27445220947265, 49.89596137883285},
				},
				{{36.261234283447266, 49.89540847364305}, {36.26243591308594, 49.89087441212101}},
			},
			"geometry", nil,
		},

		// test a multilinestring query for polygonWithHole2.
		{
			[][][]float64{
				{
					{36.27367973327637, 49.89883638369706},
					{36.27445220947265, 49.89596137883285},
				},
				{{36.279258728027344, 49.894302644257856}, {36.28166198730469, 49.887335336408235}},
			},
			"geometry",
			[]string{"polygonWithHole2"},
		},

		// test a multilinestring query for polygonWithHole2 with last line cross the hole.
		{
			[][][]float64{
				{
					{36.27367973327637, 49.89883638369706},
					{36.27445220947265, 49.89596137883285},
				},
				{{36.279258728027344, 49.894302644257856}, {36.28166198730469, 49.887335336408235}},
				{{36.254024505615234, 49.89839408640621}, {36.27016067504883, 49.90038439228633}},
			},
			"geometry", nil,
		},
	}
	i := setupGeoJsonShapesIndexForLinestringQuery(t)
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

	for n, test := range tests {
		got, err := runGeoShapeMultiLinestringQueryWithRelation("contains",
			indexReader, test.line, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.line)
		}
	}
}

func runGeoShapeMultiLinestringQueryWithRelation(relation string, i index.IndexReader,
	points [][][]float64, field string,
) ([]string, error) {
	s := geo.NewGeoJsonMultilinestring(points)
	return executeSearch(relation, i, s, field)
}

func runGeoShapeLinestringQueryWithRelation(relation string, i index.IndexReader,
	points [][]float64, field string,
) ([]string, error) {
	s := geo.NewGeoJsonLinestring(points)
	return executeSearch(relation, i, s, field)
}

func executeSearch(relation string, i index.IndexReader,
	s index.GeoJSON, field string,
) ([]string, error) {
	var rv []string
	gbs, err := NewGeoShapeSearcher(context.TODO(), i, s, relation, field, 1.0, search.SearcherOptions{})
	if err != nil {
		return nil, err
	}
	ctx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(gbs.DocumentMatchPoolSize(), 0),
	}
	docMatch, err := gbs.Next(ctx)
	for docMatch != nil && err == nil {
		docID, _ := i.ExternalID(docMatch.IndexInternalID)
		rv = append(rv, docID)
		docMatch, err = gbs.Next(ctx)
	}
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func setupGeoJsonShapesIndexForLinestringQuery(t *testing.T) index.Index {
	analysisQueue := index.NewAnalysisQueue(1)
	i, err := scorch.NewScorch(
		gtreap.Name,
		map[string]interface{}{
			"path":          "",
			"spatialPlugin": "s2",
		},
		analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = i.Open()
	if err != nil {
		t.Fatal(err)
	}

	polygon1 := [][][][]float64{{{
		{74.84642028808594, 22.402776071459712},
		{74.83234405517578, 22.39039647758608},
		{74.86719131469727, 22.38801566009795},
		{74.85139846801758, 22.39103135536648},
		{74.86461639404297, 22.394840561182853},
		{74.8495101928711, 22.397697397065034},
		{74.86186981201172, 22.401982540816856},
		{74.84642028808594, 22.402776071459712},
	}}}
	doc := document.NewDocument("polygon1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry",
		[]uint64{}, polygon1, "polygon",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	polygon2 := [][][][]float64{{{
		{74.93431091308592, 22.376428433285266},
		{74.92898941040039, 22.39103135536648},
		{74.9241828918457, 22.37722210974017},
		{74.90821838378906, 22.37388863821397},
		{74.92504119873047, 22.369920115637292},
		{74.92864608764648, 22.355632497760894},
		{74.93207931518555, 22.370396344320053},
		{74.94855880737305, 22.3743648533201},
		{74.93431091308592, 22.376428433285266},
	}}}
	doc = document.NewDocument("polygon2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry",
		[]uint64{}, polygon2, "polygon",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	envelope1 := [][][][]float64{{{
		{74.86736297607422, 22.307361269208684},
		{74.87028121948242, 22.345471522338478},
	}}}
	doc = document.NewDocument("envelope1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry",
		[]uint64{}, envelope1, "envelope",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	envelope2 := [][][][]float64{{{
		{36.23007774353027, 50.01810835593541},
		{36.25333786010742, 50.03068093791795},
	}}}
	doc = document.NewDocument("envelope2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry",
		[]uint64{}, envelope2, "envelope",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	doc = document.NewDocument("circle1")
	doc.AddField(document.NewGeoCircleFieldWithIndexingOptions("geometry",
		[]uint64{}, []float64{74.93671417236328, 22.308314152382284}, "300m",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	doc = document.NewDocument("circle2")
	doc.AddField(document.NewGeoCircleFieldWithIndexingOptions("geometry",
		[]uint64{}, []float64{36.22243881225586, 50.02941280037234}, "600m",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	linestring := [][][][]float64{{{
		{74.92697238922119, 22.320343743143248},
		{74.94036197662354, 22.32054224254707},
	}}}
	doc = document.NewDocument("linestring1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry",
		[]uint64{}, linestring, "linestring",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	linestring1 := [][][][]float64{{{
		{77.60188579559325, 12.982604078764705},
		{77.60557651519775, 12.987329508048184},
	}}}
	doc = document.NewDocument("linestring2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry",
		[]uint64{}, linestring1, "linestring",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multilinestring := [][][][]float64{{
		{
			{74.92203712463379, 22.3113315728684},
			{74.92323875427246, 22.307798008137024},
		},
		{{74.92405414581299, 22.307559787072712}, {74.92735862731934, 22.310021385140573}},
		{{74.9223804473877, 22.311688894660474}, {74.92534160614014, 22.30930673210729}},
	}}
	doc = document.NewDocument("multilinestring1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry",
		[]uint64{}, multilinestring, "multilinestring",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multilinestring1 := [][][][]float64{{
		{
			{77.6015853881836, 12.990089451715061},
			{77.60476112365723, 12.987747683302153},
		},
		{{77.59875297546387, 12.988751301039581}, {77.59446144104004, 12.98197680263484}},
		{{77.60188579559325, 12.982604078764705}, {77.60557651519775, 12.987329508048184}},
	}}
	doc = document.NewDocument("multilinestring2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry",
		[]uint64{}, multilinestring1, "multilinestring",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multipoint1 := [][][][]float64{{{
		{77.56618022918701, 12.958180959662695},
		{77.56407737731932, 12.951614746607163},
		{77.56922721862793, 12.956173473406446},
	}}}
	doc = document.NewDocument("multipoint1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry",
		[]uint64{}, multipoint1, "multipoint",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	polygonWithHole1 := [][][][]float64{{
		{
			{77.59991168975829, 12.972232910164502},
			{77.6039457321167, 12.97582941279006},
			{77.60424613952637, 12.98168407323241},
			{77.59974002838135, 12.985489528568463},
			{77.59321689605713, 12.979300406693417},
			{77.59991168975829, 12.972232910164502},
		},
		{
			{77.59682178497314, 12.975787593290978},
			{77.60295867919922, 12.975787593290978},
			{77.60295867919922, 12.98143316204164},
			{77.59682178497314, 12.98143316204164},
			{77.59682178497314, 12.975787593290978},
		},
	}}

	doc = document.NewDocument("polygonWithHole1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry",
		[]uint64{}, polygonWithHole1, "polygon",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	polygonWithHole2 := [][][][]float64{{
		{
			{36.261234283447266, 49.90712870720605},
			{36.2479305267334, 49.89480027061714},
			{36.254539489746094, 49.883408870659736},
			{36.280717849731445, 49.883408870659736},
			{36.28741264343262, 49.890432041848264},
			{36.27788543701172, 49.90276159448742},
			{36.261234283447266, 49.90712870720605},
		},

		{
			{36.264581680297844, 49.905249238801304},
			{36.25368118286133, 49.89673543545543},
			{36.253509521484375, 49.88578690918283},
			{36.270332336425774, 49.886174020645804},
			{36.27127647399902, 49.89579550794111},
			{36.264581680297844, 49.905249238801304},
		},
	}}

	doc = document.NewDocument("polygonWithHole2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry",
		[]uint64{}, polygonWithHole2, "polygon",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multipolygon1 := [][][][]float64{{{
		{36.1875057220459, 50.04363607656457},
		{36.192398071289055, 50.034871067327856},
		{36.20218276977539, 50.03955696315653},
		{36.1875057220459, 50.04363607656457},
	}}, // polygon1
		{{
			{36.2123966217041, 50.03795829715335},
			{36.218318939208984, 50.0333273779768},
			{36.226558685302734, 50.03867494711694},
			{36.217031478881836, 50.04286437899031},
			{36.2123966217041, 50.03795829715335},
		}}, // polygon2
		{{
			{36.221065521240234, 50.00365685169585},
			{36.226301193237305, 49.998029518286025},
			{36.23342514038086, 49.9995743420677},
			{36.23531341552734, 50.002994846659156},
			{36.231021881103516, 50.00630478067617},
			{36.22810363769531, 50.00663576154257},
			{36.226043701171875, 50.004815338573046},
			{36.221065521240234, 50.00365685169585},
		}}}
	doc = document.NewDocument("multipolygon1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry",
		[]uint64{}, multipolygon1, "multipolygon",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	polygonInGc := [][][][]float64{{{
		{36.1875057220459, 50.04363607656457},
		{36.192398071289055, 50.034871067327856},
		{36.20218276977539, 50.03955696315653},
		{36.1875057220459, 50.04363607656457},
	}}}
	multipolygonInGc := [][][][]float64{{{
		{36.29015922546387, 49.980150089789376},
		{36.28337860107422, 49.961656654293485},
		{36.307411193847656, 49.96033147865059},
		{36.29015922546387, 49.980150089789376},
	}}, // polygon1
		{{
			{36.16106986999512, 50.00387751801547},
			{36.161842346191406, 49.9908012905034},
			{36.17900848388672, 49.99841572888488},
			{36.16106986999512, 50.00387751801547},
		}}}
	coordinates := [][][][][]float64{polygonInGc, multipolygonInGc}
	types := []string{"polygon", "multipolygon"}
	doc = document.NewDocument("gc_polygonInGc_multipolygonInGc")
	doc.AddField(document.NewGeometryCollectionFieldWithIndexingOptions("geometry",
		[]uint64{}, coordinates, types,
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	return i
}
