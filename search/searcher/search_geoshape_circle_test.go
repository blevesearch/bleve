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
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/index/upsidedown/store/gtreap"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

func TestGeoJsonCircleIntersectsQuery(t *testing.T) {
	tests := []struct {
		centrePoint    []float64
		radiusInMeters string
		field          string
		want           []string
	}{
		// test intersecting query circle for polygon1.
		{[]float64{77.68115043640137, 12.94663769274367}, "200m",
			"geometry", []string{"polygon1"}},

		// test intersecting query circle for polygon1, circle1 and linestring1.
		{[]float64{77.68115043640137, 12.94663769274367}, "750m",
			"geometry", []string{"polygon1", "circle1", "linestring1"}},

		// test intersecting query circle for linestring2.
		{[]float64{77.69591331481932, 12.92756503709986}, "250m",
			"geometry", []string{"linestring2"}},

		// test intersecting query circle for circle1.
		{[]float64{77.6767, 12.9422}, "250m", "geometry", []string{"circle1"}},

		// test intersecting query circle for point1, envelope1 and linestring3.
		{[]float64{81.243896484375, 26.22444694563432}, "90000m",
			"geometry", []string{"point1", "envelope1", "linestring3"}},

		// test intersecting query circle for envelope.
		{[]float64{79.98458862304688, 25.339061458818374}, "1250m",
			"geometry", []string{"envelope1"}},

		// test intersecting query circle for multipoint.
		{[]float64{81.87346458435059, 25.41505910223247}, "200m",
			"geometry", []string{"multipoint1"}},

		// test intersecting query circle for multilinestring.
		{[]float64{81.8669843673706, 25.512661276952272}, "90m",
			"geometry", []string{"multilinestring1"}},
	}

	i := setupGeoJsonShapesIndexForCircleQuery(t)
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
		got, err := runGeoShapeCircleRelationQuery("intersects",
			indexReader, test.centrePoint, test.radiusInMeters, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.centrePoint)
		}
	}
}

func TestGeoJsonCircleWithInQuery(t *testing.T) {
	tests := []struct {
		centrePoint    []float64
		radiusInMeters string
		field          string
		want           []string
	}{
		// test query circle containing polygon2 and multilinestring2.
		{[]float64{81.85981750488281, 25.546778150624146}, "3700m",
			"geometry", []string{"polygon2", "multilinestring2"}},

		// test query circle containing multilinestring2.
		{[]float64{81.85981750488281, 25.546778150624146}, "3250m",
			"geometry", []string{"multilinestring2"}},

		// test query circle containing multipoint1.
		{[]float64{81.88599586486816, 25.425756968727935}, "1650m",
			"geometry", []string{"multipoint1"}},

		// test query circle containing circle2.
		{[]float64{82.09362030029297, 25.546313513788725}, "1280m",
			"geometry", []string{"envelope2", "circle2"}},

		// test query circle containing envelope2 and circle2.
		{[]float64{82.10289001464844, 25.544919592476727}, "700m",
			"geometry", []string{"envelope2", "circle2"}},

		// test query circle containing point1 and linestring3.
		{[]float64{81.27685546875, 26.1899475672235}, "5600m",
			"geometry", []string{"point1", "linestring3"}},
	}

	i := setupGeoJsonShapesIndexForCircleQuery(t)
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
		got, err := runGeoShapeCircleRelationQuery("within",
			indexReader, test.centrePoint, test.radiusInMeters, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.centrePoint)
		}
	}
}

func TestGeoJsonCircleContainsQuery(t *testing.T) {
	tests := []struct {
		centrePoint    []float64
		radiusInMeters string
		field          string
		want           []string
	}{
		// test query circle within polygon3.
		{[]float64{8.549551963806152, 47.3759038562437}, "180m",
			"geometry", []string{"polygon3"}},

		// test query circle containing envelope3.
		{[]float64{8.551011085510254, 47.380117626829275}, "75m",
			"geometry", []string{"envelope3"}},

		// test query circle exceeding envelope3 with a few meters.
		{[]float64{8.551011085510254, 47.380117626829275}, "78m",
			"geometry", nil},

		// test query circle containing circle3.
		{[]float64{8.535819053649902, 47.38297989270074}, "185m",
			"geometry", []string{"circle3"}},
	}

	i := setupGeoJsonShapesIndexForCircleQuery(t)
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
		got, err := runGeoShapeCircleRelationQuery("contains",
			indexReader, test.centrePoint, test.radiusInMeters, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.centrePoint)
		}
	}
}

func runGeoShapeCircleRelationQuery(relation string, i index.IndexReader,
	points []float64, radius string, field string) ([]string, error) {
	var rv []string
	s := geo.NewGeoCircle(points, radius)

	gbs, err := NewGeoShapeSearcher(nil, i, s, relation, field, 1.0, search.SearcherOptions{})
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

func setupGeoJsonShapesIndexForCircleQuery(t *testing.T) index.Index {
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

	polygon1 := [][][][]float64{{{{77.67248153686523, 12.957679089615821},
		{77.67956256866455, 12.948101542434257}, {77.68908977508545, 12.948896200093982},
		{77.68934726715086, 12.955211547173878}, {77.68016338348389, 12.954291440344619},
		{77.67248153686523, 12.957679089615821}}}}
	doc := document.NewDocument("polygon1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon1, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	polygon2 := [][][][]float64{{{{81.84951782226561, 25.522692102524033},
		{81.8557834625244, 25.521762640415535}, {81.86264991760254, 25.521762640415535},
		{81.86676979064941, 25.521607729364224}, {81.89560890197754, 25.542673796271302},
		{81.88977241516113, 25.543293330460937}, {81.84951782226561, 25.522692102524033}}}}
	doc = document.NewDocument("polygon2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon2, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	polygon3 := [][][][]float64{{{{8.548071384429932, 47.379216780040124},
		{8.547642230987549, 47.3771680227784}, {8.545818328857422, 47.37677569847655},
		{8.546290397644043, 47.37417465983494}, {8.551719188690186, 47.37417465983494},
		{8.553242683410645, 47.37679022905829}, {8.548071384429932, 47.379216780040124}}}}
	doc = document.NewDocument("polygon3")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon3, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	point1 := [][][][]float64{{{{81.2439, 26.2244}}}}
	doc = document.NewDocument("point1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		point1, "point", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	envelope1 := [][][][]float64{{{{79.9969482421875, 23.895882703682627},
		{80.7220458984375, 25.750424835909385}}}}
	doc = document.NewDocument("envelope1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		envelope1, "envelope", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	envelope2 := [][][][]float64{{{{82.10409164428711, 25.54360309635522},
		{82.10537910461424, 25.544609829984058}}}}
	doc = document.NewDocument("envelope2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		envelope2, "envelope", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	envelope3 := [][][][]float64{{{{8.545668125152588, 47.37942019840244},
		{8.552148342132568, 47.383778974713124}}}}
	doc = document.NewDocument("envelope3")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		envelope3, "envelope", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	doc = document.NewDocument("circle1")
	doc.AddField(document.NewGeoCircleFieldWithIndexingOptions("geometry", []uint64{},
		[]float64{77.67252445220947, 12.936348678099293}, "900m",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	doc = document.NewDocument("circle2")
	doc.AddField(document.NewGeoCircleFieldWithIndexingOptions("geometry", []uint64{},
		[]float64{82.10289001464844, 25.544919592476727}, "100m",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	doc = document.NewDocument("circle3")
	doc.AddField(document.NewGeoCircleFieldWithIndexingOptions("geometry", []uint64{},
		[]float64{8.53363037109375,
			47.38191927423153}, "400m",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	linestring := [][][][]float64{{{{77.68715858459473, 12.944755587650944},
		{77.69213676452637, 12.945090185150542}}}}
	doc = document.NewDocument("linestring1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		linestring, "linestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	linestring1 := [][][][]float64{{{{77.68913269042969, 12.929614580987227},
		{77.70252227783203, 12.929698235482276}}}}
	doc = document.NewDocument("linestring2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		linestring1, "linestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	linestring2 := [][][][]float64{{{{81.26792907714844, 26.170845301716813},
		{81.30157470703125, 26.18440207077121}}}}
	doc = document.NewDocument("linestring3")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		linestring2, "linestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multilinestring := [][][][]float64{{{{81.86170578002928, 25.430407918899984},
		{81.86273574829102, 25.421958559611397}}, {{81.88230514526367, 25.437616536907512},
		{81.90084457397461, 25.431415601111418}}, {{81.86805725097656, 25.514868905100244},
		{81.86702728271484, 25.502474677473746}}}}
	doc = document.NewDocument("multilinestring1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multilinestring, "multilinestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multilinestring1 := [][][][]float64{{{{81.84642791748047, 25.561335859046192},
		{81.84230804443358, 25.550495180470026}},
		{{81.87423706054688, 25.55142441992021}, {81.88453674316406, 25.555141305670045}},
		{{81.8642807006836, 25.572175556682115}, {81.87458038330078, 25.567839795359724}}}}
	doc = document.NewDocument("multilinestring2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multilinestring1, "multilinestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multipoint1 := [][][][]float64{{{{81.87337875366211, 25.432268248708212},
		{81.87355041503906, 25.416299483230368}, {81.90118789672852, 25.426067037656946}}}}
	doc = document.NewDocument("multipoint1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multipoint1, "multipoint", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	polygonWithHole1 := [][][][]float64{{
		{{77.59991168975829, 12.972232910164502}, {77.6039457321167, 12.97582941279006},
			{77.60424613952637, 12.98168407323241}, {77.59974002838135, 12.985489528568463},
			{77.59321689605713, 12.979300406693417}, {77.59991168975829, 12.972232910164502}},
		{{77.59682178497314, 12.975787593290978}, {77.60295867919922, 12.975787593290978},
			{77.60295867919922, 12.98143316204164}, {77.59682178497314, 12.98143316204164},
			{77.59682178497314, 12.975787593290978}}}}

	doc = document.NewDocument("polygonWithHole1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygonWithHole1, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	return i
}
