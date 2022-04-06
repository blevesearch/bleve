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

func TestGeoJsonPointWithInQuery(t *testing.T) {
	tests := []struct {
		point []float64
		field string
		want  []string
	}{
		// test points inside the polygon1.
		{[]float64{77.58334636688232, 12.948268838994263},
			"geometry", []string{"polygon1"}},

		// test points inside the circle1.
		{[]float64{77.58553504943848, 12.954040501528555},
			"geometry", []string{"circle1"}},

		// test points inside the polygon1 and the circle.
		{[]float64{77.59293794631958, 12.948896200093982},
			"geometry", []string{"polygon1", "circle1"}},

		// test points outside the polygon1 and the circle1.
		{[]float64{77.5614595413208, 12.953287683563568},
			"geometry", nil},

		// test point within the envelope1.
		{[]float64{81.28166198730469, 26.34203746601541},
			"geometry", []string{"envelope1"}},

		// test point on the linestring vertex.
		{[]float64{77.57776737213135, 12.952074805390097},
			"geometry", []string{"linestring1"}},

		// test point on the multilinestring vertex.
		{[]float64{77.5779390335083, 12.945006535817749},
			"geometry", []string{"multilinestring1"}},

		// test point on the multipoint vertex.
		{[]float64{77.56407737731932, 12.951614746607163},
			"geometry", []string{"multipoint1"}},

		// test point within the polygonWithHole1.
		{[]float64{77.60334491729736, 12.979844051951334},
			"geometry", []string{"polygonWithHole1"}},

		// test point within the hole of the polygonWithHole1.
		{[]float64{77.60244369506836, 12.976247607394027},
			"geometry", nil},
	}
	i := setupGeoJsonShapesIndex(t)
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
		got, err := runGeoShapePointRelationQuery("contains",
			false, indexReader, [][]float64{test.point}, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.point)
		}
	}
}

func TestGeoJsonMultiPointWithInQuery(t *testing.T) {
	tests := []struct {
		multipoint [][]float64
		field      string
		want       []string
	}{
		// test multipoint inside the polygon1.
		{[][]float64{{77.58334636688232, 12.948268838994263},
			{77.58467674255371, 12.944295515355652}},
			"geometry", []string{"polygon1"}},

		// test multipoint inside the circle1.
		{[][]float64{{77.58553504943848, 12.954040501528555},
			{77.58643627166747, 12.956089827794571}},
			"geometry", []string{"circle1"}},

		// test multipoint inside the envelope1.
		{[][]float64{{81.28166198730469, 26.34203746601541},
			{80.94314575195312, 26.346960121309415}},
			"geometry", []string{"envelope1"}},

		// test multipoint inside the polygon1 and the circle.
		{[][]float64{{77.59293794631958, 12.948896200093982},
			{77.58532047271729, 12.953789562459688}},
			"geometry", []string{"polygon1", "circle1"}},

		// test multipoint (only 1 point outside) outside.
		{[][]float64{{77.58334636688232, 12.948268838994263},
			{77.58643627166747, 12.956089827794571},
			{77.5615, 12.9533}}, "geometry", nil},

		// test multipoint on the linestring vertex.
		{[][]float64{{77.5841188430786, 12.957093573282744},
			{77.57776737213135, 12.952074805390097}},
			"geometry", []string{"linestring1"}},

		// test multipoint outside the linestring vertex.
		{[][]float64{{77.5841188430786, 12.957093573282744},
			{77.57776737213135, 12.952074805390097},
			{77.58334636688232, 12.948268838994263}},
			"geometry", nil},

		// test multipoint on the multilinestring vertex.
		{[][]float64{{77.5779390335083, 12.94471376293191},
			{77.57218837738037, 12.948268838994263}},
			"geometry", []string{"multilinestring1"}},

		// test multipoint outside the multilinestring vertex.
		{[][]float64{{77.5779390335083, 12.94471376293191},
			{77.57218837738037, 12.948268838994263},
			{77.58532047271729, 12.953789562459688}},
			"geometry", nil},

		// test multipoint with one inside the hole within the polygonWithHole1.
		{[][]float64{{77.60334491729736, 12.979844051951334},
			{77.60244369506836, 12.976247607394027}},
			"geometry", nil},

		// test multipoint with all inside the hole witin the polygonWithHole1.
		{[][]float64{{77.59656429290771, 12.981767710239714},
			{77.59888172149658, 12.979969508380469}},
			"geometry", nil},

		// test multipoint with all inside the polygonWithHole1.
		{[][]float64{{77.60334491729736, 12.979844051951334},
			{77.59656429290771, 12.981767710239714},
			{77.59802341461182, 12.9751602999608}},
			"geometry", []string{"polygonWithHole1"}},
	}
	i := setupGeoJsonShapesIndex(t)
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
		got, err := runGeoShapePointRelationQuery("contains",
			true, indexReader, test.multipoint, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.multipoint)
		}
	}
}

func TestGeoJsonMultiPointIntersectsQuery(t *testing.T) {
	tests := []struct {
		multipoint [][]float64
		field      string
		want       []string
	}{
		// test multipoint inside the polygon1.
		{[][]float64{{77.58334636688232, 12.948268838994263},
			{77.58467674255371, 12.944295515355652}},
			"geometry", []string{"polygon1"}},

		// test multipoint inside the circle1.
		{[][]float64{{77.58553504943848, 12.954040501528555},
			{77.58643627166747, 12.956089827794571}},
			"geometry", []string{"circle1"}},

		// test multipoint inside the envelope1. (1 point outside)
		{[][]float64{{81.28166198730469, 26.34203746601541},
			{80.94314575195312, 26.346960121309415},
			{81.12716674804688, 26.353728430338332}},
			"geometry", []string{"envelope1"}},

		// test multipoint inside the polygon1 and the circle.
		{[][]float64{{77.59293794631958, 12.948896200093982},
			{77.58532047271729, 12.953789562459688}},
			"geometry", []string{"polygon1", "circle1"}},

		// test multipoint (only 1 point outside) intersects.
		{[][]float64{{77.58334636688232, 12.948268838994263},
			{77.58643627166747, 12.956089827794571},
			{77.5615, 12.9533}}, "geometry",
			[]string{"polygon1", "circle1"}},

		// test multipoint on the linestring vertex.
		{[][]float64{{77.5841188430786, 12.957093573282744},
			{77.57776737213135, 12.952074805390097}},
			"geometry", []string{"linestring1"}},

		// test multipoint outside the linestring vertex.
		{[][]float64{{77.5841188430786, 12.957093573282744},
			{77.57776737213135, 12.952074805390097},
			{77.58334636688232, 12.948268838994263}},
			"geometry", []string{"polygon1", "linestring1"}},

		// test multipoint on the multilinestring vertex.
		{[][]float64{{77.5779390335083, 12.94471376293191},
			{77.57218837738037, 12.948268838994263}},
			"geometry", []string{"multilinestring1"}},

		// test multipoint outside the multilinestring vertex.
		{[][]float64{{77.5779390335083, 12.94471376293191},
			{77.57218837738037, 12.948268838994263},
			{77.58532047271729, 12.953789562459688}},
			"geometry", []string{"polygon1", "circle1", "multilinestring1"}},

		// test multipoint with one inside the hole within the polygonWithHole1.
		{[][]float64{{77.60334491729736, 12.979844051951334},
			{77.60244369506836, 12.976247607394027}},
			"geometry", []string{"polygonWithHole1"}},

		// test multipoint with all inside the hole witin the polygonWithHole1.
		{[][]float64{{77.60244369506836, 12.976247607394027},
			{77.59888172149658, 12.979969508380469}},
			"geometry", nil},

		// test multipoint with all inside the polygonWithHole1.
		{[][]float64{{77.60334491729736, 12.979844051951334},
			{77.59656429290771, 12.981767710239714},
			{77.59802341461182, 12.9751602999608}},
			"geometry", []string{"polygonWithHole1"}},
	}
	i := setupGeoJsonShapesIndex(t)
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
		got, err := runGeoShapePointRelationQuery("intersects",
			true, indexReader, test.multipoint, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.multipoint)
		}
	}
}

func runGeoShapePointRelationQuery(relation string, multi bool,
	i index.IndexReader, points [][]float64, field string) ([]string, error) {
	var rv []string
	var s index.GeoJSON
	if multi {
		s = geo.NewGeoJsonMultiPoint(points)
	} else {
		s = geo.NewGeoJsonPoint(points[0])
	}

	gbs, err := NewGeoShapeSearcher(i, s, relation, field, 1.0, search.SearcherOptions{})
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

type Fatalfable interface {
	Fatalf(format string, args ...interface{})
}

func setupGeoJsonShapesIndex(t *testing.T) index.Index {
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

	polygon1 := [][][][]float64{{{{77.5853419303894, 12.953977766785052},
		{77.58405447006226, 12.95393594361393}, {77.5819730758667, 12.9495026476557},
		{77.58068561553955, 12.94883346405509}, {77.58019208908081, 12.948331575175299},
		{77.57991313934326, 12.943814529775414}, {77.58497714996338, 12.94394000436408},
		{77.58517026901245, 12.9446301134728}, {77.58572816848755, 12.945508431393435},
		{77.58785247802734, 12.946365833997325}, {77.58967638015747, 12.946428570657417},
		{77.59070634841918, 12.947474179333993}, {77.59317398071289, 12.948875288082773},
		{77.59167194366454, 12.949962710338657}, {77.59077072143555, 12.950276388953625},
		{77.59098529815674, 12.951196510612728}, {77.58729457855225, 12.952472128200755},
		{77.5853419303894, 12.953977766785052}}}}
	doc := document.NewDocument("polygon1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon1, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	// not working envelope
	envelope1 := [][][][]float64{{{{80.93696594238281, 26.33957605983274},
		{81.28440856933594, 26.351267272877074}}}}
	doc = document.NewDocument("envelope1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		envelope1, "envelope", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	doc = document.NewDocument("circle1")
	doc.AddField(document.NewGeoCircleFieldWithIndexingOptions("geometry", []uint64{},
		[]float64{77.59137153625487, 12.952660333521468}, 900,
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	linestring := [][][][]float64{{{{77.5841188430786, 12.957093573282744},
		{77.57776737213135, 12.952074805390097}}}}
	doc = document.NewDocument("linestring1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		linestring, "linestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multilinestring := [][][][]float64{{{{77.57227420806883, 12.948687079902895},
		{77.57600784301758, 12.954165970968194}, {77.5779390335083, 12.94471376293191},
		{77.57218837738037, 12.948268838994263}, {77.57781028747559, 12.951740217268595},
		{77.5779390335083, 12.945006535817749}}}}
	doc = document.NewDocument("multilinestring1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multilinestring, "multilinestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multipoint1 := [][][][]float64{{{{77.56618022918701, 12.958180959662695},
		{77.56407737731932, 12.951614746607163}, {77.56922721862793, 12.956173473406446}}}}
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
