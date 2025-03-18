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

func TestGeoJsonEnvelopeWithInQuery(t *testing.T) {
	tests := []struct {
		points [][]float64
		field  string
		want   []string
	}{
		// test within query envelope for point1.
		{
			[][]float64{
				{76.256103515625, 16.76772739719064},
				{76.35772705078125, 16.872890378907783},
			},
			"geometry",
			[]string{"point1"},
		},

		// test within query envelope for multipoint1.
		{
			[][]float64{
				{81.046142578125, 17.156537255486093},
				{81.331787109375, 17.96305758238804},
			},
			"geometry",
			[]string{"multipoint1"},
		},

		// test within query envelope for partial points in a multipoint1.
		{
			[][]float64{
				{81.05987548828125, 17.16178591271515},
				{81.36199951171875, 17.861132899477624},
			},
			"geometry", nil,
		},

		// test within query envelope for polygon2 and point1.
		{
			[][]float64{
				{76.00341796875, 16.573022719182777},
				{76.717529296875, 17.006888277600524},
			},
			"geometry",
			[]string{"polygon2", "point1"},
		},

		// test within query envelope for linestring1.
		{
			[][]float64{
				{76.84112548828125, 16.86500518090961},
				{77.62115478515625, 17.531439701706244},
			},
			"geometry",
			[]string{"linestring1"},
		},

		// test within query envelope for multilinestring1.
		{
			[][]float64{
				{81.683349609375, 17.104042525557904},
				{81.99234008789062, 17.66495983051931},
			},
			"geometry",
			[]string{"multilinestring1"},
		},

		// test within query envelope that is intersecting multilinestring1.
		{
			[][]float64{
				{81.65725708007812, 17.2601707001208},
				{81.95114135742186, 17.66495983051931},
			},
			"geometry", nil,
		},

		// test within query envelope for envelope1 and circle1.
		{
			[][]float64{
				{74.75372314453125, 17.36636733709516},
				{75.509033203125, 18.038809662036805},
			},
			"geometry",
			[]string{"envelope1", "circle1"},
		},

		// test within query envelope for envelope1.
		{
			[][]float64{
				{74.783935546875, 17.38209494787749},
				{75.96221923828125, 17.727758609852284},
			},
			"geometry",
			[]string{"envelope1"},
		},
	}
	i := setupGeoJsonShapesIndexForEnvelopeQuery(t)
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
		got, err := runGeoShapeEnvelopeRelationQuery("within",
			indexReader, test.points, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.points)
		}
	}
}

func TestGeoJsonEnvelopeIntersectsQuery(t *testing.T) {
	tests := []struct {
		points [][]float64
		field  string
		want   []string
	}{
		// test intersecting query envelope for partial points in a multipoint1.
		{
			[][]float64{
				{81.00769042968749, 17.80622614478282},
				{81.199951171875, 17.983957957423037},
			},
			"geometry",
			[]string{"multipoint1"},
		},

		// test intersecting query envelope that is intersecting multilinestring1.
		{
			[][]float64{
				{81.65725708007812, 17.2601707001208},
				{81.95114135742186, 17.66495983051931},
			},
			"geometry",
			[]string{"multilinestring1"},
		},

		// test intersecting query envelope for linestring2.
		{
			[][]float64{
				{81.9854736328125, 18.27369419984127},
				{82.14752197265625, 18.633232565431218},
			},
			"geometry",
			[]string{"linestring2"},
		},

		// test intersecting query envelope for circle2.
		{
			[][]float64{
				{82.6336669921875, 17.82714499951342},
				{82.66387939453125, 17.861132899477624},
			},
			"geometry",
			[]string{"circle2"},
		},

		// test intersecting query envelope for polygon3.
		{
			[][]float64{
				{82.92343139648438, 17.739530934289657},
				{82.98797607421874, 17.79184300887134},
			},
			"geometry",
			[]string{"polygon3"},
		},
	}
	i := setupGeoJsonShapesIndexForEnvelopeQuery(t)
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
		got, err := runGeoShapeEnvelopeRelationQuery("intersects", indexReader, test.points, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v", n, test.want, got, test.points)
		}
	}
}

func TestGeoJsonEnvelopeContainsQuery(t *testing.T) {
	tests := []struct {
		points [][]float64
		field  string
		want   []string
	}{
		// test envelope contained within polygon1.
		{
			[][]float64{
				{8.548285961151123, 47.376092756617446},
				{8.551225662231445, 47.37764752629426},
			},
			"geometry",
			[]string{"polygon1"},
		},

		// test envelope partially contained within polygon1.
		{
			[][]float64{
				{8.549273014068604, 47.376194471922986},
				{8.551654815673828, 47.37827232736301},
			},
			"geometry", nil,
		},

		// test envelope partially contained within polygon1.
		{
			[][]float64{
				{8.549273014068604, 47.376194471922986},
				{8.551654815673828, 47.37827232736301},
			},
			"geometry", nil,
		},

		// test envelope fully contained within circle3.
		{
			[][]float64{
				{8.532772064208984, 47.380379160110856},
				{8.534531593322752, 47.38299442157271},
			},
			"geometry",
			[]string{"circle3"},
		},

		// test envelope partially contained within circle3.
		{
			[][]float64{
				{8.532836437225342, 47.38010309716447},
				{8.538415431976318, 47.383081594720466},
			},
			"geometry", nil,
		},
	}
	i := setupGeoJsonShapesIndexForEnvelopeQuery(t)
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
		got, err := runGeoShapeEnvelopeRelationQuery("contains",
			indexReader, test.points, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.points)
		}
	}
}

func runGeoShapeEnvelopeRelationQuery(relation string, i index.IndexReader,
	points [][]float64, field string,
) ([]string, error) {
	var rv []string
	s := geo.NewGeoEnvelope(points)

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

func setupGeoJsonShapesIndexForEnvelopeQuery(t *testing.T) index.Index {
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
		{8.548071384429932, 47.379216780040124},
		{8.547642230987549, 47.3771680227784},
		{8.545818328857422, 47.37677569847655},
		{8.546290397644043, 47.37417465983494},
		{8.551719188690186, 47.37417465983494},
		{8.553242683410645, 47.37679022905829},
		{8.548071384429932, 47.379216780040124},
	}}}
	doc := document.NewDocument("polygon1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon1, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	polygon2 := [][][][]float64{{{
		{76.70379638671874, 16.828203242420393},
		{76.36322021484375, 16.58881695544584},
		{76.70928955078125, 16.720385051694},
		{76.70379638671874, 16.828203242420393},
	}}}
	doc = document.NewDocument("polygon2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon2, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	polygon3 := [][][][]float64{{{
		{82.9522705078125, 17.749994573141873},
		{82.94952392578125, 17.692436998627272},
		{82.87673950195312, 17.64009591883757},
		{82.76412963867188, 17.58643052828743},
		{82.8094482421875, 17.522272941245202},
		{82.99621582031249, 17.64009591883757},
		{82.9522705078125, 17.749994573141873},
	}}}
	doc = document.NewDocument("polygon3")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon3, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	envelope1 := [][][][]float64{{{
		{74.89654541015625, 17.403062993328923},
		{74.92401123046875, 17.66495983051931},
	}}}
	doc = document.NewDocument("envelope1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		envelope1, "envelope", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	doc = document.NewDocument("circle1")
	doc.AddField(document.NewGeoCircleFieldWithIndexingOptions("geometry", []uint64{},
		[]float64{75.0531005859375, 17.675427818339383}, "12900m",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	doc = document.NewDocument("circle2")
	doc.AddField(document.NewGeoCircleFieldWithIndexingOptions("geometry", []uint64{},
		[]float64{82.69683837890625, 17.902955242676995}, "6000m",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	doc = document.NewDocument("circle3")
	doc.AddField(document.NewGeoCircleFieldWithIndexingOptions("geometry", []uint64{},
		[]float64{
			8.53363037109375,
			47.38191927423153,
		}, "400m",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	point1 := [][][][]float64{{{{76.29730224609375, 16.796653031618053}}}}
	doc = document.NewDocument("point1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		point1, "point",
		document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	linestring1 := [][][][]float64{{{
		{76.85211181640624, 17.51048642597462},
		{77.24212646484374, 16.93070509876554},
	}}}
	doc = document.NewDocument("linestring1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		linestring1, "linestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	linestring2 := [][][][]float64{{{
		{81.89208984375, 18.555136195095105},
		{82.21343994140625, 18.059701055000478},
	}}}
	doc = document.NewDocument("linestring2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		linestring2, "linestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multipoint1 := [][][][]float64{{{
		{81.24938964843749, 17.602139123350838},
		{81.30432128906249, 17.56548361143177},
		{81.29058837890625, 17.180155043474496},
		{81.09283447265625, 17.87681743233167},
	}}}
	doc = document.NewDocument("multipoint1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multipoint1, "multipoint", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multilinestring := [][][][]float64{{
		{
			{81.69708251953125, 17.641404631355755},
			{81.90994262695312, 17.642713334367667},
		},
		{{81.6998291015625, 17.620464090732245}, {81.69708251953125, 17.468572623463153}},
		{{81.70120239257811, 17.458092664041494}, {81.81243896484375, 17.311310073048123}},
		{{81.815185546875, 17.3034434020238}, {81.81243896484375, 17.109292665395643}},
	}}
	doc = document.NewDocument("multilinestring1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multilinestring, "multilinestring", document.DefaultGeoShapeIndexingOptions))
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
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multilinestring1, "multilinestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	return i
}
