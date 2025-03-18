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

func TestGeoJSONIntersectsQueryAgainstGeometryCollection(t *testing.T) {
	tests := []struct {
		points [][][][][]float64
		types  []string
		field  string
		want   []string
	}{
		// test intersects geometrycollection query for gc_polygon1_linestring1.
		{
			[][][][][]float64{
				{{{
					{-120.80017089843749, 36.54053616262899},
					{-120.67932128906249, 36.33725319397006},
					{-120.30578613281251, 36.90597988519294},
					{-120.80017089843749, 36.54053616262899},
				}}},
				{{{{-118.24584960937499, 35.32184842037683}, {-117.8668212890625, 35.06597313798418}}}},
			},
			[]string{"polygon", "linestring"},
			"geometry",
			[]string{"gc_polygon1_linestring1"},
		},

		// test intersects geometrycollection query for gc_polygon1_linestring1.
		{
			[][][][][]float64{
				{{
					{{-118.3172607421875, 35.250105158539355}, {-117.50976562499999, 35.37561413174875}},
					{{-118.69628906249999, 34.6241677899049}, {-118.3172607421875, 35.03899204678081}},
					{{-117.94921874999999, 35.146862906756304}, {-117.674560546875, 34.41144164327245}},
				}},
				{{{
					{-117.04284667968749, 35.263561862152095},
					{-116.8505859375, 35.263561862152095},
					{-116.8505859375, 35.33529320309328},
					{-117.04284667968749, 35.33529320309328},
					{-117.04284667968749, 35.263561862152095},
				}}},
			},
			[]string{"multilinestring", "polygon"},
			"geometry",
			[]string{"gc_polygon1_linestring1"},
		},

		// test intersects geometrycollection query for gc_multipolygon1_multilinestring1.
		{
			[][][][][]float64{
				{{
					{{-115.8563232421875, 38.53957267203905}, {-115.58166503906251, 38.54816542304656}},
					{{-115.8343505859375, 38.45789034424927}, {-115.81237792968749, 38.19502155795575}},
				}},
				{{{{-116.64905548095702, 37.94920616351679}}}},
			},
			[]string{"multilinestring", "point"},
			"geometry",
			[]string{"gc_multipolygon1_multilinestring1"},
		},

		// test intersects geometrycollection query for gc_polygon1_linestring1 and gc_multipolygon1_multilinestring1.
		{
			[][][][][]float64{
				{{{{-116.64905548095702, 37.94920616351679}, {-118.29528808593751, 34.52466147177172}}}},
				{{
					{{-115.8563232421875, 38.53957267203905}, {-115.58166503906251, 38.54816542304656}},
					{{-115.8343505859375, 38.45789034424927}, {-115.81237792968749, 38.19502155795575}},
				}},
			},
			[]string{"multipoint", "multilinestring"},
			"geometry",
			[]string{
				"gc_polygon1_linestring1",
				"gc_multipolygon1_multilinestring1",
			},
		},

		// test intersects geometrycollection query for gc_polygon1_linestring1 and gc_multipolygon1_multilinestring1.
		{
			[][][][][]float64{
				{{{
					{-117.46582031249999, 36.146746777814364},
					{-116.70227050781249, 36.146746777814364},
					{-116.70227050781249, 36.69485094156225},
					{-117.46582031249999, 36.69485094156225},
					{-117.46582031249999, 36.146746777814364},
				}}, {{
					{-115.5267333984375, 38.06106741381201},
					{-115.4937744140625, 37.18220222107978},
					{-114.93896484374999, 37.304644804751106},
					{-115.5267333984375, 38.06106741381201},
				}}},
				{{
					{{-115.8563232421875, 38.53957267203905}, {-115.58166503906251, 38.54816542304656}},
					{{-115.8343505859375, 38.45789034424927}, {-115.81237792968749, 38.19502155795575}},
				}},
			},
			[]string{"multipolygon", "multilinestring"},
			"geometry",
			[]string{"gc_point1_multipoint1"},
		},
	}
	i := setupGeoJsonShapesIndexForGeometryCollectionQuery(t)
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
		got, err := runGeoShapeGeometryCollectionRelationQuery("intersects",
			indexReader, test.points, test.types, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.points)
		}
	}
}

func TestGeoJSONWithInQueryAgainstGeometryCollection(t *testing.T) {
	tests := []struct {
		points [][][][][]float64
		types  []string
		field  string
		want   []string
	}{
		// test within geometrycollection query for gc_multipoint2_multipolygon2_multiline2.
		{
			[][][][][]float64{
				{{{{-122.40434646606444, 37.73400071182758}, {-122.39730834960938, 37.73691949864062}}}},
				{{{
					{-122.42511749267578, 37.760808496517235},
					{-122.42314338684082, 37.74248523826606},
					{-122.40082740783691, 37.756669194195815},
					{-122.42511749267578, 37.760808496517235},
				}}},
				{
					{{
						{-122.46339797973633, 37.76637243960179},
						{-122.46176719665527, 37.7502901437285},
						{-122.43644714355469, 37.75911208915015},
						{-122.46339797973633, 37.76637243960179},
					}},
					{{
						{-122.43653297424315, 37.714720253587004},
						{-122.40563392639159, 37.714720253587004},
						{-122.40563392639159, 37.72904529863455},
						{-122.43653297424315, 37.72904529863455},
						{-122.43653297424315, 37.714720253587004},
					}},
				},
			},
			[]string{"linestring", "polygon", "multipolygon"},
			"geometry",
			[]string{"gc_multipoint2_multipolygon2_multiline2"},
		},

		// test within geometrycollection query.
		{
			[][][][][]float64{
				{{{{-122.40434646606444, 37.73400071182758}, {-122.39730834960938, 37.73691949864062}}}},
				{
					{{
						{-122.46339797973633, 37.76637243960179},
						{-122.46176719665527, 37.7502901437285},
						{-122.43644714355469, 37.75911208915015},
						{-122.46339797973633, 37.76637243960179},
					}},
					{{
						{-122.43653297424315, 37.714720253587004},
						{-122.40563392639159, 37.714720253587004},
						{-122.40563392639159, 37.72904529863455},
						{-122.43653297424315, 37.72904529863455},
						{-122.43653297424315, 37.714720253587004},
					}},
				},
			},
			[]string{"linestring", "multipolygon"},
			"geometry", nil,
		},

		// test within geometrycollection for gc_multipoint2_multipolygon2_multiline2.
		{
			[][][][][]float64{
				{{{
					{-122.4491500854492, 37.78170504295941},
					{-122.4862289428711, 37.747371884118664},
					{-122.43078231811525, 37.6949593672454},
					{-122.3799705505371, 37.72945260537779},
					{-122.3928451538086, 37.78007695280165},
					{-122.4491500854492, 37.78170504295941},
				}}},
			},
			[]string{"polygon"},
			"geometry",
			[]string{"gc_multipoint2_multipolygon2_multiline2"},
		},

		// test within geometrycollection for gc_multipolygon3
		// gc_multipolygon3's multipolygons within the geometrycollection is covered by the
		// query's geometric collection of a polygon and a multipolygon.
		{
			[][][][][]float64{
				{{{
					{86.6162109375, 57.26716357153586},
					{85.1220703125, 8119},
					{84.462890625, 56.27996083172844},
					{86.98974609375, 55.70235509327093},
					{87.802734375, 56.77680831656842},
					{86.6162109375, 57.26716357153586},
				}}},
				{
					{{
						{75.1025390625, 54.3549556895541},
						{73.1689453125, 54.29088164657006},
						{72.7294921875, 53.08082737207479},
						{74.091796875, 51.998410382390325},
						{76.79443359375, 53.396432127095984},
						{75.1025390625, 54.3549556895541},
					}},
					{{
						{80.1123046875, 55.57834467218206},
						{78.9697265625, 55.65279803318956},
						{78.5302734375, 54.635697306063854},
						{79.87060546875, 54.18815548107151},
						{80.96923828125, 54.80068486732233},
						{80.1123046875, 55.57834467218206},
					}},
				},
			},
			[]string{"polygon", "multipolygon"},
			"geometry",
			[]string{"gc_multipolygon3"},
		},
	}

	i := setupGeoJsonShapesIndexForGeometryCollectionQuery(t)
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
		got, err := runGeoShapeGeometryCollectionRelationQuery("within", indexReader, test.points, test.types, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v", n, test.want, got, test.points)
		}
	}
}

func TestGeoJSONContainsQueryAgainstGeometryCollection(t *testing.T) {
	tests := []struct {
		points [][][][][]float64
		types  []string
		field  string
		want   []string
	}{
		// test contains for a geometrycollection that comprises of a linestring,
		// polygon, multipolygon, point and multipoint for polygon2.
		{
			[][][][][]float64{
				// linestring
				{{{{7.457013130187988, 46.966401589723894}, {7.482891082763671, 46.94554547022893}}}},
				// polygon
				{{{
					{7.466454505920409, 46.965054389418476},
					{7.46143341064453, 46.9641171865865},
					{7.466325759887694, 46.96101258493027},
					{7.466454505920409, 46.965054389418476},
				}}},
				// multipolygon
				{
					{{
						{7.4811744689941415, 46.957966385567474},
						{7.478899955749511, 46.95492001277476},
						{7.484478950500488, 46.95509576976545},
						{7.4811744689941415, 46.957966385567474},
					}},
					{{
						{7.466540336608888, 46.94753769790697},
						{7.464609146118165, 46.946219320241674},
						{7.468342781066894, 46.94592634301753},
						{7.466540336608888, 46.94753769790697},
					}},
					{{
						{7.504348754882812, 47.00425575323296},
						{7.501087188720703, 47.001680295206874},
						{7.507266998291015, 47.00191443288521},
						{7.504348754882812, 47.00425575323296},
					}},
				},
				// point
				{{{{7.449932098388673, 46.95817142366062}}}},
				// multipoint
				{{{{7.479157447814942, 46.96370715518446}, {7.4532365798950195, 46.96657730900153}}}},
			},
			[]string{"linestring", "polygon", "multipolygon", "point", "multipoint"},
			"geometry",
			[]string{"multipolygon4"},
		},

		// test contains for a geometrycollection query with one point inside the multipoint lying outside
		// polygon2.
		{
			[][][][][]float64{
				// linestring
				{{{{7.457013130187988, 46.966401589723894}, {7.482891082763671, 46.94554547022893}}}},
				// polygon
				{{{
					{7.466454505920409, 46.965054389418476},
					{7.46143341064453, 46.9641171865865},
					{7.466325759887694, 46.96101258493027},
					{7.466454505920409, 46.965054389418476},
				}}},
				// multipolygon
				{
					{{
						{7.4811744689941415, 46.957966385567474},
						{7.478899955749511, 46.95492001277476},
						{7.484478950500488, 46.95509576976545},
						{7.4811744689941415, 46.957966385567474},
					}},
					{{
						{7.466540336608888, 46.94753769790697},
						{7.464609146118165, 46.946219320241674},
						{7.468342781066894, 46.94592634301753},
						{7.466540336608888, 46.94753769790697},
					}},
				},
				// point
				{{{{7.449932098388673, 46.95817142366062}}}},
				// multipoint
				{{{{7.479157447814942, 46.96370715518446}, {7.475638389587402, 46.965200825877794}}}},
			},
			[]string{"linestring", "polygon", "multipolygon", "point", "multipoint"},
			"geometry",
			nil,
		},

		// test contains for a geometrycollection query with one point inside the multipoint lying outside
		// polygon2.
		{
			[][][][][]float64{
				// linestring
				{{{{7.457013130187988, 46.966401589723894}, {7.482891082763671, 46.94554547022893}}}},
				// polygon
				{{{
					{7.466454505920409, 46.965054389418476},
					{7.46143341064453, 46.9641171865865},
					{7.466325759887694, 46.96101258493027},
					{7.466454505920409, 46.965054389418476},
				}}},
				// multipolygon
				{
					{{
						{7.4811744689941415, 46.957966385567474},
						{7.478899955749511, 46.95492001277476},
						{7.484478950500488, 46.95509576976545},
						{7.4811744689941415, 46.957966385567474},
					}},
					{{
						{7.466540336608888, 46.94753769790697},
						{7.464609146118165, 46.946219320241674},
						{7.468342781066894, 46.94592634301753},
						{7.466540336608888, 46.94753769790697},
					}},
				},
				// point
				{{{{7.449932098388673, 46.95817142366062}}}},
				// multipoint
				{{{{7.479157447814942, 46.96370715518446}, {7.4532365798950195, 46.96657730900153}}}},
			},
			[]string{"linestring", "polygon", "multipolygon", "point", "multipoint"},
			"geometry",
			[]string{"polygon2", "multipolygon4"},
		},
	}

	i := setupGeoJsonShapesIndexForGeometryCollectionQuery(t)
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
		got, err := runGeoShapeGeometryCollectionRelationQuery("contains",
			indexReader, test.points, test.types, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.points)
		}
	}
}

func runGeoShapeGeometryCollectionRelationQuery(relation string, i index.IndexReader,
	points [][][][][]float64, types []string, field string,
) ([]string, error) {
	var rv []string
	s, _, err := geo.NewGeometryCollection(points, types)
	if err != nil {
		return nil, err
	}

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

func setupGeoJsonShapesIndexForGeometryCollectionQuery(t *testing.T) index.Index {
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

	// document gc_polygon1_linestring1
	polygon1 := [][][][]float64{{{
		{-118.15246582031249, 34.876918445772084},
		{-118.46557617187499, 34.773203753940734},
		{-118.3172607421875, 34.50655662164561},
		{-117.91625976562499, 34.4793919710481},
		{-117.76245117187499, 34.76417891445512},
		{-118.15246582031249, 34.876918445772084},
	}}}

	linestring1 := [][][][]float64{{{
		{-120.78918457031251, 36.87522650673951},
		{-118.9215087890625, 34.95349314197422},
	}}}

	coordinates := [][][][][]float64{polygon1, linestring1}
	types := []string{"polygon", "linestring"}

	doc := document.NewDocument("gc_polygon1_linestring1")
	doc.AddField(document.NewGeometryCollectionFieldWithIndexingOptions("geometry",
		[]uint64{}, coordinates, types, document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	// document gc_multipolygon1_multilinestring1
	multipolygon1 := [][][][]float64{
		{{
			{-117.24609374999999, 37.67512527892127},
			{-117.61962890624999, 37.26530995561875},
			{-116.597900390625, 37.56199695314352},
			{-117.24609374999999, 37.67512527892127},
		}},
		{{
			{-117.60864257812501, 38.71123253895224},
			{-117.41638183593749, 38.36750215395045},
			{-117.66357421875, 37.93986540897977},
			{-116.6473388671875, 37.94852933714952},
			{-117.1307373046875, 38.363195134453846},
			{-116.75170898437501, 38.7283759182398},
			{-117.60864257812501, 38.71123253895224},
		}},
	}
	multilinestring1 := [][][][]float64{{
		{{-118.9215087890625, 38.74123075381228}, {-118.78967285156249, 38.43207668538207}},
		{{-118.57543945312501, 38.8225909761771}, {-118.45458984375, 38.522384090200845}},
		{{-118.94897460937499, 38.788345355085625}, {-118.61938476562499, 38.86965182408357}},
	}}

	coordinates = [][][][][]float64{multipolygon1, multilinestring1}
	types = []string{"multipolygon", "multilinestring"}
	doc = document.NewDocument("gc_multipolygon1_multilinestring1")
	doc.AddField(document.NewGeometryCollectionFieldWithIndexingOptions("geometry",
		[]uint64{}, coordinates, types, document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	// document gc_point1_multipoint1
	point1 := [][][][]float64{{{{-115.10925292968749, 36.20882309283712}}}}
	multipoint1 := [][][][]float64{{{
		{-117.13623046874999, 36.474306755095235},
		{-118.57543945312501, 36.518465989675875},
		{-118.58642578124999, 36.90597988519294},
		{-119.5477294921875, 37.85316995894978},
	}}}

	coordinates = [][][][][]float64{point1, multipoint1}
	types = []string{"point", "multipoint"}

	doc = document.NewDocument("gc_point1_multipoint1")
	doc.AddField(document.NewGeometryCollectionFieldWithIndexingOptions("geometry",
		[]uint64{}, coordinates, types, document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	// document gc_multipoint2_multipolygon2_multiline2
	multipoint2 := [][][][]float64{{{
		{-122.4052906036377, 37.75626203719391},
		{-122.42091178894044, 37.74757548736071},
	}}}
	multipolygon2 := [][][][]float64{
		{{
			{-122.46168136596681, 37.765151122096945},
			{-122.46168136596681, 37.754972691904946},
			{-122.45103836059569, 37.754972691904946},
			{-122.451810836792, 37.7624370109886},
			{-122.46168136596681, 37.765151122096945},
		}},
		{{
			{-122.41902351379395, 37.726194088705576},
			{-122.43533134460448, 37.71668926284967},
			{-122.40777969360353, 37.71634978222733},
			{-122.41902351379395, 37.726194088705576},
		}},
	}
	multilinestring2 := [][][][]float64{{
		{{-122.41284370422362, 37.73155698786267}, {-122.40700721740721, 37.73338978839743}},
		{{-122.40434646606444, 37.73400071182758}, {-122.39730834960938, 37.73691949864062}},
	}}

	coordinates = [][][][][]float64{multipoint2, multipolygon2, multilinestring2}
	types = []string{"multipoint", "multipolygon", "multiline"}

	doc = document.NewDocument("gc_multipoint2_multipolygon2_multiline2")
	doc.AddField(document.NewGeometryCollectionFieldWithIndexingOptions("geometry",
		[]uint64{}, coordinates, types, document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	// document gc_multipolygon3
	multipolygon3 := [][][][]float64{
		{{
			{85.60546875, 57.20771009775018},
			{86.396484375, 55.99838095535963},
			{87.03369140625, 56.71656572651468},
			{85.60546875, 57.20771009775018},
		}},
		{{
			{79.56298828125, 55.3915921070334},
			{79.60693359375, 54.43171285946844},
			{80.39794921875, 54.85131525968606},
			{79.56298828125, 55.3915921070334},
		}},
		{{
			{74.35546875, 54.13669645687002},
			{74.1796875, 52.802761415419674},
			{75.87158203125, 53.44880683542759},
			{74.35546875, 54.13669645687002},
		}},
	}

	coordinates = [][][][][]float64{multipolygon3}
	types = []string{"multipolygon"}

	doc = document.NewDocument("gc_multipolygon3")
	doc.AddField(document.NewGeometryCollectionFieldWithIndexingOptions("geometry",
		[]uint64{}, coordinates, types, document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	polygon2 := [][][][]float64{{{
		{7.452635765075683, 46.96692874582506},
		{7.449803352355956, 46.95817142366062},
		{7.4573564529418945, 46.95149263607834},
		{7.462162971496582, 46.945955640812095},
		{7.483148574829102, 46.945311085627445},
		{7.487225532531738, 46.957029058564686},
		{7.4793291091918945, 46.96388288331302},
		{7.464480400085448, 46.96903731827891},
		{7.452635765075683, 46.96692874582506},
	}}}

	doc = document.NewDocument("polygon2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon2, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multipolygon4 := [][][][]float64{
		{{
			{7.452635765075683, 46.96692874582506},
			{7.449803352355956, 46.95817142366062},
			{7.4573564529418945, 46.95149263607834},
			{7.462162971496582, 46.945955640812095},
			{7.483148574829102, 46.945311085627445},
			{7.487225532531738, 46.957029058564686},
			{7.4793291091918945, 46.96388288331302},
			{7.464480400085448, 46.96903731827891},
			{7.452635765075683, 46.96692874582506},
		}},
		{{
			{7.4478721618652335, 47.00015837528636},
			{7.5110435485839835, 47.00015837528636},
			{7.5110435485839835, 47.00683108710118},
			{7.4478721618652335, 47.00683108710118},
			{7.4478721618652335, 47.00015837528636},
		}},
	}

	doc = document.NewDocument("multipolygon4")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multipolygon4, "multipolygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	return i
}
