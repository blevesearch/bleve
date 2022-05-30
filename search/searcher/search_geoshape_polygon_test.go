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

func TestGeoJsonPolygonIntersectsQuery(t *testing.T) {
	tests := []struct {
		polygon [][][]float64
		field   string
		want    []string
	}{
		// test intersecting query polygon for polygon1.
		{[][][]float64{{{77.57926940917969, 12.945257483731918},
			{77.57875442504883, 12.942036966318216}, {77.58278846740721, 12.9424970427816},
			{77.57926940917969, 12.945257483731918}}}, "geometry", []string{"polygon1"}},

		// test intersecting query polygon for polygon1, polygon2, circle1.
		{[][][]float64{{{77.59562015533446, 12.94099133483504},
			{77.59665012359619, 12.949356263896634}, {77.59313106536865, 12.951321981484776},
			{77.59085655212402, 12.948477959536318}, {77.59562015533446, 12.94099133483504}}},
			"geometry", []string{"polygon1", "polygon2", "circle1"}},

		// test intersecting query polygon for polygon1, polygon2 and polygon3.
		{[][][]float64{{{77.5929594039917, 12.939151012774925},
			{77.58321762084961, 12.94546660680072}, {77.59737968444824, 12.931998723107322},
			{77.60111331939697, 12.955169724209911}, {77.59592056274414, 12.936265025833965},
			{77.5929594039917, 12.939151012774925},
		}}, "geometry", []string{"polygon1", "polygon2", "polygon3"}},

		// test intersecting query polygon for polygon2 and the circle1.
		{[][][]float64{{{77.59012699127197, 12.959853852513307},
			{77.59836673736572, 12.959853852513307}, {77.59836673736572, 12.965541604118611},
			{77.59012699127197, 12.965541604118611}, {77.59012699127197, 12.959853852513307}}},
			"geometry", []string{"polygon2", "circle1"}},

		// test intersecting query polygon for linestring2 and multilinestring2.
		{[][][]float64{{{77.59669303894043, 12.989504011681609},
			{77.60699272155762, 12.983231353311314}, {77.60115623474121, 12.993183897537897},
			{77.59669303894043, 12.989504011681609}}},
			"geometry", []string{"linestring2", "multilinestring2"}},

		// test intersecting query polygon for multilinestring2.
		{[][][]float64{{{77.60124206542969, 12.987162237749484},
			{77.60330200195312, 12.992849364713313}, {77.59514808654785, 12.989671280403403},
			{77.60124206542969, 12.987162237749484}}},
			"geometry", []string{"multilinestring2"}},

		// test intersecting query polygon for multipoint1.
		{[][][]float64{{{77.56648063659668, 12.956382587313202},
			{77.56819725036621, 12.949523559614263}, {77.5718879699707, 12.958222782120954},
			{77.56648063659668, 12.956382587313202}}},
			"geometry", []string{"multipoint1"}},

		// test intersecting query polygon for envelope1.
		{[][][]float64{{{36.19986534118652, 50.00034673534484},
			{36.19351387023926, 50.00464984215712},
			{36.178321838378906, 49.991573824716205},
			{36.19986534118652, 50.00034673534484}}}, "geometry", []string{"envelope1"}},

		// test intersecting query polygon for envelope1.
		{[][][]float64{{{36.170082092285156, 49.99229116680205},
			{36.14982604980469, 49.99002874388075}, {36.227073669433594, 49.98754547425633},
			{36.170082092285156, 49.99229116680205}}}, "geometry", []string{"envelope1"}},
	}
	i := setupGeoJsonShapesIndexForPolygonQuery(t)
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
		got, err := runGeoShapePolygonQueryWithRelation("intersects",
			indexReader, test.polygon, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v", n,
				test.want, got, test.polygon)
		}
	}
}

func TestGeoJsonPolygonContainsQuery(t *testing.T) {
	tests := []struct {
		polygon [][][]float64
		field   string
		want    []string
	}{

		// test containment query polygon for polygon1.
		{[][][]float64{{{77.5843334197998, 12.952702156906767},
			{77.58510589599608, 12.952702156906767}, {77.58510589599608, 12.953622269606669},
			{77.5843334197998, 12.953622269606669}, {77.5843334197998, 12.952702156906767}}},
			"geometry", []string{"polygon1"}},

		// test containment query polygon for circle1.
		{[][][]float64{{{77.59025573730469, 12.953810474058429},
			{77.59145736694336, 12.953810474058429}, {77.59145736694336, 12.954918786278716},
			{77.59025573730469, 12.954918786278716}, {77.59025573730469, 12.953810474058429}}},
			"geometry", []string{"circle1"}},

		// test containment query polygon for polygon2, polygon3.
		{[][][]float64{{{77.60235786437988, 12.956884459972992},
			{77.60124206542969, 12.956800814599926}, {77.6008129119873, 12.955713422193524},
			{77.60244369506836, 12.955211547173878}, {77.60313034057617, 12.955880713641998},
			{77.60235786437988, 12.956884459972992}}},
			"geometry", []string{"polygon2", "polygon3"}},

		// test containment query polygon which resides within a hole in polygonWithHole1.
		{[][][]float64{{{77.60012626647949, 12.97963495776207},
			{77.5978946685791, 12.978213112610835}, {77.60089874267577, 12.977962197916442},
			{77.60012626647949, 12.97963495776207}}},
			"geometry", nil},

		// test containment query polygon which resides within polygonWithHole1.
		{[][][]float64{{{77.59978294372559, 12.984067716910454},
			{77.59780883789062, 12.982227713276774}, {77.60089874267577, 12.982227713276774},
			{77.59978294372559, 12.984067716910454}}},
			"geometry", []string{"polygonWithHole1"}},

		// test with query polygon for polygon4 with a single vertex lying outside.
		{[][][]float64{{{-121.48138761520384, 38.50964107572585},
			{-121.48226737976073, 38.509238097766875}, {-121.48115158081055, 38.50781086602439},
			{-121.48014307022095, 38.50806273250507}, {-121.48138761520384, 38.50964107572585}}},
			"geometry", nil},

		// test with query polygon for polygon4.
		{[][][]float64{{{-121.48381233215332, 38.507974579337045},
			{-121.48361384868622, 38.507869634948676}, {-121.48361921310425, 38.50765135013098},
			{-121.48343682289122, 38.50797038156446}, {-121.48381233215332, 38.507974579337045}}},
			"geometry", []string{"polygon4"}},

		// test with query polygon for multipolygon1.
		{[][][]float64{{{-121.47578716278075, 38.51617236229197},
			{-121.47578716278075, 38.51566868518406}, {-121.47546529769896, 38.516105205547866},
			{-121.47578716278075, 38.51617236229197}}}, "geometry", []string{"multipolygon1"}},

		// test with query polygon for envelope1.
		{[][][]float64{{{36.197547912597656, 49.99642946989866},
			{36.18939399719238, 49.988649165474}, {36.20201110839844, 49.98853879749191},
			{36.1970329284668, 49.980150089789376}, {36.205787658691406,
				49.9885939815146}, {36.197547912597656, 49.99642946989866}}},
			"geometry", []string{"envelope1"}},

		// test with query polygon for no hits. (envelope1 has one vertex outside the polygon)
		{[][][]float64{{{36.19832038879394, 49.99626394461266},
			{36.19016647338867, 49.98439981533724}, {36.20698928833008, 49.98158510403259},
			{36.19832038879394, 49.99626394461266}}}, "geometry", nil},
	}
	i := setupGeoJsonShapesIndexForPolygonQuery(t)
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
		got, err := runGeoShapePolygonQueryWithRelation("contains",
			indexReader, test.polygon, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.polygon)
		}
	}
}

func TestGeoJsonPolygonWithInQuery(t *testing.T) {
	tests := []struct {
		polygon [][][]float64
		field   string
		want    []string
	}{

		// test with query polygon for polygon1.
		{[][][]float64{{{77.58407592773438, 12.956382587313202},
			{77.57746696472168, 12.943249893344905}, {77.5920581817627, 12.944086391304364},
			{77.59454727172852, 12.95353862313803}, {77.58407592773438, 12.956382587313202}}},
			"geometry", []string{"polygon1"}},

		// test with query polygon for circle1 and polygon3.
		{[][][]float64{{{77.59248733520508, 12.967841760870071},
			{77.58261680603027, 12.968594534825176}, {77.57789611816406, 12.957302686416881},
			{77.58896827697754, 12.945341132980488}, {77.60450363159178, 12.947599652080394},
			{77.60673522949219, 12.96483064227584}, {77.59248733520508, 12.967841760870071}}},
			"geometry", []string{"polygon3", "circle1"}},

		// test with query polygon for linestring2, multilinestring2.
		{[][][]float64{{{77.59909629821777, 12.998118204343788},
			{77.58931159973145, 12.978882217224443}, {77.61128425598145, 12.983565899088745},
			{77.59909629821777, 12.998118204343788}}}, "geometry",
			[]string{"linestring2", "multilinestring2"}},

		// test with query polygon for multipoint1.
		{[][][]float64{{{77.55703926086426, 12.964245142762644},
			{77.5631332397461, 12.944253690559432}, {77.57429122924805, 12.957720912158363},
			{77.55703926086426, 12.964245142762644}}}, "geometry", []string{"multipoint1"}},

		// test with query polygon with no results.
		// (polygon4 has one vertex lying outside the query polygon).
		{[][][]float64{{{-121.48812532424927, 38.51058134885975},
			{-121.48258924484252, 38.500153704565065}, {-121.47492885589598, 38.50799556819636},
			{-121.48630142211913, 38.51147123890908}, {-121.48812532424927, 38.51058134885975}}},
			"geometry", nil},

		// test with query polygon for polygon4.
		{[][][]float64{{{-121.48366212844849, 38.510161585585045},
			{-121.48533582687377, 38.50841534409804}, {-121.48376941680908, 38.507777283760426},
			{-121.48370504379272, 38.50250467407243}, {-121.48010015487672, 38.50253825879518},
			{-121.48018598556519, 38.504502937819765}, {-121.47756814956665, 38.50755899866278},
			{-121.48113012313843, 38.50866720846446}, {-121.48115158081055, 38.51017837616302},
			{-121.48366212844849, 38.510161585585045}}}, "geometry", []string{"polygon4"}},

		// test with query polygon for envelope1.
		{[][][]float64{{{36.20587348937988, 50.00470500769241},
			{36.17969512939453, 49.993946530777606}, {36.19368553161621, 49.971870325635074},
			{36.21119499206543, 49.983075265826656}, {36.20587348937988, 50.00470500769241}}},
			"geometry", []string{"envelope1"}},

		// test with query polygon for linestring2 which lies outside except the endpoints.
		{[][][]float64{{{8.515305519104004, 47.392597129887},
			{8.514232635498047, 47.38896544894171}, {8.507537841796875, 47.38815191810328},
			{8.514318466186523, 47.38725120859953}, {8.516035079956053, 47.383357642070706},
			{8.516979217529295, 47.38733837470806}, {8.522472381591797, 47.38794853343167},
			{8.516507148742676, 47.388994503382285}, {8.515305519104004, 47.392597129887}}},
			"geometry", nil},

		// test with query polygon for all the shapes.
		{[][][]float64{{{-135.0, -38.0},
			{149.0, -38.0}, {149.0, 77.0}, {-135.0, 77.0}}},
			"geometry", []string{"polygon1", "polygon2", "polygon3", "envelope1", "circle1", "linestring1",
				"linestring2", "linestring3", "multilinestring1", "multilinestring2", "multipoint1",
				"polygonWithHole1", "polygon4", "multipolygon1"}},
	}

	i := setupGeoJsonShapesIndexForPolygonQuery(t)
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
		got, err := runGeoShapePolygonQueryWithRelation("within",
			indexReader, test.polygon, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.polygon)
		}
	}
}

func runGeoShapePolygonQueryWithRelation(relation string, i index.IndexReader,
	points [][][]float64, field string) ([]string, error) {
	var rv []string
	s := geo.NewGeoJsonPolygon(points)

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

func setupGeoJsonShapesIndexForPolygonQuery(t *testing.T) index.Index {
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

	polygon2 := [][][][]float64{{{{77.59527683258057, 12.951112863329588},
		{77.59420394897461, 12.947976069940545}, {77.59579181671143, 12.946010325958518},
		{77.60347366333008, 12.950401860289055}, {77.60673522949219, 12.95600618215462},
		{77.60107040405273, 12.96345053407734}, {77.5984525680542, 12.961861309096507},
		{77.59527683258057, 12.951112863329588}}}}
	doc = document.NewDocument("polygon2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon2, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	polygon3 := [][][][]float64{{{{77.59974002838135, 12.953789562459688},
		{77.60347366333008, 12.953789562459688}, {77.60347366333008, 12.957720912158363},
		{77.59974002838135, 12.957720912158363}, {77.59974002838135, 12.953789562459688}}}}
	doc = document.NewDocument("polygon3")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon3, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	/*polygon4 := [][][][]float64{{{{8.515305519104004, 47.392597129887},
		{8.514232635498047, 47.38896544894171}, {8.507537841796875, 47.38815191810328},
		{8.514318466186523, 47.38725120859953}, {8.516035079956053, 47.383357642070706},
		{8.516979217529295, 47.38733837470806}, {8.522472381591797, 47.38794853343167},
		{8.516507148742676, 47.388994503382285}, {8.515305519104004, 47.392597129887}}}}
	doc = document.NewDocument("polygon4")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon4, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}*/

	// not working envelope
	envelope1 := [][][][]float64{{{{36.18896484375, 49.9799293145682},
		{36.20613098144531, 49.99714673955337}}}}
	doc = document.NewDocument("envelope1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		envelope1, "envelope", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	doc = document.NewDocument("circle1")
	doc.AddField(document.NewGeoCircleFieldWithIndexingOptions("geometry",
		[]uint64{}, []float64{77.59253025054932, 12.955587953533424}, 900,
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

	linestring1 := [][][][]float64{{{{77.60188579559325, 12.982604078764705},
		{77.60557651519775, 12.987329508048184}}}}
	doc = document.NewDocument("linestring2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		linestring1, "linestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	linestring3 := [][][][]float64{{{{8.51539134979248, 47.390592472948434},
		{8.520884513854979, 47.388006643417924}}}}
	doc = document.NewDocument("linestring3")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		linestring3, "linestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multilinestring := [][][][]float64{{{{77.57227420806883, 12.948687079902895},
		{77.57600784301758, 12.954165970968194}},
		{{77.5779390335083, 12.94471376293191}, {77.57218837738037, 12.948268838994263}},
		{{77.57781028747559, 12.951740217268595}, {77.5779390335083, 12.945006535817749}}}}
	doc = document.NewDocument("multilinestring1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multilinestring, "multilinestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multilinestring1 := [][][][]float64{{{{77.6015853881836, 12.990089451715061},
		{77.60476112365723, 12.987747683302153}},
		{{77.59875297546387, 12.988751301039581}, {77.59446144104004, 12.98197680263484}},
		{{77.60188579559325, 12.982604078764705}, {77.60557651519775, 12.987329508048184}}}}
	doc = document.NewDocument("multilinestring2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multilinestring1, "multilinestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multipoint1 := [][][][]float64{{{{77.56618022918701, 12.958180959662695},
		{77.56407737731932, 12.951614746607163},
		{77.56922721862793, 12.956173473406446}}}}
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

	polygon4 := [][][][]float64{{{{-121.48125886917113, 38.51009442323401},
		{-121.48361921310425, 38.51012800441735}, {-121.48497104644774, 38.50858325377352},
		{-121.48366212844849, 38.507861239391026}, {-121.48353338241577, 38.50277335141579},
		{-121.4803147315979, 38.50267259752949}, {-121.48033618927, 38.5046204810195},
		{-121.47771835327147, 38.50754220747402}, {-121.48123741149902, 38.508616835661655},
		{-121.48125886917113, 38.51009442323401}}}}

	doc = document.NewDocument("polygon4")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon4, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multipolygon1 := [][][][]float64{{{{-121.49104356765746, 38.52149433504263},
		{-121.47857666015625, 38.51592052417851}, {-121.47688150405884, 38.515970891871696},
		{-121.4770746231079, 38.51714612804143}, {-121.49033546447754, 38.52221621271097},
		{-121.49104356765746, 38.52149433504263}}},
		{{{-121.47647380828859, 38.51714612804143}, {-121.47658109664916, 38.51477884701455},
			{-121.4741563796997, 38.5159876810949}, {-121.47647380828859, 38.51714612804143}}}}

	doc = document.NewDocument("multipolygon1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multipolygon1, "multipolygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	return i
}

func TestGeoJsonMultiPolygonWithInQuery(t *testing.T) {
	tests := []struct {
		polygon [][][][]float64
		field   string
		want    []string
	}{
		// test within multipolygon query for multipolygon1.
		// (where each query polygon contains each of the indexed polygons)
		{[][][][]float64{{
			{{-121.49458408355713, 38.53270780324851}, {-121.48823261260985, 38.52533866992879},
				{-121.48048639297485, 38.53253994984147}, {-121.49458408355713, 38.53270780324851}}},
			{{{-121.48700952529907, 38.53306029412857}, {-121.48160219192505, 38.53306029412857},
				{-121.48160219192505, 38.53829709805414}, {-121.48700952529907, 38.53829709805414},
				{-121.48700952529907, 38.53306029412857}}},
			{{{-121.47344827651976, 38.54475865436684}, {-121.46396398544312, 38.54475865436684},
				{-121.46396398544312, 38.55366961462033},
				{-121.47344827651976, 38.55366961462033}, {-121.47344827651976, 38.54475865436684}}}},
			"geometry", []string{"multipolygon1"}},

		// test within multipolygon query. (only partial containment of the three
		// indexed polygons by the two query polygons)
		{[][][][]float64{{
			{{-121.49458408355713, 38.53270780324851}, {-121.48823261260985, 38.52533866992879},
				{-121.48048639297485, 38.53253994984147}, {-121.49458408355713, 38.53270780324851}}},
			{{{-121.48700952529907, 38.53306029412857}, {-121.48160219192505, 38.53306029412857},
				{-121.48160219192505, 38.53829709805414}, {-121.48700952529907, 38.53829709805414},
				{-121.48700952529907, 38.53306029412857}}},
			{{{-121.4734697341919, 38.544825784372485}, {-121.4644145965576, 38.544825784372485},
				{-121.4644145965576, 38.5537199558913}, {-121.4734697341919, 38.5537199558913},
				{-121.4734697341919, 38.544825784372485}}}},
			"geometry", nil},

		// test within multipolygon query for multilinestring1.
		{[][][][]float64{{{{-121.49876832962036, 38.551739839324334},
			{-121.49814605712889, 38.54553064564853}, {-121.49158000946044, 38.54908841140355},
			{-121.49876832962036, 38.551739839324334}}},
			{{{-121.49258852005006, 38.54294612052762}, {-121.49117231369017, 38.54294612052762},
				{-121.49117231369017, 38.54526212788182}, {-121.49258852005006, 38.54526212788182},
				{-121.49258852005006, 38.54294612052762}},
			}}, "geometry", []string{"multilinestring1"}},

		// test within multipolygon query for multipoint1.
		{[][][][]float64{{{{-121.50286674499512, 38.564810956372185},
			{-121.49694442749023, 38.56226068115802}, {-121.48406982421875, 38.5675624676039},
			{-121.4875030517578, 38.57514535565976}, {-121.50286674499512, 38.564810956372185}}},
			{{{-121.48685932159422, 38.565163289911425}, {-121.48623704910278, 38.56283114531348},
				{-121.48357629776001, 38.565129734410704}, {-121.48685932159422, 38.565163289911425}}},
			{{{-121.49430513381958, 38.56195866888961}, {-121.4899492263794, 38.5584518779682},
				{-121.48842573165892, 38.56194189039304}, {-121.49430513381958, 38.56195866888961}},
			}}, "geometry", []string{"multipoint1"}},
	}
	i := setupGeoJsonShapesIndexForMultiPolygonQuery(t)
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
		got, err := runGeoShapeMultiPolygonQueryWithRelation("within",
			indexReader, test.polygon, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.polygon)
		}
	}
}

func runGeoShapeMultiPolygonQueryWithRelation(relation string,
	i index.IndexReader,
	points [][][][]float64, field string) ([]string, error) {
	var rv []string
	s := geo.NewGeoJsonMultiPolygon(points)

	gbs, err := NewGeoShapeSearcher(i, s, relation,
		field, 1.0, search.SearcherOptions{})
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

func setupGeoJsonShapesIndexForMultiPolygonQuery(t *testing.T) index.Index {
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

	multipolygon1 := [][][][]float64{{{{-121.49140834808348, 38.5320028163074},
		{-121.49112939834593, 38.52916601331889}, {-121.48889780044556, 38.52913244101627},
		{-121.4887261390686, 38.527655244193205}, {-121.48559331893921, 38.52794061412457},
		{-121.48638725280762, 38.53213710006686}, {-121.49140834808348, 38.5320028163074}}}, // polygon1
		{{{-121.48677349090575, 38.533194575914315}, {-121.48179531097412, 38.533194575914315},
			{-121.48179531097412, 38.53814604174215}, {-121.48677349090575, 38.53814604174215},
			{-121.48677349090575, 38.533194575914315}}}, // polygon2
		{{{-121.47334098815918, 38.553485029658475}, {-121.47329807281494, 38.54485934935182},
			{-121.46415710449219, 38.54526212788182}, {-121.47334098815918, 38.553485029658475}}}}
	doc := document.NewDocument("multipolygon1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multipolygon1, "multipolygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multilinestring1 := [][][][]float64{{{{-121.4983820915222, 38.55081688500274},
		{-121.49649381637572, 38.550447699956685}},
		{{-121.49655818939209, 38.548635309508775}, {-121.49370431900023, 38.54811507788636}},
		{{-121.49134397506714, 38.54490969679143}, {-121.4919662475586, 38.54304681805045}}}}
	doc = document.NewDocument("multilinestring1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multilinestring1, "multilinestring", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	multipoint1 := [][][][]float64{{{{-121.48960590362547, 38.56066671319285},
		{-121.4933180809021, 38.56157276247755}, {-121.4973521232605, 38.56318348855919},
		{-121.48582935333252, 38.56736114108619}, {-121.50104284286498, 38.56449217691959},
		{-121.4881682395935, 38.57158887950165}}}}
	doc = document.NewDocument("multipoint1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		multipoint1, "multipoint", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	return i
}

func setupGeoJsonPolygonS2LoopPortingIssue(t *testing.T) index.Index {
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

	polygon1 := [][][][]float64{{{{-135.0, -38.0},
		{149.0, -38.0}, {149.0, 77.0},
		{-135.0, 77.0}}}}
	doc := document.NewDocument("polygon1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon1, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	return i
}

func TestGeoJsonPolygonContainsQueryS2LoopPortingIssue(t *testing.T) {
	tests := []struct {
		polygon [][][]float64
		field   string
		want    []string
	}{

		// test containment query polygon for polygon1.
		{[][][]float64{{{13.007812500000002, 37.99616267972809},
			{13.559375000000002, 37.99616267972809}, {13.559375000000002, 38.472819658516866},
			{13.007812500000002, 38.472819658516866}}},
			"geometry", []string{"polygon1"}},

		// test containment query polygon for polygon1.
		{[][][]float64{{{13.007812500000002, 37.99616267972809},
			{13.359375000000002, 37.99616267972809}, {13.359375000000002, 38.272819658516866},
			{13.007812500000002, 38.272819658516866}}},
			"geometry", []string{"polygon1"}},
	}
	i := setupGeoJsonPolygonS2LoopPortingIssue(t)
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
		got, err := runGeoShapePolygonQueryWithRelation("contains",
			indexReader, test.polygon, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v",
				n, test.want, got, test.polygon)
		}
	}
}

func TestGeoJsonPolygonIntersectsQuery1(t *testing.T) {
	tests := []struct {
		polygon [][][]float64
		field   string
		want    []string
	}{

		// test non-intersecting query polygon.
		{[][][]float64{{{97.745361328125,
			68.21644657802169},
			{97.701416015625,
				67.97051353559428}, {97.80029296875,
				67.97875365614591},
			{97.745361328125,
				68.21644657802169}}}, "geometry", nil},

		// test intersecting query polygon.
		{[][][]float64{{{77.59214401245117,
			12.966043458314124},
			{77.58853912353516,
				12.95232574618635}, {77.60943889617919,
				12.956466232826733},
			{77.59214401245117,
				12.966043458314124}}}, "geometry", nil},

		// test intersecting query polygon for polygon1.
		{[][][]float64{{{97.0806884765625, 61.61423180712503},
			{96.7510986328125, 61.54625879879804},
			{97.305908203125, 61.367777577924},
			{97.0806884765625, 61.61423180712503}}}, "geometry", []string{"polygon1"}},
	}
	i := setupGeoJsonShapesIndexForPolygonQuery1(t)
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
		got, err := runGeoShapePolygonQueryWithRelation("intersects",
			indexReader, test.polygon, test.field)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("test %d, expected %v, got %v for polygon: %+v", n,
				test.want, got, test.polygon)
		}
	}
}

func setupGeoJsonShapesIndexForPolygonQuery1(t *testing.T) index.Index {
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

	polygon1 := [][][][]float64{{{{96.69202458735312, 61.59480859768306},
		{96.79202458735311, 61.39480859768306},
		{96.79202458735311, 61.59480859768306},
		{96.69202458735312, 61.59480859768306}}}}
	doc := document.NewDocument("polygon1")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon1, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	polygon2 := [][][][]float64{{{{91.35604953911839, 65.11164029408492},
		{91.45604953911838, 64.91164029408492},
		{91.45604953911838, 65.11164029408492},
		{91.35604953911839, 65.11164029408492}}}}
	doc = document.NewDocument("polygon2")
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		polygon2, "polygon", document.DefaultGeoShapeIndexingOptions))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	return i
}
