//  Copyright (c) 2026 Couchbase, Inc.
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
	"fmt"
	"sort"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/index/upsidedown/store/gtreap"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/geo/geojson"
)

const geoV2TestField = "geometry"

// box returns a single-polygon coordinate set (one CCW exterior ring)
// spanning the given lng/lat extents.
func box(minLng, minLat, maxLng, maxLat float64) [][][][]float64 {
	return [][][][]float64{{{
		{minLng, minLat},
		{maxLng, minLat},
		{maxLng, maxLat},
		{minLng, maxLat},
		{minLng, minLat},
	}}}
}

func addGeoShapeV2Doc(t *testing.T, i index.Index, id, typ string,
	coords [][][][]float64) {
	shape := &geojson.GeoShape{Type: typ, Coordinates: coords}
	doc := document.NewDocument(id)
	f := document.NewGeoShapeV2FieldFromShapeWithIndexingOptions(
		geoV2TestField, shape, index.IndexField)
	if f == nil {
		t.Fatalf("failed to build geoshape_v2 field for %q", id)
	}
	doc.AddField(f)
	if err := i.Update(doc); err != nil {
		t.Fatal(err)
	}
}

// setupGeoShapeV2Index builds a scorch index with a spread of shapes whose
// relationships to the query box Q = [0,0]-[10,10] are unambiguous:
//
//	smallInside   : polygon fully inside Q
//	pointInside   : point inside Q
//	overlapping   : polygon straddling the corner of Q
//	bigOutside    : polygon fully disjoint from Q
//	hugeContainer : polygon that fully contains Q
func setupGeoShapeV2Index(t *testing.T) index.Index {
	analysisQueue := index.NewAnalysisQueue(1)
	i, err := scorch.NewScorch(gtreap.Name,
		map[string]interface{}{
			"path":          "",
			"spatialPlugin": "s2",
		}, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err = i.Open(); err != nil {
		t.Fatal(err)
	}

	addGeoShapeV2Doc(t, i, "smallInside", "polygon", box(2, 2, 4, 4))
	addGeoShapeV2Doc(t, i, "pointInside", "point",
		[][][][]float64{{{{5, 5}}}})
	addGeoShapeV2Doc(t, i, "overlapping", "polygon", box(8, 8, 15, 15))
	addGeoShapeV2Doc(t, i, "bigOutside", "polygon", box(20, 20, 25, 25))
	addGeoShapeV2Doc(t, i, "hugeContainer", "polygon", box(-10, -10, 20, 20))

	return i
}

func executeGeoShapeV2Search(relation string, i index.IndexReader,
	s index.GeoJSON, field string) ([]string, error) {
	var rv []string
	gbs, err := NewGeoShapeV2Searcher(context.TODO(), i, s, relation, field,
		1.0, search.SearcherOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = gbs.Close() }()

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
	sort.Strings(rv)
	return rv, nil
}

func TestGeoShapeV2Relations(t *testing.T) {
	i := setupGeoShapeV2Index(t)
	defer func() { _ = i.Close() }()

	indexReader, err := i.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = indexReader.Close() }()

	// query box Q = [0,0]-[10,10]
	queryShape, _, err := geo.NewGeoJsonShape(box(0, 0, 10, 10), "polygon")
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		relation string
		want     []string
	}{
		{"within", []string{"pointInside", "smallInside"}},
		{"intersects", []string{"hugeContainer", "overlapping", "pointInside", "smallInside"}},
		{"contains", []string{"hugeContainer"}},
		{"disjoint", []string{"bigOutside"}},
	}

	for _, test := range tests {
		got, err := executeGeoShapeV2Search(test.relation, indexReader,
			queryShape, geoV2TestField)
		if err != nil {
			t.Fatalf("relation %q: %v", test.relation, err)
		}
		want := append([]string(nil), test.want...)
		sort.Strings(want)
		if !equalStrings(got, want) {
			t.Errorf("relation %q: expected %v, got %v", test.relation, want, got)
		}
	}
}

func TestGeoShapeV2Count(t *testing.T) {
	i := setupGeoShapeV2Index(t)
	defer func() { _ = i.Close() }()

	indexReader, err := i.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = indexReader.Close() }()

	queryShape, _, err := geo.NewGeoJsonShape(box(0, 0, 10, 10), "polygon")
	if err != nil {
		t.Fatal(err)
	}

	gbs, err := NewGeoShapeV2Searcher(context.TODO(), indexReader, queryShape,
		"intersects", geoV2TestField, 1.0, search.SearcherOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = gbs.Close() }()

	if got := gbs.Count(); got != 4 {
		t.Fatalf("expected Count 4 for intersects, got %d", got)
	}
}

func TestGeoShapeV2UnsupportedReaderErrors(t *testing.T) {
	// a nil / non-GeoShapeV2 index reader must produce an error rather than
	// a nil searcher, so that callers which do not nil-check (e.g. the
	// conjunction searcher) do not panic
	queryShape, _, err := geo.NewGeoJsonShape(box(0, 0, 10, 10), "polygon")
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewGeoShapeV2Searcher(context.TODO(), nil, queryShape,
		"intersects", geoV2TestField, 1.0, search.SearcherOptions{})
	if err == nil {
		t.Fatal("expected an error for an index reader that does not support geoshape_v2")
	}
}

// TestGeoShapeV2Advance drives Advance across the per-segment iterators and
// confirms it lands on the requested document and continues correctly. It
// also guards the ConstantScorer's per-match ID copy: the internal IDs
// collected across successive Next calls must stay distinct rather than all
// aliasing the reader's reused buffer.
func TestGeoShapeV2Advance(t *testing.T) {
	i := setupGeoShapeV2Index(t)
	defer func() { _ = i.Close() }()

	indexReader, err := i.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = indexReader.Close() }()

	queryShape, _, err := geo.NewGeoJsonShape(box(0, 0, 10, 10), "polygon")
	if err != nil {
		t.Fatal(err)
	}

	// first pass: collect the ordered internal IDs of all intersects hits,
	// copying each one so a reused buffer cannot corrupt the collection
	newSearcher := func() search.Searcher {
		s, serr := NewGeoShapeV2Searcher(context.TODO(), indexReader, queryShape,
			"intersects", geoV2TestField, 1.0, search.SearcherOptions{})
		if serr != nil {
			t.Fatal(serr)
		}
		return s
	}

	s1 := newSearcher()
	defer func() { _ = s1.Close() }()
	ctx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(s1.DocumentMatchPoolSize(), 0),
	}

	var ids []index.IndexInternalID
	dm, err := s1.Next(ctx)
	for dm != nil && err == nil {
		ids = append(ids, append(index.IndexInternalID(nil), dm.IndexInternalID...))
		dm, err = s1.Next(ctx)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) < 3 {
		t.Fatalf("expected at least 3 intersects hits to test Advance, got %d", len(ids))
	}

	// the collected IDs must be strictly increasing and distinct; if the
	// scorer aliased the reader's buffer they would all be equal
	for k := 1; k < len(ids); k++ {
		if ids[k].Compare(ids[k-1]) <= 0 {
			t.Fatalf("collected internal IDs are not strictly increasing: %v", ids)
		}
	}

	// second pass: Advance directly to the second hit and confirm we land on it
	s2 := newSearcher()
	defer func() { _ = s2.Close() }()
	ctx2 := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(s2.DocumentMatchPoolSize(), 0),
	}
	got, err := s2.Advance(ctx2, ids[1])
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || !got.IndexInternalID.Equals(ids[1]) {
		t.Fatalf("Advance did not land on the requested doc: want %v, got %v",
			ids[1], got)
	}
	// continuing with Next must yield the following hit
	next, err := s2.Next(ctx2)
	if err != nil {
		t.Fatal(err)
	}
	if next == nil || !next.IndexInternalID.Equals(ids[2]) {
		t.Fatalf("Next after Advance: want %v, got %v", ids[2], next)
	}
}

func TestGeoShapeV2CircleAndCollection(t *testing.T) {
	analysisQueue := index.NewAnalysisQueue(1)
	i, err := scorch.NewScorch(gtreap.Name,
		map[string]interface{}{"path": "", "spatialPlugin": "s2"}, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err = i.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = i.Close() }()

	// a circle centred inside Q
	circle := &geojson.GeoShape{Type: geo.CircleType, Center: []float64{5, 5}, Radius: "100m"}
	doc := document.NewDocument("circleInside")
	cf := document.NewGeoShapeV2FieldFromShapeWithIndexingOptions(geoV2TestField,
		circle, index.IndexField)
	if cf == nil {
		t.Fatal("failed to build circle geoshape_v2 field")
	}
	doc.AddField(cf)
	if err = i.Update(doc); err != nil {
		t.Fatal(err)
	}

	// a geometry collection with one polygon inside Q and one far outside
	shapes := []*geojson.GeoShape{
		{Type: "polygon", Coordinates: box(1, 1, 3, 3)},
		{Type: "polygon", Coordinates: box(40, 40, 42, 42)},
	}
	gcDoc := document.NewDocument("collection")
	gf := document.NewGeometryCollectionV2FieldFromShapesWithIndexingOptions(
		geoV2TestField, shapes, index.IndexField)
	if gf == nil {
		t.Fatal("failed to build geometry collection geoshape_v2 field")
	}
	gcDoc.AddField(gf)
	if err = i.Update(gcDoc); err != nil {
		t.Fatal(err)
	}

	indexReader, err := i.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = indexReader.Close() }()

	queryShape, _, err := geo.NewGeoJsonShape(box(0, 0, 10, 10), "polygon")
	if err != nil {
		t.Fatal(err)
	}

	// both the circle and the collection (via its inside polygon) intersect Q
	got, err := executeGeoShapeV2Search("intersects", indexReader, queryShape, geoV2TestField)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"circleInside", "collection"}
	if !equalStrings(got, want) {
		t.Errorf("intersects: expected %v, got %v", want, got)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// geoV2FieldSuffix is appended to a v1 geoshape field name to produce the
// parallel geoshape_v2 field name. The shared v1 test corpus indexes each
// shape under both names so every v1 test also exercises the v2 path.
const geoV2FieldSuffix = "_v2"

// addGeoShapeV2Parallel adds only the geoshape_v2 field, under "<name>_v2".
func addGeoShapeV2Parallel(doc *document.Document, name, typ string,
	coords [][][][]float64) {
	shape := &geojson.GeoShape{Type: typ, Coordinates: coords}
	if f := document.NewGeoShapeV2FieldFromShapeWithIndexingOptions(
		name+geoV2FieldSuffix, shape, index.IndexField); f != nil {
		doc.AddField(f)
	}
}

// addGeoCircleV2Parallel adds only the geoshape_v2 circle field, under "<name>_v2".
func addGeoCircleV2Parallel(doc *document.Document, name string,
	center []float64, radius string) {
	shape := &geojson.GeoShape{Type: geo.CircleType, Center: center, Radius: radius}
	if f := document.NewGeoShapeV2FieldFromShapeWithIndexingOptions(
		name+geoV2FieldSuffix, shape, index.IndexField); f != nil {
		doc.AddField(f)
	}
}

// addGeoCollectionV2Parallel adds only the geoshape_v2 geometry collection
// field, under "<name>_v2".
func addGeoCollectionV2Parallel(doc *document.Document, name string,
	coords [][][][][]float64, types []string) {
	shapes := make([]*geojson.GeoShape, 0, len(coords))
	for idx := range coords {
		shapes = append(shapes, &geojson.GeoShape{Type: types[idx], Coordinates: coords[idx]})
	}
	if f := document.NewGeometryCollectionV2FieldFromShapesWithIndexingOptions(
		name+geoV2FieldSuffix, shapes, index.IndexField); f != nil {
		doc.AddField(f)
	}
}

// addGeoShapeFieldV1V2 adds the v1 geoshape field and, under "<name>_v2",
// the equivalent geoshape_v2 field carrying the same shape.
func addGeoShapeFieldV1V2(doc *document.Document, name string, ap []uint64,
	coords [][][][]float64, typ string, opts index.FieldIndexingOptions) {
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions(name, ap, coords, typ, opts))
	addGeoShapeV2Parallel(doc, name, typ, coords)
}

// addGeoCircleFieldV1V2 mirrors addGeoShapeFieldV1V2 for circle shapes.
func addGeoCircleFieldV1V2(doc *document.Document, name string, ap []uint64,
	center []float64, radius string, opts index.FieldIndexingOptions) {
	doc.AddField(document.NewGeoCircleFieldWithIndexingOptions(name, ap, center, radius, opts))
	addGeoCircleV2Parallel(doc, name, center, radius)
}

// addGeoCollectionFieldV1V2 mirrors addGeoShapeFieldV1V2 for geometry collections.
func addGeoCollectionFieldV1V2(doc *document.Document, name string, ap []uint64,
	coords [][][][][]float64, types []string, opts index.FieldIndexingOptions) {
	doc.AddField(document.NewGeometryCollectionFieldWithIndexingOptions(name, ap, coords, types, opts))
	addGeoCollectionV2Parallel(doc, name, coords, types)
}

// sameStringSet reports whether a and b contain the same elements, ignoring
// order and treating nil and empty as equal.
func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, v := range a {
		seen[v]++
	}
	for _, v := range b {
		seen[v]--
	}
	for _, c := range seen {
		if c != 0 {
			return false
		}
	}
	return true
}

// assertGeoShapeV2Agrees runs the geoshape_v2 searcher for the same shape and
// relation against the parallel "<field>_v2" field, and returns an error when
// its result set differs from the v1 result set. This is how each v1 searcher
// test transparently also validates the v2 implementation.
func assertGeoShapeV2Agrees(relation string, i index.IndexReader,
	s index.GeoJSON, field string, v1 []string) error {
	v2, err := executeGeoShapeV2Search(relation, i, s, field+geoV2FieldSuffix)
	if err != nil {
		return fmt.Errorf("geoshape_v2 search error for relation %q: %w", relation, err)
	}
	if !sameStringSet(v1, v2) {
		return fmt.Errorf("geoshape_v2 mismatch for relation %q on field %q: "+
			"v1=%v v2=%v", relation, field, v1, v2)
	}
	return nil
}
