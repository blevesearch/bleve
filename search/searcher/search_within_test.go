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
)

var (
	leftRect             [][][]float64   = [][][]float64{{{-1, 0}, {0, 0}, {0, 1}, {-1, 1}, {-1, 0}}}
	leftRectPoint        []float64       = []float64{-0.5, 0.5}
	rightRect            [][][]float64   = [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}}
	rightRectPoint       []float64       = []float64{0.5, 0.5}
	overLappingRightRect [][][][]float64 = [][][][]float64{{{{-1, 0}, {1, 0}, {1, 1}, {-0.1, 1}, {-1, 0}}}}
	middleLine           [][]float64     = [][]float64{{-0.5, 0.5}, {0.5, 0.5}}
)

func TestPointWithin(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices []float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       []float64{1.0, 1.0},
			DocShapeVertices: []float64{1.0, 1.0},
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point contains itself",
			QueryType:        "within",
		},
		{
			QueryShape:       []float64{1.0, 1.0},
			DocShapeVertices: []float64{1.0, 1.1},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point does not contain a different point",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{{test.DocShapeVertices}}}, "point", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultiPointWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}},
			DocShapeVertices: [][]float64{{1.0, 1.0}},
			DocShapeName:     "multipoint1",
			Expected:         []string{"multipoint1"},
			Desc:             "single multipoint common",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}},
			DocShapeVertices: [][]float64{{1.0, 1.0}, {2.0, 2.0}},
			DocShapeName:     "multipoint1",
			Expected:         nil,
			Desc:             "multipoint not covered by multiple multipoints",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "multipoint", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
				true, indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipoint: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestEnvelopePointWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Desc             string
		Expected         []string
		QueryType        string
	}{
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: []float64{0.5, 0.5},
			DocShapeName:     "point1",
			Desc:             "point completely within bounded rectangle",
			Expected:         []string{"point1"},
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: []float64{0, 1},
			DocShapeName:     "point1",
			Desc:             "point on vertex of bounded rectangle",
			Expected:         []string{"point1"},
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: []float64{10, 11},
			DocShapeName:     "point1",
			Desc:             "point outside bounded rectangle",
			Expected:         nil,
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{{test.DocShapeVertices}}}, "point", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeEnvelopeRelationQuery(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for Envelope: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
	}
}

func TestEnvelopeLinestringWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
		QueryType        string
	}{
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: [][]float64{{0.5, 0.5}, {0.75, 0.75}},
			DocShapeName:     "linestring1",
			Desc:             "linestring completely within bounded rectangle",
			Expected:         []string{"linestring1"},
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: [][]float64{{0.5, 0.5}, {1.75, 1.75}},
			DocShapeName:     "linestring1",
			Desc:             "linestring partially within bounded rectangle",
			Expected:         nil,
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: [][]float64{{1.5, 2.5}, {2.75, 2.75}},
			DocShapeName:     "linestring1",
			Desc:             "linestring completely outside bounded rectangle",
			Expected:         nil,
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "linestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeEnvelopeRelationQuery(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for Envelope: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
	}
}

func TestEnvelopePolygonWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
		QueryType        string
	}{
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: [][][]float64{{{0.5, 0.5}, {1, 0.5}, {1, 1}, {0.5, 1}, {0.5, 0.5}}},
			DocShapeName:     "polygon1",
			Desc:             "polygon completely within bounded rectangle",
			Expected:         nil,
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: [][][]float64{{{0.5, 0.5}, {1.5, 0.5}, {1.5, 1.5}, {0.5, 1.5}, {0.5, 0.5}}},
			DocShapeName:     "polygon1",
			Desc:             "polygon partially within bounded rectangle",
			Expected:         nil,
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: [][][]float64{{{10.5, 10.5}, {11.5, 10.5}, {11.5, 11.5}, {10.5, 11.5}, {10.5, 10.5}}},
			DocShapeName:     "polygon1",
			Desc:             "polygon completely outside bounded rectangle",
			Expected:         nil,
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "polygon", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeEnvelopeRelationQuery(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for Envelope: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
	}
}

func TestPointLinestringWithin(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       []float64{1.0, 1.0},
			DocShapeVertices: [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "point does not cover different linestring",
			QueryType:        "within",
		},
		{
			QueryShape:       []float64{179.1, 0.0},
			DocShapeVertices: [][]float64{{-179.0, 0.0}, {179.0, 0.0}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "point across latitudinal boundary of linestring",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "linestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPointPolygonWithin(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       []float64{1.0, 1.0},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeName:     "polygon1",
			Expected:         nil,
			Desc:             "point not within polygon",
			QueryType:        "within",
		},
		{ // from binary predicates file
			QueryShape:       rightRectPoint,
			DocShapeVertices: rightRect,
			DocShapeName:     "polygon1",
			Expected:         nil, // will return nil since a point only returns non-nil for a coincident point
			// even if the point is on the polygon
			Desc:      "point on rectangle vertex",
			QueryType: "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "polygon", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLinestringPointWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		// expect nil in each case since when linestring is the query shape, it's always nil
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{1.0, 1.0},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point at start of linestring",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{2.0, 2.0},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point in the middle of linestring",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{3.0, 3.0},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point at end of linestring",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{1.5, 1.50017},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point in between linestring",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeVertices: []float64{4, 5},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point not contained by linestring",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{{test.DocShapeVertices}}}, "point", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeLinestringIntersectsQuery(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for linestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultiPointMultiLinestringWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][]float64{{2, 2}, {2.1, 2.1}},
			DocShapeVertices: [][][]float64{{{1, 1}, {1.1, 1.1}}, {{2, 2}, {2.1, 2.1}}},
			DocShapeName:     "multilinestring1",
			Expected:         nil, // nil since multipoint within multiline is always nil
			Desc:             "multilinestring covering multipoint",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{2, 2}, {1, 1}, {3, 3}},
			DocShapeVertices: [][][]float64{{{1, 1}, {1.1, 1.1}}, {{2, 2}, {2.1, 2.1}}},
			DocShapeName:     "multipoint1",
			Expected:         nil,
			Desc:             "multilinestring not covering multipoint",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "multilinestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
				true, indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multilinestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLinestringWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][]float64{{1, 1}, {2, 2}, {3, 3}},
			DocShapeVertices: [][]float64{{1, 1}, {2, 2}, {3, 3}, {4, 4}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "longer linestring",
			QueryType:        "within",
		},
		{
			QueryShape:       [][]float64{{1, 1}, {2, 2}, {3, 3}},
			DocShapeVertices: [][]float64{{1, 1}, {2, 2}, {3, 3}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "coincident linestrings",
			QueryType:        "within",
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "linestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeLinestringIntersectsQuery(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for linestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestLinestringGeometryCollectionWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][][][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
		Types            []string
	}{
		{
			QueryShape:       [][]float64{{1, 1}, {2, 2}},
			DocShapeVertices: [][][][][]float64{{{{{1, 1}}}}},
			DocShapeName:     "geometrycollection1",
			Expected:         nil, // LS is not a closed shape
			Desc:             "geometry collection with a point on vertex of linestring",
			Types:            []string{"point"},
			QueryType:        "within",
		},
		{},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeometryCollectionFieldWithIndexingOptions("geometry",
			[]uint64{}, test.DocShapeVertices, test.Types, document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeLinestringIntersectsQuery(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for linestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPolygonPointWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: []float64{0.5, 0.5},
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point within polygon",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: []float64{5.5, 5.5},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point not within polygon",
			QueryType:        "within",
		},
		{
			QueryShape: [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0},
				{0.2, 0.2}, {0.2, 0.4}, {0.4, 0.4}, {0.4, 0.2}, {0.2, 0.2}}},
			DocShapeVertices: []float64{0.3, 0.3},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point within polygon hole",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeVertices: []float64{1.0, 0.0},
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point on polygon vertex",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{1, 1}, {2, 2}, {0, 2}, {1, 1}}},
			DocShapeVertices: []float64{1.5, 1.5001714},
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point inside polygon",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{150, 85}, {-20, -85}, {-30, 85}, {160, -85}, {150, 85}}},
			DocShapeVertices: []float64{170, 85},
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point outside the polygon's latitudinal boundary",
			QueryType:        "within",
		},
		{
			// from binary predicates tests
			QueryShape:       leftRect,
			DocShapeVertices: leftRectPoint,
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point in left rectangle",
			QueryType:        "within",
		},
		{
			// from binary predicates tests
			QueryShape:       rightRect,
			DocShapeVertices: rightRectPoint,
			DocShapeName:     "point1",
			Expected:         []string{"point1"},
			Desc:             "point in right rectangle",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{{test.DocShapeVertices}}}, "point", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPolygonLinestringWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][]float64{{0.1, 0.1}, {0.4, 0.4}},
			DocShapeName:     "linestring1",
			Expected:         []string{"linestring1"},
			Desc:             "linestring within polygon",
			QueryType:        "within",
		},
		{
			QueryShape: [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
				{{0.2, 0.2}, {0.2, 0.4}, {0.4, 0.4}, {0.4, 0.2}, {0.2, 0.2}}},
			DocShapeVertices: [][]float64{{0.3, 0.3}, {0.55, 0.55}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "linestring intersecting with polygon hole",
			QueryType:        "within",
		},
		{
			QueryShape: [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
				{{0.2, 0.2}, {0.2, 0.4}, {0.4, 0.4}, {0.4, 0.2}, {0.2, 0.2}}},
			DocShapeVertices: [][]float64{{0.3, 0.3}, {4.0, 4.0}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "linestring intersecting with polygon hole and outside",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][]float64{{-1, -1}, {-2, -2}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "linestring outside polygon",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][]float64{{-0.5, -0.5}, {0.5, 0.5}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "linestring intersecting polygon",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "linestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestPolygonWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "coincident polygon",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][][]float64{{{0.2, 0.2}, {1, 0}, {1, 1}, {0, 1}, {0.2, 0.2}}},
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "polygon covers an intersecting window of itself",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][][]float64{{{0.1, 0.1}, {0.2, 0.1}, {0.2, 0.2}, {0.1, 0.2}, {0.1, 0.1}}},
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "polygon covers a nested version of itself",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][][]float64{{{-1, 0}, {1, 0}, {1, 1}, {-1, 1}, {-1, 0}}},
			DocShapeName:     "polygon1",
			Expected:         nil,
			Desc:             "intersecting polygons",
			QueryType:        "within",
		},
		{
			QueryShape:       [][][]float64{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
			DocShapeVertices: [][][]float64{{{3, 3}, {4, 3}, {4, 4}, {3, 4}, {3, 3}}},
			DocShapeName:     "polygon1",
			Expected:         nil,
			Desc:             "polygon totally out of range",
			QueryType:        "within",
		},
		{
			QueryShape:       leftRect,
			DocShapeVertices: rightRect,
			DocShapeName:     "polygon1",
			Expected:         nil,
			Desc:             "left and right polygons,sharing an edge",
			QueryType:        "within",
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "polygon", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultiPolygonMultiPointWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape: [][][][]float64{{{{30, 25}, {45, 40}, {10, 40}, {30, 20}, {30, 25}}},
				{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}}},
			DocShapeVertices: [][]float64{{30, 20}, {15, 5}},
			DocShapeName:     "multipoint1",
			Expected:         []string{"multipoint1"},
			Desc:             "multipolygon covers multipoint",
			QueryType:        "within",
		},
		{
			QueryShape: [][][][]float64{{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}},
				{{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][]float64{{30, 20}, {30, 30}, {45, 66}},
			DocShapeName:     "multipoint1",
			Expected:         nil,
			Desc:             "multipolygon does not cover multipoint",
			QueryType:        "within",
		},
		{
			QueryShape: [][][][]float64{{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}},
				{{{1, 0}, {2, 0}, {2, 1}, {1, 1}, {1, 0}}}},
			DocShapeVertices: [][]float64{{0.5, 0.5}, {1.5, 0.5}},
			DocShapeName:     "multipoint1",
			Expected:         []string{"multipoint1"},
			Desc:             "multiple multipolygons required to cover multipoint",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{{test.DocShapeVertices}}, "multipoint", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiPolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultiLinestringWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][][]float64{{{1, 2}, {2, 3}, {3, 4}}, {{5, 6}, {6.5, 7.8}}},
			DocShapeVertices: [][][]float64{{{1, 2}, {2, 3}, {3, 4}}},
			DocShapeName:     "multilinestring1",
			Expected:         nil,
			Desc:             "multilinestrings with common linestrings",
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "multilinestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiLinestringIntersectsQuery("within",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multilinestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultiPolygonMultiLinestringWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape: [][][][]float64{{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}},
				{{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][][]float64{{{45, 40}, {10, 40}}, {{45, 40}, {10, 40}, {30, 20}}},
			DocShapeName:     "multilinestring1",
			Expected:         []string{"multilinestring1"},
			Desc:             "multilinestring intersecting at the edge of multipolygon",
			QueryType:        "within",
		},
		{
			QueryShape: [][][][]float64{{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}},
				{{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][][]float64{{{45, 40}, {10, 40}}, {{45, 40}, {10, 40}, {30, 12}}},
			DocShapeName:     "multilinestring1",
			Expected:         []string{"multilinestring1"},
			Desc:             "multipolygon does not cover multilinestring",
			QueryType:        "within",
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			[][][][]float64{test.DocShapeVertices}, "multilinestring", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiPolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipolygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMultiPolygonWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][]float64
		DocShapeVertices [][][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape: [][][][]float64{{{{16, 6}, {41, 11}, {11, 21}, {6, 11}, {16, 6}}},
				{{{31, 21}, {46, 41}, {11, 41}, {31, 21}}}},
			DocShapeVertices: [][][][]float64{{{{31, 21}, {46, 41}, {11, 41}, {31, 21}}}},
			DocShapeName:     "multipolygon1",
			Expected:         []string{"multipolygon1"},
			Desc:             "multipolygon covers another multipolygon",
			QueryType:        "within",
		},
		{
			QueryShape: [][][][]float64{{{{16, 6}, {41, 11}, {11, 21}, {6, 11}, {16, 6}}},
				{{{31, 21}, {46, 41}, {11, 41}, {31, 21}}}},
			DocShapeVertices: [][][][]float64{{{{31, 21}, {46, 41}, {16, 46}, {31, 21}}}},
			DocShapeName:     "multipolygon1",
			Expected:         nil,
			Desc:             "multipolygon does not cover multipolygon",
			QueryType:        "within",
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
			test.DocShapeVertices, "multipolygon", document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Error(err)
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiPolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipolygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestGeometryCollectionWithin(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][][]float64
		DocShapeVertices [][][][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
		QueryType        string
		QueryShapeTypes  []string
		DocShapeTypes    []string
	}{
		{
			QueryShape:       nil,
			DocShapeVertices: nil,
			DocShapeName:     "geometrycollection1",
			Desc:             "empty geometry collections",
			Expected:         nil,
			QueryType:        "within",
			QueryShapeTypes:  nil,
			DocShapeTypes:    nil,
		},
		{
			QueryShape:       [][][][][]float64{{{{{1, 2}, {2, 3}}}}},
			DocShapeVertices: [][][][][]float64{{{{{1, 2}}}}},
			DocShapeName:     "geometrycollection1",
			Desc:             "geometry collection with a linestring",
			Expected:         nil, // linestring in geometry collection does not support within
			QueryShapeTypes:  []string{"linestring"},
			DocShapeTypes:    []string{"point"},
			QueryType:        "within",
		},
		{
			QueryShape:       [][][][][]float64{{{{{1, 2}, {2, 3}, {5, 6}}}}},
			DocShapeVertices: [][][][][]float64{{{{{1, 2}}}}},
			DocShapeName:     "geometrycollection1",
			Desc:             "geometry collections with common points and multipoints",
			Expected:         []string{"geometrycollection1"},
			QueryShapeTypes:  []string{"multipoint"},
			DocShapeTypes:    []string{"point"},
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeometryCollectionFieldWithIndexingOptions("geometry",
			[]uint64{}, test.DocShapeVertices, test.DocShapeTypes, document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeGeometryCollectionRelationQuery(test.QueryType,
				indexReader, test.QueryShape, test.QueryShapeTypes, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for geometry collection: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestGeometryCollectionPointWithin(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][][][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
		Types            []string
		QueryType        string
	}{
		{
			QueryShape:       []float64{1.0, 2.0},
			DocShapeVertices: nil,
			DocShapeName:     "geometrycollection1",
			Desc:             "empty geometry collection not within a point",
			Expected:         nil,
			Types:            nil,
			QueryType:        "within",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		doc := document.NewDocument(test.DocShapeName)
		doc.AddField(document.NewGeometryCollectionFieldWithIndexingOptions("geometry",
			[]uint64{}, test.DocShapeVertices, test.Types, document.DefaultGeoShapeIndexingOptions))
		err := i.Update(doc)
		if err != nil {
			t.Errorf(err.Error())
		}

		indexReader, err := i.Reader()
		if err != nil {
			t.Fatal(err)
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("intersects",
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = i.Delete(doc.ID())
		if err != nil {
			t.Errorf(err.Error())
		}
		err = indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
}
