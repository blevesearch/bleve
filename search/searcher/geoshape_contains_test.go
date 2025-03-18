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
	index "github.com/blevesearch/bleve_index_api"
)

var (
	leftRectEdgeMultiPoint [][]float64   = [][]float64{{-1, 0.2}, {-0.9, 0.1}}
	leftRectWithHole       [][][]float64 = [][][]float64{
		{{-1, 0}, {0, 0}, {0, 1}, {-1, 1}, {-1, 0}},
		{{-0.75, 0.25}, {-0.75, -0.75}, {-0.25, 0.75}, {-0.25, 0.25}, {-0.74, 0.25}},
	}
	leftRectEdgePoint  []float64   = []float64{-1, 0.2}
	leftRectMultiPoint [][]float64 = [][]float64{{0.5, 0.5}, {-0.9, 0.1}}
)

func testCaseSetup(t *testing.T, docShapeName, docShapeType string, docShapeVertices [][][][]float64,
	i index.Index,
) (index.IndexReader, func() error, error) {
	doc := document.NewDocument(docShapeName)
	doc.AddField(document.NewGeoShapeFieldWithIndexingOptions("geometry", []uint64{},
		docShapeVertices, docShapeType, document.DefaultGeoShapeIndexingOptions))
	err := i.Update(doc)
	if err != nil {
		return nil, nil, err
	}
	indexReader, err := i.Reader()
	if err != nil {
		t.Fatal(err)
	}

	closeFn := func() error {
		err = i.Delete(doc.ID())
		if err != nil {
			return err
		}
		err = indexReader.Close()
		if err != nil {
			return err
		}
		return nil
	}

	return indexReader, closeFn, nil
}

func TestPointPolygonContains(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       rightRectPoint,
			DocShapeVertices: rightRect,
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "point inside polygon",
			QueryType:        "contains",
		},
		{
			QueryShape:       leftRectPoint,
			DocShapeVertices: nil,
			DocShapeName:     "",
			Expected:         nil,
			Desc:             "empty polygon",
			QueryType:        "contains",
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "polygon", [][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType, false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v", test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestLinestringPolygonContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
		QueryType        string
	}{
		{
			QueryShape:       [][]float64{{1, 2}, {3, 5}},
			DocShapeVertices: [][][]float64{{{1, 2}, {3, 5}, {2, 7}, {1, 2}}},
			DocShapeName:     "polygon1",
			Desc:             "linestring coinciding with edge of the polygon",
			Expected:         []string{"polygon1"},
			QueryType:        "contains",
		},
		{
			QueryShape:       [][]float64{{1, 0}, {0, 1}},
			DocShapeVertices: rightRect,
			DocShapeName:     "polygon1",
			Desc:             "diagonal of a square",
			Expected:         []string{"polygon1"},
			QueryType:        "contains",
		},
		{
			QueryShape:       [][]float64{{0.2, 0.2}, {0.8, 0.8}},
			DocShapeVertices: rightRect,
			DocShapeName:     "polygon1",
			Desc:             "linestring within polygon",
			Expected:         []string{"polygon1"},
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "polygon",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeLinestringQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for linestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestEnvelopePointContains(t *testing.T) {
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
			Expected:         nil, // will always be nil since point can't contain envelope
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "point",
			[][][][]float64{{{test.DocShapeVertices}}}, i)
		if err != nil {
			t.Error(err.Error())
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

		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestEnvelopeLinestringContains(t *testing.T) {
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
			DocShapeVertices: [][]float64{{0.5, 0.5}, {10, 10}},
			DocShapeName:     "linestring1",
			Desc:             "linestring partially within bounded rectangle",
			Expected:         nil, // will always be nil since linestring can't contain envelope
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "linestring",
			[][][][]float64{{test.DocShapeVertices}}, i)
		if err != nil {
			t.Error(err.Error())
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

		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestEnvelopePolygonContains(t *testing.T) {
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
			QueryType:        "contains",
		},
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: [][][]float64{{{10.5, 10.5}, {11.5, 10.5}, {11.5, 11.5}, {10.5, 11.5}, {10.5, 10.5}}},
			DocShapeName:     "polygon1",
			Desc:             "polygon completely outside bounded rectangle",
			Expected:         nil,
			QueryType:        "contains",
		},
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: rightRect,
			DocShapeName:     "polygon1",
			Desc:             "polygon coincident with bounded rectangle",
			Expected:         []string{"polygon1"},
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "polygon",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Error(err.Error())
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

		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestPolygonPointContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       rightRect,
			DocShapeVertices: rightRectPoint,
			DocShapeName:     "point1",
			Expected:         nil, // nil since point is a non-closed shape
			Desc:             "point inside polygon",
			QueryType:        "contains",
		},
		{
			QueryShape:       leftRect,
			DocShapeVertices: leftRectEdgePoint,
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point on edge of polygon",
			QueryType:        "contains",
		},
		{
			QueryShape:       leftRectWithHole,
			DocShapeVertices: leftRectPoint,
			DocShapeName:     "point1",
			Expected:         nil,
			Desc:             "point in polygon's hole",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "point",
			[][][][]float64{{{test.DocShapeVertices}}}, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestPolygonLinestringContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       rightRect,
			DocShapeVertices: [][]float64{{0, 1}, {1, 0}},
			DocShapeName:     "linestring1",
			Expected:         nil, // nil since linestring is a non-closed shape
			Desc:             "diagonal of a square",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "linestring",
			[][][][]float64{{test.DocShapeVertices}}, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestPolygonEnvelopeContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][][]float64{{{0.5, 0.5}, {1, 0.5}, {1, 1}, {0.5, 1}, {0.5, 0.5}}},
			DocShapeVertices: [][]float64{{0, 1}, {1, 0}},
			DocShapeName:     "envelope1",
			Expected:         nil,
			Desc:             "polygon contained inside envelope with edge overlaps", // this fails since
			// contains doesn't include edges or vertices
			QueryType: "contains",
		},
		{
			QueryShape:       [][][]float64{{{0.25, 0.25}, {0.5, 0.25}, {0.5, 0.5}, {0.25, 0.25}, {0.25, 0.25}}},
			DocShapeVertices: [][]float64{{0, 1}, {1, 0}},
			DocShapeName:     "envelope1",
			Expected:         []string{"envelope1"},
			Desc:             "polygon contained completely inside envelope",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "envelope",
			[][][][]float64{{test.DocShapeVertices}}, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestMultiPointPolygonContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       leftRectEdgeMultiPoint,
			DocShapeVertices: leftRectWithHole,
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "multi point inside polygon with hole",
			QueryType:        "contains",
		},
		{
			QueryShape:       [][]float64{{1, 0.5}},
			DocShapeVertices: rightRect,
			DocShapeName:     "polygon1",
			Expected:         nil,
			Desc:             "multi point on polygon edge",
			QueryType:        "contains",
		},
		{
			QueryShape: [][]float64{{0.3, 0.3}, {0.5, 0.5}},
			DocShapeVertices: [][][]float64{
				{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}},
				{{0.2, 0.2}, {0.4, 0.2}, {0.4, 0.4}, {0.2, 0.4}, {0.2, 0.2}},
			},
			DocShapeName: "polygon1",
			Expected:     nil, // returns nil since one of the points is within the hole
			Desc:         "multi point inside polygon and hole",
			QueryType:    "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "polygon",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
				true, indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipoint: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestMultiPointLinestringContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       leftRectEdgeMultiPoint,
			DocShapeVertices: [][]float64{{-1, 0.2}, {-0.9, 0.1}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "multi point overlaps with all linestring end points",
			QueryType:        "contains",
		},
		{
			QueryShape:       [][]float64{{-1, 0.2}, {-0.9, 0.1}, {0.5, 0.5}},
			DocShapeVertices: [][]float64{{-1, 0.2}, {-0.9, 0.1}},
			DocShapeName:     "linestring1",
			Expected:         nil,
			Desc:             "multi point overlaps with some linestring end points",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "linestring",
			[][][][]float64{{test.DocShapeVertices}}, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
				true, indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipoint: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestMultiPointContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       leftRectEdgeMultiPoint,
			DocShapeVertices: [][]float64{{-1, 0.2}, {-0.9, 0.1}},
			DocShapeName:     "multipoint1",
			Expected:         []string{"multipoint1"},
			Desc:             "multi point overlaps with all multi points",
			QueryType:        "contains",
		},
		{
			QueryShape:       [][]float64{{-1, 0.2}, {-0.9, 0.1}, {0.5, 0.5}},
			DocShapeVertices: [][]float64{{-1, 0.2}, {-0.9, 0.1}},
			DocShapeName:     "multipoint1",
			Expected:         nil,
			Desc:             "multi point overlaps with some multi points",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "multipoint",
			[][][][]float64{{test.DocShapeVertices}}, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery(test.QueryType,
				true, indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipoint: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestPolygonContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       leftRect,
			DocShapeVertices: rightRect,
			DocShapeName:     "polygon1",
			Expected:         nil,
			Desc:             "polygons sharing an edge",
			QueryType:        "contains",
		},
		{
			QueryShape:       rightRect,
			DocShapeVertices: rightRect,
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "coincident polygons",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "polygon",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestPolygonMultiPointContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       leftRect,
			DocShapeVertices: leftRectEdgeMultiPoint,
			DocShapeName:     "multipoint1",
			Expected:         nil, // nil since multipoint is a non-closed shape
			Desc:             "multiple points on polygon edge",
			QueryType:        "contains",
		},
		{
			QueryShape:       leftRect,
			DocShapeVertices: leftRectMultiPoint,
			DocShapeName:     "multipoint1",
			Expected:         nil,
			Desc:             "multiple points, both outside and inside polygon",
			QueryType:        "contains",
		},
		{
			QueryShape:       leftRectWithHole,
			DocShapeVertices: leftRectMultiPoint,
			DocShapeName:     "multipoint1",
			Expected:         nil,
			Desc:             "multiple points in polygon hole",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "multipoint",
			[][][][]float64{{test.DocShapeVertices}}, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestMultiPolygonPolygonContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][][][]float64{leftRect},
			DocShapeVertices: leftRect,
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "coincident polygons",
			QueryType:        "contains",
		},
		{
			QueryShape:       [][][][]float64{{{{2, 2}, {-2, 2}, {-2, -2}, {2, -2}}}},
			DocShapeVertices: leftRect,
			DocShapeName:     "polygon1",
			Expected:         nil,
			Desc:             "polygon larger than polygons in query shape",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "polygon",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiPolygonQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestMultiLinestringMultiPolygonContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][][]float64{{{0.2, 1}, {0.8, 1}}, {{1, 0.2}, {1, 0.8}}},
			DocShapeVertices: [][][][]float64{rightRect},
			DocShapeName:     "multipolygon1",
			Expected:         nil, // contains doesn't include edges or vertices
			Desc:             "linestrings on edge of polygon",
			QueryType:        "contains",
		},
		{
			QueryShape:       [][][]float64{{{0.2, 0.2}, {0.8, 0.8}}, {{0.8, 0.2}, {0.2, 0.8}}},
			DocShapeVertices: [][][][]float64{{{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}}}},
			DocShapeName:     "multipolygon1",
			Expected:         []string{"multipolygon1"},
			Desc:             "linestrings within polygon",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "multipolygon", test.DocShapeVertices, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiLinestringQueryWithRelation(test.QueryType,
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multilinestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestGeometryCollectionPolygonContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][][]float64
		QueryShapeTypes  []string
		DocShapeVertices [][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][][][][]float64{{{{{0, 1}, {1, 0}}}}},
			QueryShapeTypes:  []string{"linestring"},
			DocShapeVertices: rightRect,
			DocShapeName:     "polygon1",
			Expected:         []string{"polygon1"},
			Desc:             "linestring on edge of polygon",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "polygon",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeGeometryCollectionRelationQuery(test.QueryType,
				indexReader, test.QueryShape, test.QueryShapeTypes, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}

func TestGeometryCollectionMultiPolygonContains(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][][]float64
		QueryShapeTypes  []string
		DocShapeVertices [][][][]float64
		DocShapeName     string
		Expected         []string
		Desc             string
		QueryType        string
	}{
		{
			QueryShape:       [][][][][]float64{{{{{1, 1}}}}},
			QueryShapeTypes:  []string{"point"},
			DocShapeVertices: [][][][]float64{rightRect, leftRect},
			DocShapeName:     "multipolygon1",
			Expected:         nil,
			Desc:             "point on vertex of one of the polygons",
			QueryType:        "contains",
		},
		{
			// WIP - Adding a point (-0.5,-0.5)
			QueryShape:       [][][][][]float64{{{{{0.2, 0.4}, {0.2, 0.2}, {0.4, 0.2}, {0.4, 0.4}}}}},
			QueryShapeTypes:  []string{"polygon"},
			DocShapeVertices: [][][][]float64{rightRect, leftRect},
			DocShapeName:     "multipolygon1",
			Expected:         []string{"multipolygon1"},
			Desc:             "polygon contained completely within multipolygons",
			QueryType:        "contains",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "multipolygon", test.DocShapeVertices, i)
		if err != nil {
			t.Error(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeGeometryCollectionRelationQuery(test.QueryType,
				indexReader, test.QueryShape, test.QueryShapeTypes, "geometry")
			if err != nil {
				t.Error(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipolygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Error(err.Error())
		}
	}
}
