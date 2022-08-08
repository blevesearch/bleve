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
)

func TestPointDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices []float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       []float64{2.0, 2.0},
			DocShapeVertices: []float64{2.0, 2.0},
			DocShapeName:     "point1",
			Desc:             "coincident points",
			Expected:         nil,
		},
		{
			QueryShape:       []float64{2.0, 2.0},
			DocShapeVertices: []float64{2.0, 2.1},
			DocShapeName:     "point2",
			Desc:             "non coincident points",
			Expected:         []string{"point2"},
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "point",
			[][][][]float64{{{test.DocShapeVertices}}}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		// indexing and searching independently for each case.
		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("disjoint",
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestPointMultiPointDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       []float64{2.0, 2.0},
			DocShapeVertices: [][]float64{{2.0, 2.0}, {3.0, 2.0}},
			DocShapeName:     "point1",
			Desc:             "point coincides with one point in multipoint",
			Expected:         nil,
		},
		{
			QueryShape:       []float64{2.0, 2.0},
			DocShapeVertices: [][]float64{{2.0, 2.1}, {3.0, 3.1}},
			DocShapeName:     "point2",
			Desc:             "non coincident points",
			Expected:         []string{"point2"},
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "point",
			[][][][]float64{{test.DocShapeVertices}}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		// indexing and searching independently for each case.
		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("disjoint",
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestPointLinestringDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       []float64{4.0, 4.0},
			DocShapeVertices: [][]float64{{2.0, 2.0}, {3.0, 3.0}, {4.0, 4.0}},
			DocShapeName:     "linestring1",
			Desc:             "point at the vertex of linestring",
			Expected:         nil,
		},
		{
			QueryShape:       []float64{1.5, 1.5001714},
			DocShapeVertices: [][]float64{{0.0, 0.0}, {1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeName:     "linestring1",
			Desc:             "point along linestring",
			Expected:         []string{"linestring1"},
		},
		{
			QueryShape:       []float64{1.5, 1.6001714},
			DocShapeVertices: [][]float64{{0.0, 0.0}, {1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}},
			DocShapeName:     "linestring1",
			Desc:             "point outside linestring",
			Expected:         []string{"linestring1"},
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "linestring",
			[][][][]float64{{test.DocShapeVertices}}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("disjoint",
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestPointMultiLinestringDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       []float64{3.0, 3.0},
			DocShapeVertices: [][][]float64{{{2.0, 2.0}, {3.0, 3.0}, {4.0, 4.0}}},
			DocShapeName:     "multilinestring1",
			Desc:             "point at the vertex of linestring",
			Expected:         nil,
		},
		{
			QueryShape:       []float64{1.5, 1.5001714},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}}},
			DocShapeName:     "multilinestring1",
			Desc:             "point along a linestring",
			Expected:         []string{"multilinestring1"},
		},
		{
			QueryShape:       []float64{1.5, 1.6001714},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 1.0}, {2.0, 2.0}, {3.0, 3.0}}, {{1, 1.1}, {2, 2.1}, {3, 3.4}}},
			DocShapeName:     "multilinestring1",
			Desc:             "point outside all linestrings",
			Expected:         []string{"multilinestring1"},
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "multilinestring",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("disjoint",
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestPointPolygonDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       []float64{3.0, 3.0},
			DocShapeVertices: [][][]float64{{{2.0, 2.0}, {3.0, 3.0}, {1.0, 3.0}, {2.0, 2.0}}},
			DocShapeName:     "polygon1",
			Desc:             "point on polygon vertex",
			Expected:         nil,
		},
		{
			QueryShape:       []float64{1.5, 1.500714},
			DocShapeVertices: [][][]float64{{{1.0, 1.0}, {2.0, 2.0}, {0.0, 2.0}, {1.0, 1.0}}},
			DocShapeName:     "polygon1",
			Desc:             "point on polygon edge",
			Expected:         nil,
		},
		{
			QueryShape:       []float64{1.5, 1.9},
			DocShapeVertices: [][][]float64{{{1.0, 1.0}, {2.0, 2.0}, {0.0, 2.0}, {1.0, 1.0}}},
			DocShapeName:     "polygon1",
			Desc:             "point inside polygon",
			Expected:         nil,
		},
		{
			QueryShape: []float64{0.3, 0.3},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}},
				{{0.2, 0.2}, {0.2, 0.4}, {0.4, 0.4}, {0.4, 0.2}, {0.2, 0.2}}},
			DocShapeName: "polygon1",
			Desc:         "point inside hole inside polygon",
			Expected:     []string{"polygon1"},
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "polygon",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("disjoint",
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestPointMultiPolygonDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       []float64{3.0, 3.0},
			DocShapeVertices: [][][][]float64{{{{2.0, 2.0}, {3.0, 3.0}, {1.0, 3.0}, {2.0, 2.0}}}},
			DocShapeName:     "multipolygon1",
			Desc:             "point on a polygon vertex",
			Expected:         nil,
		},
		{
			QueryShape:       []float64{1.5, 1.500714},
			DocShapeVertices: [][][][]float64{{{{1.0, 1.0}, {2.0, 2.0}, {0.0, 2.0}, {1.0, 1.0}}}},
			DocShapeName:     "multipolygon1",
			Desc:             "point on polygon edge",
			Expected:         nil,
		},
		{
			QueryShape: []float64{1.5, 1.9},
			DocShapeVertices: [][][][]float64{{{{1.0, 1.0}, {2.0, 2.0}, {0.0, 2.0}, {1.0, 1.0}}},
				{{{1.5, 1.9}, {2.5, 2.9}, {0.5, 2.9}, {1.5, 1.9}}}},
			DocShapeName: "multipolygon1",
			Desc:         "point inside a polygon and on vertex of another polygon",
			Expected:     nil,
		},
		{
			QueryShape: []float64{0.3, 0.3},
			DocShapeVertices: [][][][]float64{{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}},
				{{0.2, 0.2}, {0.2, 0.4}, {0.4, 0.4}, {0.4, 0.2}, {0.2, 0.2}}}},
			DocShapeName: "multipolygon1",
			Desc:         "point inside hole inside one of the polygons",
			Expected:     []string{"multipolygon1"},
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "multipolygon", test.DocShapeVertices, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("disjoint",
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestEnvelopePointDisjoint(t *testing.T) {
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
			DocShapeVertices: rightRectPoint,
			DocShapeName:     "point1",
			Desc:             "point on vertex of bounded rectangle",
			Expected:         nil,
			QueryType:        "disjoint",
		},
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: []float64{10, 10},
			DocShapeName:     "point1",
			Desc:             "point outside bounded rectangle",
			Expected:         []string{"point1"},
			QueryType:        "disjoint",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "point",
			[][][][]float64{{{test.DocShapeVertices}}}, i)
		if err != nil {
			t.Errorf(err.Error())
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
			t.Errorf(err.Error())
		}
	}
}

func TestEnvelopeLinestringDisjoint(t *testing.T) {
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
			DocShapeVertices: [][]float64{{0.25, 0.25}, {0.5, 0.5}},
			DocShapeName:     "linestring1",
			Desc:             "linestring completely in bounded rectangle",
			Expected:         nil,
			QueryType:        "disjoint",
		},
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: [][]float64{{2.5, 2.5}, {4.5, 4.5}},
			DocShapeName:     "linestring1",
			Desc:             "linestring outside bounded rectangle",
			Expected:         []string{"linestring1"},
			QueryType:        "disjoint",
		},
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: [][]float64{{0.25, 0.25}, {4.5, 4.5}},
			DocShapeName:     "linestring1",
			Desc:             "linestring partially in bounded rectangle",
			Expected:         nil,
			QueryType:        "disjoint",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "linestring",
			[][][][]float64{{test.DocShapeVertices}}, i)
		if err != nil {
			t.Errorf(err.Error())
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
			t.Errorf(err.Error())
		}
	}
}

func TestEnvelopePolygonDisjoint(t *testing.T) {
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
			DocShapeVertices: [][][]float64{{{0.5, 0.5}, {1.5, 0.5}, {1.5, 1.5}, {0.5, 1.5}, {0.5, 0.5}}},
			DocShapeName:     "polygon1",
			Desc:             "polygon intersects bounded rectangle",
			Expected:         nil,
			QueryType:        "disjoint",
		},
		{
			QueryShape:       [][]float64{{0, 1}, {1, 0}},
			DocShapeVertices: [][][]float64{{{10.5, 10.5}, {11.5, 10.5}, {11.5, 11.5}, {10.5, 11.5}, {10.5, 10.5}}},
			DocShapeName:     "polygon1",
			Desc:             "polygon completely outside bounded rectangle",
			Expected:         []string{"polygon1"},
			QueryType:        "disjoint",
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "polygon",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Errorf(err.Error())
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
			t.Errorf(err.Error())
		}
	}
}

func TestMultiPointDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][]float64{{3.0, 3.0}, {4.0, 4.0}},
			DocShapeVertices: [][]float64{{4.0, 4.0}},
			DocShapeName:     "multipoint1",
			Desc:             "single coincident multipoint",
			Expected:         nil,
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "multipoint",
			[][][][]float64{{test.DocShapeVertices}}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("disjoint",
				true, indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Errorf(err.Error())
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipoint: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestLinestringDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][]float64{{3.0, 2.0}, {4.0, 2.0}},
			DocShapeVertices: [][]float64{{3.0, 2.0}, {4.0, 2.0}},
			DocShapeName:     "linestring1",
			Desc:             "coincident linestrings",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {1.5, 1.5}, {2.0, 2.0}},
			DocShapeVertices: [][]float64{{2.0, 2.0}, {4.0, 3.0}},
			DocShapeName:     "linestring1",
			Desc:             "linestrings intersecting at the ends",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {3.0, 3.0}},
			DocShapeVertices: [][]float64{{1.5499860, 1.5501575}, {4.0, 6.0}},
			DocShapeName:     "linestring1",
			Desc:             "subline not at vertex",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {2.0, 2.0}},
			DocShapeVertices: [][]float64{{1.5499860, 1.5501575}, {1.5, 1.5001714}},
			DocShapeName:     "linestring1",
			Desc:             "subline inside linestring",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {1.5, 1.5}, {2.0, 2.0}},
			DocShapeVertices: [][]float64{{1.0, 2.0}, {2.0, 1.0}},
			DocShapeName:     "linestring1",
			Desc:             "linestrings intersecting at some edge",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {1.5, 1.5}, {2.0, 2.0}},
			DocShapeVertices: [][]float64{{1.0, 2.0}, {1.0, 4.0}},
			DocShapeName:     "linestring1",
			Desc:             "non intersecting linestrings",
			Expected:         []string{"linestring1"},
		},
		{
			QueryShape:       [][]float64{{59.32, 0.52}, {68.99, -7.36}, {75.49, -12.21}},
			DocShapeVertices: [][]float64{{71.98, 0}, {67.58, -6.57}, {63.19, -12.72}},
			DocShapeName:     "linestring1",
			Desc:             "linestrings with more than 2 points intersecting at some edges",
			Expected:         nil,
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "linestring",
			[][][][]float64{{test.DocShapeVertices}}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeLinestringQueryWithRelation("disjoint",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for linestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestLinestringPolygonDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][]float64{{1.0, 1.0}, {1.5, 1.5}, {2.0, 2.0}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeName:     "polygon1",
			Desc:             "linestring intersects polygon at a vertex",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{0.2, 0.2}, {0.4, 0.4}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeName:     "polygon1",
			Desc:             "linestring within polygon",
			Expected:         []string{"polygon1"},
		},
		{
			QueryShape:       [][]float64{{-0.5, 0.5}, {0.5, 0.5}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeName:     "polygon1",
			Desc:             "linestring intersects polygon at an edge",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{-0.5, 0.5}, {1.5, 0.5}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeName:     "polygon1",
			Desc:             "linestring intersects polygon as a whole",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{-0.5, 0.5}, {-1.5, -1.5}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}}},
			DocShapeName:     "polygon1",
			Desc:             "linestring does not intersect polygon",
			Expected:         []string{"polygon1"},
		},
		{
			QueryShape: [][]float64{{0.3, 0.3}, {0.35, 0.35}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}},
				{{0.2, 0.2}, {0.2, 0.4}, {0.4, 0.4}, {0.4, 0.2}, {0.2, 0.2}}},
			DocShapeName: "polygon1",
			Desc:         "linestring does not intersect polygon when contained in the hole",
			Expected:     []string{"polygon1"},
		},
		{
			QueryShape: [][]float64{{0.3, 0.3}, {0.5, 0.5}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}},
				{{0.2, 0.2}, {0.2, 0.4}, {0.4, 0.4}, {0.4, 0.2}, {0.2, 0.2}}},
			DocShapeName: "polygon1",
			Desc:         "linestring intersects polygon in the hole",
			Expected:     nil,
		},
		{
			QueryShape: [][]float64{{0.4, 0.3}, {0.6, 0.3}},
			DocShapeVertices: [][][]float64{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0}, {0.0, 1.0}, {0.0, 0.0}},
				{{0.3, 0.3}, {0.4, 0.2}, {0.5, 0.3}, {0.4, 0.4}, {0.3, 0.3}},
				{{0.5, 0.3}, {0.6, 0.2}, {0.7, 0.3}, {0.6, 0.4}, {0.5, 0.3}}},
			DocShapeName: "polygon1",
			Desc:         "linestring intersects polygon through touching holes",
			Expected:     nil,
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "polygon",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeLinestringQueryWithRelation("disjoint",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for linestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestLinestringPointDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       [][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][]float64{{179, 0}, {-179, 0}},
			DocShapeVertices: []float64{179.1, 0},
			DocShapeName:     "point1",
			Desc:             "point across longitudinal boundary of linestring",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{-179, 0}, {179, 0}},
			DocShapeVertices: []float64{179.1, 0},
			DocShapeName:     "point1",
			Desc:             "point across longitudinal boundary of reversed linestring",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{179, 0}, {-179, 0}},
			DocShapeVertices: []float64{170, 0},
			DocShapeName:     "point1",
			Desc:             "point does not intersect linestring",
			Expected:         []string{"point1"},
		},
		{
			QueryShape:       [][]float64{{-179, 0}, {179, 0}},
			DocShapeVertices: []float64{170, 0},
			DocShapeName:     "point1",
			Desc:             "point does not intersect reversed linestring",
			Expected:         []string{"point1"},
		},
		{
			QueryShape:       [][]float64{{-179, 0}, {179, 0}, {178, 0}},
			DocShapeVertices: []float64{178, 0},
			DocShapeName:     "point1",
			Desc:             "point intersects linestring at end vertex",
			Expected:         nil,
		},
		{
			QueryShape:       [][]float64{{-179, 0}, {179, 0}, {178, 0}, {180, 0}},
			DocShapeVertices: []float64{178, 0},
			DocShapeName:     "point1",
			Desc:             "point intersects linestring with more than two points",
			Expected:         nil,
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "point",
			[][][][]float64{{{test.DocShapeVertices}}}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeLinestringQueryWithRelation("disjoint",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for linestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestMultiLinestringDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][][]float64{{{1.0, 1.0}, {1.1, 1.1}, {2.0, 2.0}, {2.1, 2.1}}},
			DocShapeVertices: [][][]float64{{{0.0, 0.5132}, {-1.1, -1.1}, {1.5, 1.512}, {2.1, 2.1}}},
			DocShapeName:     "multilinestring1",
			Desc:             "intersecting multilinestrings",
			Expected:         nil,
		},
		{
			QueryShape:       [][][]float64{{{1.0, 1.0}, {1.1, 1.1}, {2.0, 2.0}, {2.1, 2.1}}},
			DocShapeVertices: [][][]float64{{{10.1, 10.5}, {11.5, 12.5}}},
			DocShapeName:     "multilinestring1",
			Desc:             "non-intersecting multilinestrings",
			Expected:         []string{"multilinestring1"},
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "multilinestring",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiLinestringQueryWithRelation("disjoint",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multilinestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestMultiLinestringMultiPointDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][][]float64{{{2.0, 2.0}, {2.1, 2.1}}, {{3.0, 3.0}, {3.1, 3.1}}},
			DocShapeVertices: [][]float64{{5.0, 6.0}, {67, 67}, {3.1, 3.1}},
			DocShapeName:     "multipoint1",
			Desc:             "multilinestring intersects one of the multipoints",
			Expected:         nil,
		},
		{
			// check this?
			QueryShape:       [][][]float64{{{2.0, 2.0}, {2.1, 2.1}}, {{3.0, 3.0}, {3.1, 3.1}}},
			DocShapeVertices: [][]float64{{56.0, 56.0}, {66, 66}},
			DocShapeName:     "multipoint1",
			Desc:             "multilinestring does not intersect any of the multipoints",
			Expected:         nil,
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "multipoint",
			[][][][]float64{{test.DocShapeVertices}}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiLinestringQueryWithRelation("disjoint",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multilinestring: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestPolygonDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape: [][][]float64{{{1.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.0, 1.0}}},
			DocShapeVertices: [][][]float64{{{1.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.0, 1.0}}},
			DocShapeName: "polygon1",
			Desc:         "coincident polygons",
			Expected:     nil,
		},
		{
			QueryShape: [][][]float64{{{1.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.0, 1.0}}},
			DocShapeVertices: [][][]float64{{{1.2, 1.2}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.2, 1.2}}},
			DocShapeName: "polygon1",
			Desc:         "polygon and a window polygon",
			Expected:     nil,
		},
		{
			QueryShape: [][][]float64{{{1.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.0, 1.0}}},
			DocShapeVertices: [][][]float64{{{1.1, 1.1}, {1.2, 1.1}, {1.2, 1.2},
				{1.1, 1.2}, {1.1, 1.1}}},
			DocShapeName: "polygon1",
			Desc:         "nested polygons",
			Expected:     nil,
		},
		{
			QueryShape: [][][]float64{{{1.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.0, 1.0}}},
			DocShapeVertices: [][][]float64{{{0.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{0.0, 2.0}, {0.0, 1.0}}},
			DocShapeName: "polygon1",
			Desc:         "intersecting polygons",
			Expected:     nil,
		},
		{
			QueryShape: [][][]float64{{{0, 0}, {5, 0}, {5, 5}, {0, 5}, {0, 0}}, {{1, 4}, {4, 4},
				{4, 1}, {1, 1}, {1, 4}}},
			DocShapeVertices: [][][]float64{{{2, 2}, {3, 2}, {3, 3}, {2, 3}, {2, 2}}},
			DocShapeName:     "polygon1",
			Desc:             "polygon inside hole of a larger polygon",
			Expected:         []string{"polygon1"},
		},
		{
			QueryShape: [][][]float64{{{1.0, 1.0}, {2.0, 1.0}, {2.0, 2.0},
				{1.0, 2.0}, {1.0, 1.0}}},
			DocShapeVertices: [][][]float64{{{3.0, 3.0}, {4.0, 3.0}, {4.0, 4.0},
				{3.0, 4.0}, {3.0, 3.0}}},
			DocShapeName: "polygon1",
			Desc:         "disjoint polygons",
			Expected:     []string{"polygon1"},
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "polygon",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation("disjoint",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestPolygonPointDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       [][][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][][]float64{{{150, 85}, {160, 85}, {-20, 85}, {-30, 85}, {150, 85}}},
			DocShapeVertices: []float64{150, 88},
			DocShapeName:     "point1",
			Desc:             "polygon intersects point in latitudinal boundary",
			Expected:         nil,
		},
		{
			QueryShape:       [][][]float64{{{150, 85}, {160, 85}, {-20, 85}, {-30, 85}, {150, 85}}},
			DocShapeVertices: []float64{170, 88},
			DocShapeName:     "point1",
			Desc:             "polygon does not intersects point outside latitudinal boundary",
			Expected:         []string{"point1"},
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "point",
			[][][][]float64{{{test.DocShapeVertices}}}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePolygonQueryWithRelation("disjoint",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for polygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestMultiPolygonDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][]float64
		DocShapeVertices [][][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape: [][][][]float64{{{{15, 5}, {40, 10}, {10, 20},
				{5, 10}, {15, 5}}}, {{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][][][]float64{{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0},
				{0.0, 1.0}, {0.0, 0.0}}, {{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeName: "multipolygon1",
			Desc:         "intersecting multi polygons",
			Expected:     nil,
		},
		{
			QueryShape: [][][][]float64{{{{15, 5}, {40, 10}, {10, 20},
				{5, 10}, {15, 5}}}, {{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][][][]float64{{{{0.0, 0.0}, {1.0, 0.0}, {1.0, 1.0},
				{0.0, 1.0}, {0.0, 0.0}}}},
			DocShapeName: "multipolygon1",
			Desc:         "non intersecting multi polygons",
			Expected:     []string{"multipolygon1"},
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "multipolygon",
			test.DocShapeVertices, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiPolygonQueryWithRelation("disjoint",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipolygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestMultiPolygonMultiPointDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][]float64
		DocShapeVertices [][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape: [][][][]float64{{{{30, 20}, {45, 40}, {10, 40}, {30, 20}}},
				{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}}},
			DocShapeVertices: [][]float64{{30, 20}, {30, 30}},
			DocShapeName:     "multipoint1",
			Desc:             "multipolygon intersects multipoint at the vertex",
			Expected:         nil,
		},
		{
			// check this
			QueryShape: [][][][]float64{{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}},
				{{{30, 20}, {45, 50}, {10, 50}, {30, 20}}}},
			DocShapeVertices: [][]float64{{30, -20}, {-30, 30}, {45, 66}},
			DocShapeName:     "multipoint1",
			Desc:             "multipolygon does not intersect multipoint",
			Expected:         nil, // should not be nil
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "multipoint",
			[][][][]float64{{test.DocShapeVertices}}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiPolygonQueryWithRelation("disjoint",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipolygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})

		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestMultiPolygonMultiLinestringDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
	}{
		{
			QueryShape:       [][][][]float64{{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}}, {{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][][]float64{{{65, 40}, {60, 40}}, {{45, 40}, {10, 40}, {30, 20}}},
			DocShapeName:     "multilinestring1",
			Desc:             "multipolygon intersects multilinestring",
			Expected:         nil,
		},
		{
			QueryShape:       [][][][]float64{{{{15, 5}, {40, 10}, {10, 20}, {5, 10}, {15, 5}}}, {{{30, 20}, {45, 40}, {10, 40}, {30, 20}}}},
			DocShapeVertices: [][][]float64{{{45, 41}, {60, 80}}, {{-45, -40}, {-10, -40}}},
			DocShapeName:     "multilinestring1",
			Desc:             "multipolygon does not intersect multilinestring",
			Expected:         []string{"multilinestring1"},
		},
	}
	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "multilinestring",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeMultiPolygonQueryWithRelation("disjoint",
				indexReader, test.QueryShape, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for multipolygon: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestGeometryCollectionPointDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][][]float64
		DocShapeVertices []float64
		DocShapeName     string
		Desc             string
		Expected         []string
		Types            []string
	}{
		{
			QueryShape:       [][][][][]float64{{{{{4, 5}}}}},
			DocShapeVertices: []float64{4, 5},
			DocShapeName:     "point1",
			Desc:             "point coincident with point in geometry collection",
			Expected:         nil,
			Types:            []string{"point"},
		},
		{
			QueryShape:       [][][][][]float64{{{{{4, 5}, {6, 7}}}}},
			DocShapeVertices: []float64{4, 5},
			DocShapeName:     "point1",
			Desc:             "point on vertex of linestring in geometry collection",
			Expected:         nil,
			Types:            []string{"linestring"},
		},
		{
			QueryShape:       [][][][][]float64{{{{{1, 1}, {2, 2}, {0, 2}, {1, 0}}, {{5, 6}}}}},
			DocShapeVertices: []float64{1.5, 1.9},
			DocShapeName:     "point1",
			Desc:             "point inside polygon in geometry collection",
			Expected:         nil,
			Types:            []string{"polygon", "point"},
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "point",
			[][][][]float64{{{test.DocShapeVertices}}}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeGeometryCollectionRelationQuery("disjoint",
				indexReader, test.QueryShape, test.Types, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for geometry collection: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestGeometryCollectionPolygonDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       [][][][][]float64
		DocShapeVertices [][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
		Types            []string
	}{
		{
			QueryShape:       [][][][][]float64{{{{{4, 5}, {6, 7}, {7, 8}, {4, 5}}}, {{{1, 2}, {2, 3}, {3, 4}, {1, 2}}}}},
			DocShapeVertices: [][][]float64{{{4, 5}, {6, 7}, {7, 8}, {4, 5}}},
			DocShapeName:     "polygon1",
			Desc:             "polygon coincides with one of the polygons in multipolygon in geometry collection",
			Expected:         nil,
			Types:            []string{"multipolygon"},
		},
		{
			QueryShape:       [][][][][]float64{{{{{14, 15}}}}},
			DocShapeVertices: [][][]float64{{{4, 5}, {6, 7}, {7, 8}, {4, 5}}},
			DocShapeName:     "polygon1",
			Desc:             "polygon does not intersect point in geometry collection",
			Expected:         []string{"polygon1"},
			Types:            []string{"point"},
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetup(t, test.DocShapeName, "polygon",
			[][][][]float64{test.DocShapeVertices}, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapeGeometryCollectionRelationQuery("disjoint",
				indexReader, test.QueryShape, test.Types, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for geometry collection: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}

func TestPointGeometryCollectionDisjoint(t *testing.T) {
	tests := []struct {
		QueryShape       []float64
		DocShapeVertices [][][][][]float64
		DocShapeName     string
		Desc             string
		Expected         []string
		Types            []string
	}{
		{
			QueryShape:       []float64{1.0, 2.0},
			DocShapeVertices: [][][][][]float64{{{{{11.0, 12.0}, {13.0, 14.0}}}}},
			DocShapeName:     "geometrycollection1",
			Desc:             "geometry collection does not intersect with a point",
			Expected:         []string{"geometrycollection1"},
			Types:            []string{"linestring"},
		},
		{
			QueryShape:       []float64{1.0, 2.0},
			DocShapeVertices: [][][][][]float64{{{{{1.0, 2.0}}}}},
			DocShapeName:     "geometrycollection1",
			Desc:             "geometry collection intersects with a point",
			Expected:         nil,
			Types:            []string{"point"},
		},
	}

	i := setupIndex(t)

	for _, test := range tests {
		indexReader, closeFn, err := testCaseSetupGeometryCollection(t, test.DocShapeName, test.Types,
			test.DocShapeVertices, i)
		if err != nil {
			t.Errorf(err.Error())
		}

		t.Run(test.Desc, func(t *testing.T) {
			got, err := runGeoShapePointRelationQuery("disjoint",
				false, indexReader, [][]float64{test.QueryShape}, "geometry")
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, test.Expected) {
				t.Errorf("expected %v, got %v for point: %+v",
					test.Expected, got, test.QueryShape)
			}
		})
		err = closeFn()
		if err != nil {
			t.Errorf(err.Error())
		}
	}
}
