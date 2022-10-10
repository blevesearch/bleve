//  Copyright (c) 2014 Couchbase, Inc.
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

package query

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/mapping"
)

var minNum = 5.1
var maxNum = 7.1
var minTerm = "bob"
var maxTerm = "cat"
var startDateStr = "2011-01-01T00:00:00Z"
var endDateStr = "2012-01-01T00:00:00Z"
var startDate time.Time
var endDate time.Time

func init() {
	var err error
	startDate, err = time.Parse(time.RFC3339, startDateStr)
	if err != nil {
		panic(err)
	}
	endDate, err = time.Parse(time.RFC3339, endDateStr)
	if err != nil {
		panic(err)
	}
}

func TestParseQuery(t *testing.T) {
	tests := []struct {
		input  []byte
		output Query
		err    bool
	}{
		{
			input: []byte(`{"term":"water","field":"desc"}`),
			output: func() Query {
				q := NewTermQuery("water")
				q.SetField("desc")
				return q
			}(),
		},
		{
			input: []byte(`{"match":"beer","field":"desc"}`),
			output: func() Query {
				q := NewMatchQuery("beer")
				q.SetField("desc")
				return q
			}(),
		},
		{
			input: []byte(`{"match":"beer","field":"desc","operator":"or"}`),
			output: func() Query {
				q := NewMatchQuery("beer")
				q.SetField("desc")
				return q
			}(),
		},
		{
			input: []byte(`{"match":"beer","field":"desc","operator":"and"}`),
			output: func() Query {
				q := NewMatchQuery("beer")
				q.SetOperator(MatchQueryOperatorAnd)
				q.SetField("desc")
				return q
			}(),
		},
		{
			input: []byte(`{"match":"beer","field":"desc","operator":"and"}`),
			output: func() Query {
				operator := MatchQueryOperatorAnd
				q := NewMatchQuery("beer")
				q.SetOperator(operator)
				q.SetField("desc")
				return q
			}(),
		},
		{
			input: []byte(`{"match":"beer","field":"desc","operator":"or"}`),
			output: func() Query {
				q := NewMatchQuery("beer")
				q.SetOperator(MatchQueryOperatorOr)
				q.SetField("desc")
				return q
			}(),
		},
		{
			input: []byte(`{"match":"beer","field":"desc","operator":"or"}`),
			output: func() Query {
				operator := MatchQueryOperatorOr
				q := NewMatchQuery("beer")
				q.SetOperator(operator)
				q.SetField("desc")
				return q
			}(),
		},
		{
			input:  []byte(`{"match":"beer","field":"desc","operator":"does not exist"}`),
			output: nil,
			err:    true,
		},
		{
			input: []byte(`{"match_phrase":"light beer","field":"desc"}`),
			output: func() Query {
				q := NewMatchPhraseQuery("light beer")
				q.SetField("desc")
				return q
			}(),
		},
		{
			input: []byte(`{"must":{"conjuncts": [{"match":"beer","field":"desc"}]},"should":{"disjuncts": [{"match":"water","field":"desc"}],"min":1.0},"must_not":{"disjuncts": [{"match":"devon","field":"desc"}]}}`),
			output: func() Query {
				q := NewBooleanQuery(
					[]Query{func() Query {
						q := NewMatchQuery("beer")
						q.SetField("desc")
						return q
					}()},
					[]Query{func() Query {
						q := NewMatchQuery("water")
						q.SetField("desc")
						return q
					}()},
					[]Query{func() Query {
						q := NewMatchQuery("devon")
						q.SetField("desc")
						return q
					}()})
				q.SetMinShould(1)
				return q
			}(),
		},
		{
			input:  []byte(`{"terms":["watered","down"],"field":"desc"}`),
			output: NewPhraseQuery([]string{"watered", "down"}, "desc"),
		},
		{
			input:  []byte(`{"query":"+beer \"light beer\" -devon"}`),
			output: NewQueryStringQuery(`+beer "light beer" -devon`),
		},
		{
			input: []byte(`{"min":5.1,"max":7.1,"field":"desc"}`),
			output: func() Query {
				q := NewNumericRangeQuery(&minNum, &maxNum)
				q.SetField("desc")
				return q
			}(),
		},
		{
			input: []byte(`{"min":"bob","max":"cat","field":"desc"}`),
			output: func() Query {
				q := NewTermRangeQuery(minTerm, maxTerm)
				q.SetField("desc")
				return q
			}(),
		},
		{
			input: []byte(`{"start":"` + startDateStr + `","end":"` + endDateStr + `","field":"desc"}`),
			output: func() Query {
				q := NewDateRangeQuery(startDate, endDate)
				q.SetField("desc")
				return q
			}(),
		},
		{
			input: []byte(`{"prefix":"budwei","field":"desc"}`),
			output: func() Query {
				q := NewPrefixQuery("budwei")
				q.SetField("desc")
				return q
			}(),
		},
		{
			input:  []byte(`{"match_all":{}}`),
			output: NewMatchAllQuery(),
		},
		{
			input:  []byte(`{"match_none":{}}`),
			output: NewMatchNoneQuery(),
		},
		{
			input:  []byte(`{"ids":["a","b","c"]}`),
			output: NewDocIDQuery([]string{"a", "b", "c"}),
		},
		{
			input:  []byte(`{"bool": true}`),
			output: NewBoolFieldQuery(true),
		},
		{
			input: []byte(`{"field": "x", "cidr": "1.2.3.0/4"}`),
			output: func() Query {
				q := NewIPRangeQuery("1.2.3.0/4")
				q.SetField("x")
				return q
			}(),
		},
		{
			input:  []byte(`{"madeitup":"queryhere"}`),
			output: nil,
			err:    true,
		},
	}

	for i, test := range tests {
		actual, err := ParseQuery(test.input)
		if err != nil && test.err == false {
			t.Errorf("error %v for %d", err, i)
		}

		if !reflect.DeepEqual(test.output, actual) {
			t.Errorf("expected: %#v, got: %#v for %s", test.output, actual, string(test.input))
		}
	}
}

func TestQueryValidate(t *testing.T) {
	tests := []struct {
		query Query
		err   bool
	}{
		{
			query: func() Query {
				q := NewTermQuery("water")
				q.SetField("desc")
				return q
			}(),
		},
		{
			query: func() Query {
				q := NewMatchQuery("beer")
				q.SetField("desc")
				return q
			}(),
		},
		{
			query: func() Query {
				q := NewMatchPhraseQuery("light beer")
				q.SetField("desc")
				return q
			}(),
		},
		{
			query: func() Query {
				q := NewNumericRangeQuery(&minNum, &maxNum)
				q.SetField("desc")
				return q
			}(),
		},
		{
			query: func() Query {
				q := NewNumericRangeQuery(nil, nil)
				q.SetField("desc")
				return q
			}(),
			err: true,
		},
		{
			query: func() Query {
				q := NewDateRangeQuery(startDate, endDate)
				q.SetField("desc")
				return q
			}(),
		},
		{
			query: func() Query {
				q := NewPrefixQuery("budwei")
				q.SetField("desc")
				return q
			}(),
		},
		{
			query: NewQueryStringQuery(`+beer "light beer" -devon`),
		},
		{
			query: NewPhraseQuery([]string{"watered", "down"}, "desc"),
		},
		{
			query: NewPhraseQuery([]string{}, "field"),
			err:   true,
		},
		{
			query: func() Query {
				q := NewMatchNoneQuery()
				q.SetBoost(25)
				return q
			}(),
		},
		{
			query: func() Query {
				q := NewMatchAllQuery()
				q.SetBoost(25)
				return q
			}(),
		},
		{
			query: NewBooleanQuery(
				[]Query{func() Query {
					q := NewMatchQuery("beer")
					q.SetField("desc")
					return q
				}()},
				[]Query{func() Query {
					q := NewMatchQuery("water")
					q.SetField("desc")
					return q
				}()},
				[]Query{func() Query {
					q := NewMatchQuery("devon")
					q.SetField("desc")
					return q
				}()}),
		},
		{
			query: NewBooleanQuery(
				nil,
				nil,
				[]Query{func() Query {
					q := NewMatchQuery("devon")
					q.SetField("desc")
					return q
				}()}),
		},
		{
			query: NewBooleanQuery(
				[]Query{},
				[]Query{},
				[]Query{func() Query {
					q := NewMatchQuery("devon")
					q.SetField("desc")
					return q
				}()}),
		},
		{
			query: NewBooleanQuery(
				nil,
				nil,
				nil),
			err: true,
		},
		{
			query: NewBooleanQuery(
				[]Query{},
				[]Query{},
				[]Query{}),
			err: true,
		},
		{
			query: func() Query {
				q := NewBooleanQuery(
					[]Query{func() Query {
						q := NewMatchQuery("beer")
						q.SetField("desc")
						return q
					}()},
					[]Query{func() Query {
						q := NewMatchQuery("water")
						q.SetField("desc")
						return q
					}()},
					[]Query{func() Query {
						q := NewMatchQuery("devon")
						q.SetField("desc")
						return q
					}()})
				q.SetMinShould(2)
				return q
			}(),
			err: true,
		},
		{
			query: func() Query {
				q := NewDocIDQuery(nil)
				q.SetBoost(25)
				return q
			}(),
		},
	}

	for _, test := range tests {
		if vq, ok := test.query.(ValidatableQuery); ok {
			actual := vq.Validate()
			if actual != nil && !test.err {
				t.Errorf("expected no error: %#v got %#v", test.err, actual)
			} else if actual == nil && test.err {
				t.Errorf("expected error: %#v got %#v", test.err, actual)
			}
		}
	}
}

func TestDumpQuery(t *testing.T) {
	mapping := mapping.NewIndexMapping()
	q := NewQueryStringQuery("+water -light beer")
	s, err := DumpQuery(mapping, q)
	if err != nil {
		t.Fatal(err)
	}
	s = strings.TrimSpace(s)
	wanted := strings.TrimSpace(`{
  "must": {
    "conjuncts": [
      {
        "match": "water",
        "prefix_length": 0,
        "fuzziness": 0
      }
    ]
  },
  "should": {
    "disjuncts": [
      {
        "match": "beer",
        "prefix_length": 0,
        "fuzziness": 0
      }
    ],
    "min": 0
  },
  "must_not": {
    "disjuncts": [
      {
        "match": "light",
        "prefix_length": 0,
        "fuzziness": 0
      }
    ],
    "min": 0
  }
}`)
	if wanted != s {
		t.Fatalf("query:\n%s\ndiffers from expected:\n%s", s, wanted)
	}
}

func TestGeoShapeQuery(t *testing.T) {
	tests := []struct {
		input  []byte
		output Query
		err    bool
	}{
		{
			input: []byte(`{
				"field" : "region",
				 "geometry": {
					 "shape": {
						 "type": "polygon",
						"coordinates": [[
							[
								74.1357421875,
								30.600093873550072
							],
							[
								67.0166015625,
								21.57571893245848
							],
							[
								68.8623046875,
								9.145486056167277
							],
							[
								83.1884765625,
								4.083452772038619
							],
							[
								88.9892578125,
								22.67484735118852
							],
							[
								74.1357421875,
								30.600093873550072
							]]]
						},
					  "relation": "intersects"
			  }}`),
			output: func() Query {
				q, _ := NewGeoShapeQuery([][][][]float64{{{{74.1357421875, 30.600093873550072},
					{67.0166015625, 21.57571893245848}, {68.8623046875, 9.145486056167277},
					{83.1884765625, 4.083452772038619}, {88.9892578125, 22.67484735118852},
					{74.1357421875, 30.600093873550072}}}}, geo.PolygonType, "intersects")
				q.SetField("region")
				return q
			}(),
		},
		{
			input: []byte(`{
				"field" : "region",
				 "geometry": {
					 "shape": {
						 "type": "multipolygon",
						 "coordinates": [
							 [[
								[
									77.58268117904663,
									12.980513152175025
								],
								[
									77.58147954940794,
									12.977983107483992
								],
								[
									77.58708000183104,
									12.97886130773254
								],
								[
									77.58268117904663,
									12.980513152175025
								]
							]],
							[[
								[
									77.5864577293396,
									12.97762764459667
								],
								[
									77.58879661560059,
									12.975076660730531
								],
								[
									77.59115695953369,
									12.979216768855913
								],
								[
									77.5864577293396,
									12.97762764459667
								]
							]]
						]
					},
					"relation": "contains"
			  }}`),
			output: func() Query {
				q, _ := NewGeoShapeQuery([][][][]float64{
					{{{77.58268117904663, 12.980513152175025},
						{77.58147954940794, 12.977983107483992}, {77.58708000183104, 12.97886130773254},
						{77.58268117904663, 12.980513152175025}}},
					{{{77.5864577293396, 12.97762764459667}, {77.58879661560059, 12.975076660730531},
						{77.59115695953369, 12.979216768855913}, {77.5864577293396, 12.97762764459667}}}},
					geo.MultiPolygonType, "contains")
				q.SetField("region")
				return q
			}(),
		},
		{
			input: []byte(`{
				"field" : "region",
				 "geometry": {
					 "shape": {
						 "type": "point",
						 "coordinates": [77.58268117904663, 12.980513152175025]
					},
					"relation": "contains"
			  }}`),
			output: func() Query {
				q, _ := NewGeoShapeQuery([][][][]float64{
					{{{77.58268117904663, 12.980513152175025}}}},
					geo.PointType, "contains")
				q.SetField("region")
				return q
			}(),
		},
		{
			input: []byte(`{
				"field" : "region",
				 "geometry": {
					 "shape": {
						 "type": "multipoint",
						 "coordinates": [[77.58268117904663, 12.980513152175025],
						 [77.5864577293396, 12.97762764459667]]
					},
					"relation": "intersects"
			  }}`),
			output: func() Query {
				q, _ := NewGeoShapeQuery([][][][]float64{
					{{{77.58268117904663, 12.980513152175025},
						{77.5864577293396, 12.97762764459667}}}},
					geo.MultiPointType, "intersects")
				q.SetField("region")
				return q
			}(),
		},
		{
			input: []byte(`{
				"field" : "region",
				 "geometry": {
					 "shape": {
						 "type": "linestring",
						 "coordinates": [[77.58268117904663, 12.980513152175025],
						 [77.5864577293396, 12.97762764459667]]
					},
					"relation": "intersects"
			  }}`),
			output: func() Query {
				q, _ := NewGeoShapeQuery([][][][]float64{
					{{{77.58268117904663, 12.980513152175025},
						{77.5864577293396, 12.97762764459667}}}},
					geo.LineStringType, "intersects")
				q.SetField("region")
				return q
			}(),
		},
		{
			input: []byte(`{
				"field" : "region",
				 "geometry": {
					 "shape": {
						 "type": "multilinestring",
						 "coordinates": [
						[[77.58268117904663, 12.980513152175025],
						 [77.5864577293396, 12.97762764459667]],
						 [[77.5864577293396,12.97762764459667],
						 [77.58879661560059, 12.975076660730531]]]
					},
					"relation": "intersects"
			  }}`),
			output: func() Query {
				q, _ := NewGeoShapeQuery([][][][]float64{{
					{{77.58268117904663, 12.980513152175025},
						{77.5864577293396, 12.97762764459667}},
					{{77.5864577293396, 12.97762764459667},
						{77.58879661560059, 12.975076660730531}}}},
					geo.MultiLineStringType, "intersects")

				q.SetField("region")
				return q
			}(),
		},
		{
			input: []byte(`{
				"field" : "region",
				 "geometry": {
					 "shape": {
						 "type": "envelope",
						 "coordinates": [[77.58268117904663, 12.980513152175025],
						 [77.5864577293396, 12.97762764459667]]
					},
					"relation": "within"
			  }}`),
			output: func() Query {
				q, _ := NewGeoShapeQuery([][][][]float64{{
					{{77.58268117904663, 12.980513152175025},
						{77.5864577293396, 12.97762764459667}}}},
					geo.EnvelopeType, "within")

				q.SetField("region")
				return q
			}(),
		},
		{
			input: []byte(`{
				"field" : "region",
				 "geometry": {
					 "shape": {
						 "type": "circle",
						 "coordinates": [77.58268117904663, 12.980513152175025],
						 "radius": "100m"
					},
					"relation": "within"
			  }}`),
			output: func() Query {
				q, _ := NewGeoShapeCircleQuery([]float64{
					77.58268117904663, 12.980513152175025},
					"100m", "within")

				q.SetField("region")
				return q
			}(),
		},
		{
			input: []byte(`{
				"field" : "region",
				"geometry": {
					"shape": {
					  "type": "geometrycollection",
					  "geometries": [
						{
						  "type": "point",
						  "coordinates": [
							77.59158611297607,
							12.972002899506203
						  ]
						},
						{
						  "type": "linestring",
						  "coordinates": [
							[
							  77.58851766586304,
							  12.973152950670608
							],
							[
							  77.58937597274779,
							  12.972212000113458
							]
						  ]
						},
						{
						  "type": "polygon",
						  "coordinates": [
							[
							  [
								77.59055614471436,
								12.974721193688106
							  ],
							  [
								77.58954763412476,
								12.97350841995465
							  ],
							  [
								77.59141445159912,
								12.973382960265356
							  ],
							  [
								77.59055614471436,
								12.974721193688106
							  ]
							]
						  ]
						}
					  ]
					},
					"relation": "contains"
				  }}`),
			output: func() Query {
				q, _ := NewGeometryCollectionQuery([][][][][]float64{
					{{{{77.59158611297607, 12.972002899506203}}}},
					{{{{77.58851766586304, 12.973152950670608}, {77.58937597274779, 12.972212000113458}}}},
					{{{{77.59055614471436, 12.974721193688106}, {77.58954763412476, 12.97350841995465},
						{77.59141445159912, 12.973382960265356}, {77.59055614471436, 12.974721193688106}}}},
				},
					[]string{"point", "linestring", "polygon"}, "contains")
				q.SetField("region")
				return q
			}(),
		},
	}

	for i, test := range tests {
		actual, err := ParseQuery(test.input)
		if err != nil && test.err == false {
			t.Errorf("error %v for %d", err, i)
		}

		if !reflect.DeepEqual(test.output, actual) {
			t.Errorf("expected: %#v, got: %#v for %s", test.output, actual, string(test.input))
		}
	}
}
