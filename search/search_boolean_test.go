//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import (
	"testing"

	"github.com/couchbaselabs/bleve/index"
)

func TestTermBooleanSearch(t *testing.T) {

	tests := []struct {
		index   index.Index
		query   Query
		results []*DocumentMatch
	}{
		{
			index: twoDocIndex,
			query: &TermBooleanQuery{
				Must: &TermConjunctionQuery{
					Terms: []Query{
						&TermQuery{
							Term:     "beer",
							Field:    "desc",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					Explain: true,
				},
				Should: &TermDisjunctionQuery{
					Terms: []Query{
						&TermQuery{
							Term:     "marty",
							Field:    "name",
							BoostVal: 1.0,
							Explain:  true,
						},
						&TermQuery{
							Term:     "dustin",
							Field:    "name",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					Explain: true,
					Min:     0,
				},
				MustNot: &TermDisjunctionQuery{
					Terms: []Query{
						&TermQuery{
							Term:     "steve",
							Field:    "name",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					Explain: true,
					Min:     0,
				},
				Explain: true,
			},
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "1",
					Score: 1.6775110856165738,
				},
				&DocumentMatch{
					ID:    "3",
					Score: 0.8506018914159408,
				},
				&DocumentMatch{
					ID:    "4",
					Score: 0.34618161159873423,
				},
			},
		},
		{
			index: twoDocIndex,
			query: &TermBooleanQuery{
				Should: &TermDisjunctionQuery{
					Terms: []Query{
						&TermQuery{
							Term:     "marty",
							Field:    "name",
							BoostVal: 1.0,
							Explain:  true,
						},
						&TermQuery{
							Term:     "dustin",
							Field:    "name",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					Explain: true,
					Min:     0,
				},
				MustNot: &TermDisjunctionQuery{
					Terms: []Query{
						&TermQuery{
							Term:     "steve",
							Field:    "name",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					Explain: true,
					Min:     0,
				},
				Explain: true,
			},
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "1",
					Score: 0.6775110856165737,
				},
				&DocumentMatch{
					ID:    "3",
					Score: 0.6775110856165737,
				},
			},
		},
		// no MUST or SHOULD clauses yields no results
		{
			index: twoDocIndex,
			query: &TermBooleanQuery{
				MustNot: &TermDisjunctionQuery{
					Terms: []Query{
						&TermQuery{
							Term:     "steve",
							Field:    "name",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					Explain: true,
					Min:     0,
				},
				Explain: true,
			},
			results: []*DocumentMatch{},
		},
		{
			index: twoDocIndex,
			query: &TermBooleanQuery{
				Must: &TermConjunctionQuery{
					Terms: []Query{
						&TermQuery{
							Term:     "beer",
							Field:    "desc",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					Explain: true,
				},
				MustNot: &TermDisjunctionQuery{
					Terms: []Query{
						&TermQuery{
							Term:     "steve",
							Field:    "name",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					Explain: true,
					Min:     0,
				},
				Explain: true,
			},
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "1",
					Score: 1.0,
				},
				&DocumentMatch{
					ID:    "3",
					Score: 0.5,
				},
				&DocumentMatch{
					ID:    "4",
					Score: 1.0,
				},
			},
		},
		{
			index: twoDocIndex,
			query: &TermBooleanQuery{
				Must: &TermConjunctionQuery{
					Terms: []Query{
						&TermQuery{
							Term:     "beer",
							Field:    "desc",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					Explain: true,
				},
				MustNot: &TermDisjunctionQuery{
					Terms: []Query{
						&TermQuery{
							Term:     "steve",
							Field:    "name",
							BoostVal: 1.0,
							Explain:  true,
						},
						&TermQuery{
							Term:     "marty",
							Field:    "name",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					Explain: true,
					Min:     0,
				},
				Explain: true,
			},
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "3",
					Score: 0.5,
				},
				&DocumentMatch{
					ID:    "4",
					Score: 1.0,
				},
			},
		},
		{
			index: twoDocIndex,
			query: &TermBooleanQuery{
				Must: &TermConjunctionQuery{
					Terms: []Query{
						&TermQuery{
							Term:     "beer",
							Field:    "desc",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					Explain: true,
				},
				Should: &TermDisjunctionQuery{
					Terms: []Query{
						&TermQuery{
							Term:     "marty",
							Field:    "name",
							BoostVal: 1.0,
							Explain:  true,
						},
						&TermQuery{
							Term:     "dustin",
							Field:    "name",
							BoostVal: 1.0,
							Explain:  true,
						},
					},
					Explain: true,
					Min:     2,
				},
				Explain: true,
			},
			results: []*DocumentMatch{},
		},
		// test a conjunction query with a nested boolean
		{
			index: twoDocIndex,
			query: &TermConjunctionQuery{
				Terms: []Query{
					&TermBooleanQuery{
						Must: &TermConjunctionQuery{
							Terms: []Query{
								&TermQuery{
									Term:     "beer",
									Field:    "desc",
									BoostVal: 1.0,
									Explain:  true,
								},
							},
							Explain: true,
						},
						Explain: true,
					},
					&TermQuery{
						Term:     "marty",
						Field:    "name",
						BoostVal: 5.0,
						Explain:  true,
					},
				},
				Explain: true,
			},
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "1",
					Score: 2.905938399789078,
				},
			},
		},
	}

	for testIndex, test := range tests {
		searcher, err := test.query.Searcher(test.index)
		defer searcher.Close()

		next, err := searcher.Next()
		i := 0
		for err == nil && next != nil {
			if i < len(test.results) {
				if next.ID != test.results[i].ID {
					t.Errorf("expected result %d to have id %s got %s for test %d", i, test.results[i].ID, next.ID, testIndex)
				}
				if next.Score != test.results[i].Score {
					t.Errorf("expected result %d to have score %v got  %v for test %d", i, test.results[i].Score, next.Score, testIndex)
					t.Logf("scoring explanation: %s", next.Expl)
				}
			}
			next, err = searcher.Next()
			i++
		}
		if err != nil {
			t.Fatalf("error iterating searcher: %v for test %d", err, testIndex)
		}
		if len(test.results) != i {
			t.Errorf("expected %d results got %d for test %d", len(test.results), i, testIndex)
		}
	}
}
