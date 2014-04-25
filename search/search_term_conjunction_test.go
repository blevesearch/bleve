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

func TestTermConjunctionSearch(t *testing.T) {

	tests := []struct {
		index   index.Index
		query   *TermConjunctionQuery
		results []*DocumentMatch
	}{
		{
			index: twoDocIndex,
			query: &TermConjunctionQuery{
				Terms: []Query{
					&TermQuery{
						Term:     "beer",
						Field:    "desc",
						BoostVal: 1.0,
						Explain:  true,
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
					Score: 2.0097428702814377,
				},
			},
		},
		{
			index: twoDocIndex,
			query: &TermConjunctionQuery{
				Terms: []Query{
					&TermQuery{
						Term:     "angst",
						Field:    "desc",
						BoostVal: 1.0,
						Explain:  true,
					},
					&TermQuery{
						Term:     "beer",
						Field:    "desc",
						BoostVal: 1.0,
						Explain:  true,
					},
				},
				Explain: true,
			},
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "2",
					Score: 1.0807601687084403,
				},
			},
		},
		{
			index: twoDocIndex,
			query: &TermConjunctionQuery{
				Terms: []Query{
					&TermQuery{
						Term:     "beer",
						Field:    "desc",
						BoostVal: 1.0,
						Explain:  true,
					},
					&TermQuery{
						Term:     "jack",
						Field:    "name",
						BoostVal: 5.0,
						Explain:  true,
					},
				},
				Explain: true,
			},
			results: []*DocumentMatch{},
		},
		{
			index: twoDocIndex,
			query: &TermConjunctionQuery{
				Terms: []Query{
					&TermQuery{
						Term:     "beer",
						Field:    "desc",
						BoostVal: 1.0,
						Explain:  true,
					},
					&TermQuery{
						Term:     "mister",
						Field:    "title",
						BoostVal: 5.0,
						Explain:  true,
					},
				},
				Explain: true,
			},
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "2",
					Score: 1.2877980334016337,
				},
				&DocumentMatch{
					ID:    "3",
					Score: 1.2877980334016337,
				},
			},
		},
		{
			index: twoDocIndex,
			query: &TermConjunctionQuery{
				Terms: []Query{
					&TermQuery{
						Term:     "couchbase",
						Field:    "street",
						BoostVal: 1.0,
						Explain:  true,
					},
					&TermQuery{
						Term:     "mister",
						Field:    "title",
						BoostVal: 5.0,
						Explain:  true,
					},
				},
				Explain: true,
			},
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "2",
					Score: 1.4436599157093672,
				},
			},
		},
		{
			index: twoDocIndex,
			query: &TermConjunctionQuery{
				Terms: []Query{
					&TermQuery{
						Term:     "beer",
						Field:    "desc",
						BoostVal: 5.0,
						Explain:  true,
					},
					&TermConjunctionQuery{
						Terms: []Query{
							&TermQuery{
								Term:     "couchbase",
								Field:    "street",
								BoostVal: 1.0,
								Explain:  true,
							},
							&TermQuery{
								Term:     "mister",
								Field:    "title",
								BoostVal: 5.0,
								Explain:  true,
							},
						},
						Explain: true,
					},
				},
				Explain: true,
			},
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "2",
					Score: 1.754501824953384,
				},
			},
		},
	}

	for testIndex, test := range tests {
		searcher, err := NewTermConjunctionSearcher(test.index, test.query)
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
