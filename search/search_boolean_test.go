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
)

func TestTermBooleanSearch(t *testing.T) {

	// test 0
	beerTermSearcher, err := NewTermSearcher(twoDocIndex, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustSearcher, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{beerTermSearcher}, true)
	if err != nil {
		t.Fatal(err)
	}
	martyTermSearcher, err := NewTermSearcher(twoDocIndex, "marty", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher, err := NewTermSearcher(twoDocIndex, "dustin", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	shouldSearcher, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{martyTermSearcher, dustinTermSearcher}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	steveTermSearcher, err := NewTermSearcher(twoDocIndex, "steve", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustNotSearcher, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{steveTermSearcher}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher, err := NewTermBooleanSearcher(twoDocIndex, mustSearcher, shouldSearcher, mustNotSearcher, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 1
	martyTermSearcher2, err := NewTermSearcher(twoDocIndex, "marty", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher2, err := NewTermSearcher(twoDocIndex, "dustin", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	shouldSearcher2, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{martyTermSearcher2, dustinTermSearcher2}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	steveTermSearcher2, err := NewTermSearcher(twoDocIndex, "steve", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustNotSearcher2, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{steveTermSearcher2}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher2, err := NewTermBooleanSearcher(twoDocIndex, nil, shouldSearcher2, mustNotSearcher2, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 2
	steveTermSearcher3, err := NewTermSearcher(twoDocIndex, "steve", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustNotSearcher3, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{steveTermSearcher3}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher3, err := NewTermBooleanSearcher(twoDocIndex, nil, nil, mustNotSearcher3, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 3
	beerTermSearcher4, err := NewTermSearcher(twoDocIndex, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustSearcher4, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{beerTermSearcher4}, true)
	if err != nil {
		t.Fatal(err)
	}
	steveTermSearcher4, err := NewTermSearcher(twoDocIndex, "steve", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustNotSearcher4, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{steveTermSearcher4}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher4, err := NewTermBooleanSearcher(twoDocIndex, mustSearcher4, nil, mustNotSearcher4, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 4
	beerTermSearcher5, err := NewTermSearcher(twoDocIndex, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustSearcher5, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{beerTermSearcher5}, true)
	if err != nil {
		t.Fatal(err)
	}
	steveTermSearcher5, err := NewTermSearcher(twoDocIndex, "steve", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	martyTermSearcher5, err := NewTermSearcher(twoDocIndex, "marty", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustNotSearcher5, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{steveTermSearcher5, martyTermSearcher5}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher5, err := NewTermBooleanSearcher(twoDocIndex, mustSearcher5, nil, mustNotSearcher5, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 5
	beerTermSearcher6, err := NewTermSearcher(twoDocIndex, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustSearcher6, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{beerTermSearcher6}, true)
	if err != nil {
		t.Fatal(err)
	}
	martyTermSearcher6, err := NewTermSearcher(twoDocIndex, "marty", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher6, err := NewTermSearcher(twoDocIndex, "dustin", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	shouldSearcher6, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{martyTermSearcher6, dustinTermSearcher6}, 2, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher6, err := NewTermBooleanSearcher(twoDocIndex, mustSearcher6, shouldSearcher6, nil, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 6
	beerTermSearcher7, err := NewTermSearcher(twoDocIndex, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustSearcher7, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{beerTermSearcher7}, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher7, err := NewTermBooleanSearcher(twoDocIndex, mustSearcher7, nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	martyTermSearcher7, err := NewTermSearcher(twoDocIndex, "marty", "name", 5.0, true)
	if err != nil {
		t.Fatal(err)
	}
	conjunctionSearcher7, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{martyTermSearcher7, booleanSearcher7}, true)

	// test 7
	beerTermSearcher8, err := NewTermSearcher(twoDocIndex, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustSearcher8, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{beerTermSearcher8}, true)
	if err != nil {
		t.Fatal(err)
	}
	martyTermSearcher8, err := NewTermSearcher(twoDocIndex, "marty", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher8, err := NewTermSearcher(twoDocIndex, "dustin", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	shouldSearcher8, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{martyTermSearcher8, dustinTermSearcher8}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	steveTermSearcher8, err := NewTermSearcher(twoDocIndex, "steve", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustNotSearcher8, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{steveTermSearcher8}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher8, err := NewTermBooleanSearcher(twoDocIndex, mustSearcher8, shouldSearcher8, mustNotSearcher8, true)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher8a, err := NewTermSearcher(twoDocIndex, "dustin", "name", 5.0, true)
	if err != nil {
		t.Fatal(err)
	}
	conjunctionSearcher8, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{booleanSearcher8, dustinTermSearcher8a}, true)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		searcher Searcher
		results  []*DocumentMatch
	}{
		{
			searcher: booleanSearcher,
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "1",
					Score: 0.9818005051949021,
				},
				&DocumentMatch{
					ID:    "3",
					Score: 0.808709699395535,
				},
				&DocumentMatch{
					ID:    "4",
					Score: 0.34618161159873423,
				},
			},
		},
		{
			searcher: booleanSearcher2,
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
			searcher: booleanSearcher3,
			results:  []*DocumentMatch{},
		},
		{
			searcher: booleanSearcher4,
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
			searcher: booleanSearcher5,
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
			searcher: booleanSearcher6,
			results:  []*DocumentMatch{},
		},
		// test a conjunction query with a nested boolean
		{
			searcher: conjunctionSearcher7,
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "1",
					Score: 2.0097428702814377,
				},
			},
		},
		{
			searcher: conjunctionSearcher8,
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "3",
					Score: 2.0681575785068107,
				},
			},
		},
	}

	for testIndex, test := range tests {
		defer test.searcher.Close()

		next, err := test.searcher.Next()
		i := 0
		for err == nil && next != nil {
			if i < len(test.results) {
				if next.ID != test.results[i].ID {
					t.Errorf("expected result %d to have id %s got %s for test %d", i, test.results[i].ID, next.ID, testIndex)
				}
				if !scoresCloseEnough(next.Score, test.results[i].Score) {
					t.Errorf("expected result %d to have score %v got  %v for test %d", i, test.results[i].Score, next.Score, testIndex)
					t.Logf("scoring explanation: %s", next.Expl)
				}
			}
			next, err = test.searcher.Next()
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
