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

func TestTermConjunctionSearch(t *testing.T) {

	// test 0
	beerTermSearcher, err := NewTermSearcher(twoDocIndex, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	martyTermSearcher, err := NewTermSearcher(twoDocIndex, "marty", "name", 5.0, true)
	if err != nil {
		t.Fatal(err)
	}
	beerAndMartySearcher, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{beerTermSearcher, martyTermSearcher}, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 1
	angstTermSearcher, err := NewTermSearcher(twoDocIndex, "angst", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	beerTermSearcher2, err := NewTermSearcher(twoDocIndex, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	angstAndBeerSearcher, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{angstTermSearcher, beerTermSearcher2}, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 2
	beerTermSearcher3, err := NewTermSearcher(twoDocIndex, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	jackTermSearcher, err := NewTermSearcher(twoDocIndex, "jack", "name", 5.0, true)
	if err != nil {
		t.Fatal(err)
	}
	beerAndJackSearcher, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{beerTermSearcher3, jackTermSearcher}, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 3
	beerTermSearcher4, err := NewTermSearcher(twoDocIndex, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	misterTermSearcher, err := NewTermSearcher(twoDocIndex, "mister", "title", 5.0, true)
	if err != nil {
		t.Fatal(err)
	}
	beerAndMisterSearcher, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{beerTermSearcher4, misterTermSearcher}, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 4
	couchbaseTermSearcher, err := NewTermSearcher(twoDocIndex, "couchbase", "street", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	misterTermSearcher2, err := NewTermSearcher(twoDocIndex, "mister", "title", 5.0, true)
	if err != nil {
		t.Fatal(err)
	}
	couchbaseAndMisterSearcher, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{couchbaseTermSearcher, misterTermSearcher2}, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 5
	beerTermSearcher5, err := NewTermSearcher(twoDocIndex, "beer", "desc", 5.0, true)
	if err != nil {
		t.Fatal(err)
	}
	couchbaseTermSearcher2, err := NewTermSearcher(twoDocIndex, "couchbase", "street", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	misterTermSearcher3, err := NewTermSearcher(twoDocIndex, "mister", "title", 5.0, true)
	if err != nil {
		t.Fatal(err)
	}
	couchbaseAndMisterSearcher2, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{couchbaseTermSearcher2, misterTermSearcher3}, true)
	if err != nil {
		t.Fatal(err)
	}
	beerAndCouchbaseAndMisterSearcher, err := NewTermConjunctionSearcher(twoDocIndex, []Searcher{beerTermSearcher5, couchbaseAndMisterSearcher2}, true)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		searcher Searcher
		results  []*DocumentMatch
	}{
		{
			searcher: beerAndMartySearcher,
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "1",
					Score: 2.0097428702814377,
				},
			},
		},
		{
			searcher: angstAndBeerSearcher,
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "2",
					Score: 1.0807601687084403,
				},
			},
		},
		{
			searcher: beerAndJackSearcher,
			results:  []*DocumentMatch{},
		},
		{
			searcher: beerAndMisterSearcher,
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
			searcher: couchbaseAndMisterSearcher,
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "2",
					Score: 1.4436599157093672,
				},
			},
		},
		{
			searcher: beerAndCouchbaseAndMisterSearcher,
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "2",
					Score: 1.441614953806971,
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
