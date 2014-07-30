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

func TestTermDisjunctionSearch(t *testing.T) {

	martyTermSearcher, err := NewTermSearcher(twoDocIndex, "marty", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher, err := NewTermSearcher(twoDocIndex, "dustin", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	martyOrDustinSearcher, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{martyTermSearcher, dustinTermSearcher}, 0, true)
	if err != nil {
		t.Fatal(err)
	}

	martyTermSearcher2, err := NewTermSearcher(twoDocIndex, "marty", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher2, err := NewTermSearcher(twoDocIndex, "dustin", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	martyOrDustinSearcher2, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{martyTermSearcher2, dustinTermSearcher2}, 0, true)
	if err != nil {
		t.Fatal(err)
	}

	raviTermSearcher, err := NewTermSearcher(twoDocIndex, "ravi", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	nestedRaviOrMartyOrDustinSearcher, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{raviTermSearcher, martyOrDustinSearcher2}, 0, true)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		searcher Searcher
		results  []*DocumentMatch
	}{
		{
			searcher: martyOrDustinSearcher,
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
		// test a nested disjunction
		{
			searcher: nestedRaviOrMartyOrDustinSearcher,
			results: []*DocumentMatch{
				&DocumentMatch{
					ID:    "1",
					Score: 0.33875554280828685,
				},
				&DocumentMatch{
					ID:    "3",
					Score: 0.33875554280828685,
				},
				&DocumentMatch{
					ID:    "4",
					Score: 0.5531854849465642,
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

func TestDisjunctionAdvance(t *testing.T) {

	martyTermSearcher, err := NewTermSearcher(twoDocIndex, "marty", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher, err := NewTermSearcher(twoDocIndex, "dustin", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	martyOrDustinSearcher, err := NewTermDisjunctionSearcher(twoDocIndex, []Searcher{martyTermSearcher, dustinTermSearcher}, 0, true)
	if err != nil {
		t.Fatal(err)
	}

	match, err := martyOrDustinSearcher.Advance("3")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match == nil {
		t.Errorf("expected 3, got nil")
	}
}
