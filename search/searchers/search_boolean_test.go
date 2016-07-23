//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package searchers

import (
	"testing"

	"github.com/blevesearch/bleve/search"
)

func TestBooleanSearch(t *testing.T) {

	if twoDocIndex == nil {
		t.Fatal("its null")
	}
	twoDocIndexReader, err := twoDocIndex.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := twoDocIndexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// test 0
	beerTermSearcher, err := NewTermSearcher(twoDocIndexReader, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustSearcher, err := NewConjunctionSearcher(twoDocIndexReader, []search.Searcher{beerTermSearcher}, true)
	if err != nil {
		t.Fatal(err)
	}
	martyTermSearcher, err := NewTermSearcher(twoDocIndexReader, "marty", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher, err := NewTermSearcher(twoDocIndexReader, "dustin", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	shouldSearcher, err := NewDisjunctionSearcher(twoDocIndexReader, []search.Searcher{martyTermSearcher, dustinTermSearcher}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	steveTermSearcher, err := NewTermSearcher(twoDocIndexReader, "steve", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustNotSearcher, err := NewDisjunctionSearcher(twoDocIndexReader, []search.Searcher{steveTermSearcher}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher, err := NewBooleanSearcher(twoDocIndexReader, mustSearcher, shouldSearcher, mustNotSearcher, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 1
	martyTermSearcher2, err := NewTermSearcher(twoDocIndexReader, "marty", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher2, err := NewTermSearcher(twoDocIndexReader, "dustin", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	shouldSearcher2, err := NewDisjunctionSearcher(twoDocIndexReader, []search.Searcher{martyTermSearcher2, dustinTermSearcher2}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	steveTermSearcher2, err := NewTermSearcher(twoDocIndexReader, "steve", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustNotSearcher2, err := NewDisjunctionSearcher(twoDocIndexReader, []search.Searcher{steveTermSearcher2}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher2, err := NewBooleanSearcher(twoDocIndexReader, nil, shouldSearcher2, mustNotSearcher2, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 2
	steveTermSearcher3, err := NewTermSearcher(twoDocIndexReader, "steve", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustNotSearcher3, err := NewDisjunctionSearcher(twoDocIndexReader, []search.Searcher{steveTermSearcher3}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher3, err := NewBooleanSearcher(twoDocIndexReader, nil, nil, mustNotSearcher3, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 3
	beerTermSearcher4, err := NewTermSearcher(twoDocIndexReader, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustSearcher4, err := NewConjunctionSearcher(twoDocIndexReader, []search.Searcher{beerTermSearcher4}, true)
	if err != nil {
		t.Fatal(err)
	}
	steveTermSearcher4, err := NewTermSearcher(twoDocIndexReader, "steve", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustNotSearcher4, err := NewDisjunctionSearcher(twoDocIndexReader, []search.Searcher{steveTermSearcher4}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher4, err := NewBooleanSearcher(twoDocIndexReader, mustSearcher4, nil, mustNotSearcher4, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 4
	beerTermSearcher5, err := NewTermSearcher(twoDocIndexReader, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustSearcher5, err := NewConjunctionSearcher(twoDocIndexReader, []search.Searcher{beerTermSearcher5}, true)
	if err != nil {
		t.Fatal(err)
	}
	steveTermSearcher5, err := NewTermSearcher(twoDocIndexReader, "steve", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	martyTermSearcher5, err := NewTermSearcher(twoDocIndexReader, "marty", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustNotSearcher5, err := NewDisjunctionSearcher(twoDocIndexReader, []search.Searcher{steveTermSearcher5, martyTermSearcher5}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher5, err := NewBooleanSearcher(twoDocIndexReader, mustSearcher5, nil, mustNotSearcher5, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 5
	beerTermSearcher6, err := NewTermSearcher(twoDocIndexReader, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustSearcher6, err := NewConjunctionSearcher(twoDocIndexReader, []search.Searcher{beerTermSearcher6}, true)
	if err != nil {
		t.Fatal(err)
	}
	martyTermSearcher6, err := NewTermSearcher(twoDocIndexReader, "marty", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher6, err := NewTermSearcher(twoDocIndexReader, "dustin", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	shouldSearcher6, err := NewDisjunctionSearcher(twoDocIndexReader, []search.Searcher{martyTermSearcher6, dustinTermSearcher6}, 2, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher6, err := NewBooleanSearcher(twoDocIndexReader, mustSearcher6, shouldSearcher6, nil, true)
	if err != nil {
		t.Fatal(err)
	}

	// test 6
	beerTermSearcher7, err := NewTermSearcher(twoDocIndexReader, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustSearcher7, err := NewConjunctionSearcher(twoDocIndexReader, []search.Searcher{beerTermSearcher7}, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher7, err := NewBooleanSearcher(twoDocIndexReader, mustSearcher7, nil, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	martyTermSearcher7, err := NewTermSearcher(twoDocIndexReader, "marty", "name", 5.0, true)
	if err != nil {
		t.Fatal(err)
	}
	conjunctionSearcher7, err := NewConjunctionSearcher(twoDocIndexReader, []search.Searcher{martyTermSearcher7, booleanSearcher7}, true)

	// test 7
	beerTermSearcher8, err := NewTermSearcher(twoDocIndexReader, "beer", "desc", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustSearcher8, err := NewConjunctionSearcher(twoDocIndexReader, []search.Searcher{beerTermSearcher8}, true)
	if err != nil {
		t.Fatal(err)
	}
	martyTermSearcher8, err := NewTermSearcher(twoDocIndexReader, "marty", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher8, err := NewTermSearcher(twoDocIndexReader, "dustin", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	shouldSearcher8, err := NewDisjunctionSearcher(twoDocIndexReader, []search.Searcher{martyTermSearcher8, dustinTermSearcher8}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	steveTermSearcher8, err := NewTermSearcher(twoDocIndexReader, "steve", "name", 1.0, true)
	if err != nil {
		t.Fatal(err)
	}
	mustNotSearcher8, err := NewDisjunctionSearcher(twoDocIndexReader, []search.Searcher{steveTermSearcher8}, 0, true)
	if err != nil {
		t.Fatal(err)
	}
	booleanSearcher8, err := NewBooleanSearcher(twoDocIndexReader, mustSearcher8, shouldSearcher8, mustNotSearcher8, true)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher8a, err := NewTermSearcher(twoDocIndexReader, "dustin", "name", 5.0, true)
	if err != nil {
		t.Fatal(err)
	}
	conjunctionSearcher8, err := NewConjunctionSearcher(twoDocIndexReader, []search.Searcher{booleanSearcher8, dustinTermSearcher8a}, true)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		searcher search.Searcher
		results  []*search.DocumentMatch
	}{
		{
			searcher: booleanSearcher,
			results: []*search.DocumentMatch{
				{
					ID:    "1",
					Score: 0.9818005051949021,
				},
				{
					ID:    "3",
					Score: 0.808709699395535,
				},
				{
					ID:    "4",
					Score: 0.34618161159873423,
				},
			},
		},
		{
			searcher: booleanSearcher2,
			results: []*search.DocumentMatch{
				{
					ID:    "1",
					Score: 0.6775110856165737,
				},
				{
					ID:    "3",
					Score: 0.6775110856165737,
				},
			},
		},
		// no MUST or SHOULD clauses yields no results
		{
			searcher: booleanSearcher3,
			results:  []*search.DocumentMatch{},
		},
		{
			searcher: booleanSearcher4,
			results: []*search.DocumentMatch{
				{
					ID:    "1",
					Score: 1.0,
				},
				{
					ID:    "3",
					Score: 0.5,
				},
				{
					ID:    "4",
					Score: 1.0,
				},
			},
		},
		{
			searcher: booleanSearcher5,
			results: []*search.DocumentMatch{
				{
					ID:    "3",
					Score: 0.5,
				},
				{
					ID:    "4",
					Score: 1.0,
				},
			},
		},
		{
			searcher: booleanSearcher6,
			results:  []*search.DocumentMatch{},
		},
		// test a conjunction query with a nested boolean
		{
			searcher: conjunctionSearcher7,
			results: []*search.DocumentMatch{
				{
					ID:    "1",
					Score: 2.0097428702814377,
				},
			},
		},
		{
			searcher: conjunctionSearcher8,
			results: []*search.DocumentMatch{
				{
					ID:    "3",
					Score: 2.0681575785068107,
				},
			},
		},
	}

	for testIndex, test := range tests {
		defer func() {
			err := test.searcher.Close()
			if err != nil {
				t.Fatal(err)
			}
		}()

		next, err := test.searcher.Next(nil)
		i := 0
		for err == nil && next != nil {
			if i < len(test.results) {
				if next.ArrangeID() != test.results[i].ArrangeID() {
					t.Errorf("expected result %d to have id %s got %s for test %d", i, test.results[i].ArrangeID(), next.ArrangeID(), testIndex)
				}
				if !scoresCloseEnough(next.Score, test.results[i].Score) {
					t.Errorf("expected result %d to have score %v got  %v for test %d", i, test.results[i].Score, next.Score, testIndex)
					t.Logf("scoring explanation: %s", next.Expl)
				}
			}
			next, err = test.searcher.Next(nil)
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
