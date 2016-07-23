//  Copyright (c) 2013 Couchbase, Inc.
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

func TestMatchAllSearch(t *testing.T) {

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

	allSearcher, err := NewMatchAllSearcher(twoDocIndexReader, 1.0, true)
	if err != nil {
		t.Fatal(err)
	}

	allSearcher2, err := NewMatchAllSearcher(twoDocIndexReader, 1.2, true)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		searcher  search.Searcher
		queryNorm float64
		results   []*search.DocumentMatch
	}{
		{
			searcher:  allSearcher,
			queryNorm: 1.0,
			results: []*search.DocumentMatch{
				{
					ID:    "1",
					Score: 1.0,
				},
				{
					ID:    "2",
					Score: 1.0,
				},
				{
					ID:    "3",
					Score: 1.0,
				},
				{
					ID:    "4",
					Score: 1.0,
				},
				{
					ID:    "5",
					Score: 1.0,
				},
			},
		},
		{
			searcher:  allSearcher2,
			queryNorm: 0.8333333,
			results: []*search.DocumentMatch{
				{
					ID:    "1",
					Score: 1.0,
				},
				{
					ID:    "2",
					Score: 1.0,
				},
				{
					ID:    "3",
					Score: 1.0,
				},
				{
					ID:    "4",
					Score: 1.0,
				},
				{
					ID:    "5",
					Score: 1.0,
				},
			},
		},
	}

	for testIndex, test := range tests {

		if test.queryNorm != 1.0 {
			test.searcher.SetQueryNorm(test.queryNorm)
		}
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
