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

package searcher

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

func TestConjunctionSearch(t *testing.T) {
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

	explainTrue := search.SearcherOptions{Explain: true}

	// test 0
	beerTermSearcher, err := NewTermSearcher(nil, twoDocIndexReader, "beer", "desc", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	martyTermSearcher, err := NewTermSearcher(nil, twoDocIndexReader, "marty", "name", 5.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	beerAndMartySearcher, err := NewConjunctionSearcher(nil, twoDocIndexReader, []search.Searcher{beerTermSearcher, martyTermSearcher}, explainTrue)
	if err != nil {
		t.Fatal(err)
	}

	// test 1
	angstTermSearcher, err := NewTermSearcher(nil, twoDocIndexReader, "angst", "desc", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	beerTermSearcher2, err := NewTermSearcher(nil, twoDocIndexReader, "beer", "desc", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	angstAndBeerSearcher, err := NewConjunctionSearcher(nil, twoDocIndexReader, []search.Searcher{angstTermSearcher, beerTermSearcher2}, explainTrue)
	if err != nil {
		t.Fatal(err)
	}

	// test 2
	beerTermSearcher3, err := NewTermSearcher(nil, twoDocIndexReader, "beer", "desc", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	jackTermSearcher, err := NewTermSearcher(nil, twoDocIndexReader, "jack", "name", 5.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	beerAndJackSearcher, err := NewConjunctionSearcher(nil, twoDocIndexReader, []search.Searcher{beerTermSearcher3, jackTermSearcher}, explainTrue)
	if err != nil {
		t.Fatal(err)
	}

	// test 3
	beerTermSearcher4, err := NewTermSearcher(nil, twoDocIndexReader, "beer", "desc", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	misterTermSearcher, err := NewTermSearcher(nil, twoDocIndexReader, "mister", "title", 5.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	beerAndMisterSearcher, err := NewConjunctionSearcher(nil, twoDocIndexReader, []search.Searcher{beerTermSearcher4, misterTermSearcher}, explainTrue)
	if err != nil {
		t.Fatal(err)
	}

	// test 4
	couchbaseTermSearcher, err := NewTermSearcher(nil, twoDocIndexReader, "couchbase", "street", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	misterTermSearcher2, err := NewTermSearcher(nil, twoDocIndexReader, "mister", "title", 5.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	couchbaseAndMisterSearcher, err := NewConjunctionSearcher(nil, twoDocIndexReader, []search.Searcher{couchbaseTermSearcher, misterTermSearcher2}, explainTrue)
	if err != nil {
		t.Fatal(err)
	}

	// test 5
	beerTermSearcher5, err := NewTermSearcher(nil, twoDocIndexReader, "beer", "desc", 5.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	couchbaseTermSearcher2, err := NewTermSearcher(nil, twoDocIndexReader, "couchbase", "street", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	misterTermSearcher3, err := NewTermSearcher(nil, twoDocIndexReader, "mister", "title", 5.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	couchbaseAndMisterSearcher2, err := NewConjunctionSearcher(nil, twoDocIndexReader, []search.Searcher{couchbaseTermSearcher2, misterTermSearcher3}, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	beerAndCouchbaseAndMisterSearcher, err := NewConjunctionSearcher(nil, twoDocIndexReader, []search.Searcher{beerTermSearcher5, couchbaseAndMisterSearcher2}, explainTrue)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		searcher search.Searcher
		results  []*search.DocumentMatch
	}{
		{
			searcher: beerAndMartySearcher,
			results: []*search.DocumentMatch{
				{
					IndexInternalID: index.IndexInternalID("1"),
					Score:           2.0097428702814377,
				},
			},
		},
		{
			searcher: angstAndBeerSearcher,
			results: []*search.DocumentMatch{
				{
					IndexInternalID: index.IndexInternalID("2"),
					Score:           1.0807601687084403,
				},
			},
		},
		{
			searcher: beerAndJackSearcher,
			results:  []*search.DocumentMatch{},
		},
		{
			searcher: beerAndMisterSearcher,
			results: []*search.DocumentMatch{
				{
					IndexInternalID: index.IndexInternalID("2"),
					Score:           1.2877980334016337,
				},
				{
					IndexInternalID: index.IndexInternalID("3"),
					Score:           1.2877980334016337,
				},
			},
		},
		{
			searcher: couchbaseAndMisterSearcher,
			results: []*search.DocumentMatch{
				{
					IndexInternalID: index.IndexInternalID("2"),
					Score:           1.4436599157093672,
				},
			},
		},
		{
			searcher: beerAndCouchbaseAndMisterSearcher,
			results: []*search.DocumentMatch{
				{
					IndexInternalID: index.IndexInternalID("2"),
					Score:           1.441614953806971,
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

		ctx := &search.SearchContext{
			DocumentMatchPool: search.NewDocumentMatchPool(10, 0),
		}
		next, err := test.searcher.Next(ctx)
		i := 0
		for err == nil && next != nil {
			if i < len(test.results) {
				if !next.IndexInternalID.Equals(test.results[i].IndexInternalID) {
					t.Errorf("expected result %d to have id %s got %s for test %d", i, test.results[i].IndexInternalID, next.IndexInternalID, testIndex)
				}
				if !scoresCloseEnough(next.Score, test.results[i].Score) {
					t.Errorf("expected result %d to have score %v got  %v for test %d", i, test.results[i].Score, next.Score, testIndex)
					t.Logf("scoring explanation: %s", next.Expl)
				}
			}
			next, err = test.searcher.Next(ctx)
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

type compositeSearchOptimizationTest struct {
	fieldTerms  []string
	expectEmpty string
}

func TestScorchCompositeSearchOptimizations(t *testing.T) {
	dir, _ := ioutil.TempDir("", "scorchTwoDoc")
	defer func() {
		_ = os.RemoveAll(dir)
	}()

	twoDocIndex := initTwoDocScorch(dir)

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

	tests := []compositeSearchOptimizationTest{
		{fieldTerms: []string{},
			expectEmpty: "conjunction,disjunction"},
		{fieldTerms: []string{"name:marty"},
			expectEmpty: ""},
		{fieldTerms: []string{"name:marty", "desc:beer"},
			expectEmpty: ""},
		{fieldTerms: []string{"name:marty", "name:marty"},
			expectEmpty: ""},
		{fieldTerms: []string{"name:marty", "desc:beer", "title:mister", "street:couchbase"},
			expectEmpty: "conjunction"},
		{fieldTerms: []string{"name:steve", "desc:beer", "title:mister", "street:couchbase"},
			expectEmpty: ""},

		{fieldTerms: []string{"name:NotARealName"},
			expectEmpty: "conjunction,disjunction"},
		{fieldTerms: []string{"name:NotARealName", "name:marty"},
			expectEmpty: "conjunction"},
		{fieldTerms: []string{"name:NotARealName", "name:marty", "desc:beer"},
			expectEmpty: "conjunction"},
		{fieldTerms: []string{"name:NotARealName", "name:marty", "name:marty"},
			expectEmpty: "conjunction"},
		{fieldTerms: []string{"name:NotARealName", "name:marty", "desc:beer", "title:mister", "street:couchbase"},
			expectEmpty: "conjunction"},
	}

	// The theme of this unit test is that given one of the above
	// search test cases -- no matter what searcher options we
	// provide, across either conjunctions or disjunctions, whether we
	// have optimizations that are enabled or disabled, the set of doc
	// ID's from the search results from any of those combinations
	// should be the same.
	searcherOptionsToCompare := []search.SearcherOptions{
		search.SearcherOptions{},
		search.SearcherOptions{Explain: true},
		search.SearcherOptions{IncludeTermVectors: true},
		search.SearcherOptions{IncludeTermVectors: true, Explain: true},
		search.SearcherOptions{Score: "none"},
		search.SearcherOptions{Score: "none", IncludeTermVectors: true},
		search.SearcherOptions{Score: "none", IncludeTermVectors: true, Explain: true},
		search.SearcherOptions{Score: "none", Explain: true},
	}

	testScorchCompositeSearchOptimizations(t, twoDocIndexReader, tests,
		searcherOptionsToCompare, "conjunction")

	testScorchCompositeSearchOptimizations(t, twoDocIndexReader, tests,
		searcherOptionsToCompare, "disjunction")
}

func testScorchCompositeSearchOptimizations(t *testing.T, indexReader index.IndexReader,
	tests []compositeSearchOptimizationTest,
	searcherOptionsToCompare []search.SearcherOptions,
	compositeKind string) {
	for testi := range tests {
		resultsToCompare := map[string]bool{}

		testScorchCompositeSearchOptimizationsHelper(t, indexReader, tests, testi,
			searcherOptionsToCompare, compositeKind, false, resultsToCompare)

		testScorchCompositeSearchOptimizationsHelper(t, indexReader, tests, testi,
			searcherOptionsToCompare, compositeKind, true, resultsToCompare)
	}
}

func testScorchCompositeSearchOptimizationsHelper(
	t *testing.T, indexReader index.IndexReader,
	tests []compositeSearchOptimizationTest, testi int,
	searcherOptionsToCompare []search.SearcherOptions,
	compositeKind string, allowOptimizations bool, resultsToCompare map[string]bool) {
	// Save the global allowed optimization settings to restore later.
	optimizeConjunction := scorch.OptimizeConjunction
	optimizeConjunctionUnadorned := scorch.OptimizeConjunctionUnadorned
	optimizeDisjunctionUnadorned := scorch.OptimizeDisjunctionUnadorned
	optimizeDisjunctionUnadornedMinChildCardinality :=
		scorch.OptimizeDisjunctionUnadornedMinChildCardinality

	scorch.OptimizeConjunction = allowOptimizations
	scorch.OptimizeConjunctionUnadorned = allowOptimizations
	scorch.OptimizeDisjunctionUnadorned = allowOptimizations

	if allowOptimizations {
		scorch.OptimizeDisjunctionUnadornedMinChildCardinality = uint64(0)
	}

	defer func() {
		scorch.OptimizeConjunction = optimizeConjunction
		scorch.OptimizeConjunctionUnadorned = optimizeConjunctionUnadorned
		scorch.OptimizeDisjunctionUnadorned = optimizeDisjunctionUnadorned
		scorch.OptimizeDisjunctionUnadornedMinChildCardinality =
			optimizeDisjunctionUnadornedMinChildCardinality
	}()

	test := tests[testi]

	for searcherOptionsI, searcherOptions := range searcherOptionsToCompare {
		// Construct the leaf term searchers.
		var searchers []search.Searcher

		for _, fieldTerm := range test.fieldTerms {
			ft := strings.Split(fieldTerm, ":")
			field := ft[0]
			term := ft[1]

			searcher, err := NewTermSearcher(nil, indexReader, term, field, 1.0, searcherOptions)
			if err != nil {
				t.Fatal(err)
			}

			searchers = append(searchers, searcher)
		}

		// Construct the composite searcher.
		var cs search.Searcher
		var err error
		if compositeKind == "conjunction" {
			cs, err = NewConjunctionSearcher(nil, indexReader, searchers, searcherOptions)
		} else {
			cs, err = NewDisjunctionSearcher(nil, indexReader, searchers, 0, searcherOptions)
		}
		if err != nil {
			t.Fatal(err)
		}

		ctx := &search.SearchContext{
			DocumentMatchPool: search.NewDocumentMatchPool(10, 0),
		}

		next, err := cs.Next(ctx)
		i := 0
		for err == nil && next != nil {
			docID, err := indexReader.ExternalID(next.IndexInternalID)
			if err != nil {
				t.Fatal(err)
			}

			if searcherOptionsI == 0 && allowOptimizations == false {
				resultsToCompare[string(docID)] = true
			} else {
				if !resultsToCompare[string(docID)] {
					t.Errorf("missing %s", string(docID))
				}
			}

			next, err = cs.Next(ctx)
			i++
		}

		if i != len(resultsToCompare) {
			t.Errorf("mismatched count, %d vs %d", i, len(resultsToCompare))
		}

		if i == 0 && !strings.Contains(test.expectEmpty, compositeKind) {
			t.Errorf("testi: %d, compositeKind: %s, allowOptimizations: %t,"+
				" searcherOptionsI: %d, searcherOptions: %#v,"+
				" expected some results but got no results on test: %#v",
				testi, compositeKind, allowOptimizations,
				searcherOptionsI, searcherOptions, test)
		}
	}
}
