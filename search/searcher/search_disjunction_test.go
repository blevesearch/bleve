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
	"context"
	"os"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

func TestDisjunctionSearch(t *testing.T) {
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

	martyTermSearcher, err := NewTermSearcher(context.TODO(), twoDocIndexReader, "marty", "name", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher, err := NewTermSearcher(context.TODO(), twoDocIndexReader, "dustin", "name", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	martyOrDustinSearcher, err := NewDisjunctionSearcher(context.TODO(), twoDocIndexReader, []search.Searcher{martyTermSearcher, dustinTermSearcher}, 0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}

	martyTermSearcher2, err := NewTermSearcher(context.TODO(), twoDocIndexReader, "marty", "name", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher2, err := NewTermSearcher(context.TODO(), twoDocIndexReader, "dustin", "name", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	martyOrDustinSearcher2, err := NewDisjunctionSearcher(context.TODO(), twoDocIndexReader, []search.Searcher{martyTermSearcher2, dustinTermSearcher2}, 0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}

	raviTermSearcher, err := NewTermSearcher(context.TODO(), twoDocIndexReader, "ravi", "name", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	nestedRaviOrMartyOrDustinSearcher, err := NewDisjunctionSearcher(context.TODO(), twoDocIndexReader, []search.Searcher{raviTermSearcher, martyOrDustinSearcher2}, 0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		searcher search.Searcher
		results  []*search.DocumentMatch
	}{
		{
			searcher: martyOrDustinSearcher,
			results: []*search.DocumentMatch{
				{
					IndexInternalID: index.IndexInternalID("1"),
					Score:           0.6775110856165737,
				},
				{
					IndexInternalID: index.IndexInternalID("3"),
					Score:           0.6775110856165737,
				},
			},
		},
		// test a nested disjunction
		{
			searcher: nestedRaviOrMartyOrDustinSearcher,
			results: []*search.DocumentMatch{
				{
					IndexInternalID: index.IndexInternalID("1"),
					Score:           0.2765927424732821,
				},
				{
					IndexInternalID: index.IndexInternalID("3"),
					Score:           0.2765927424732821,
				},
				{
					IndexInternalID: index.IndexInternalID("4"),
					Score:           0.5531854849465642,
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
			DocumentMatchPool: search.NewDocumentMatchPool(test.searcher.DocumentMatchPoolSize(), 0),
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
			ctx.DocumentMatchPool.Put(next)
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

func TestDisjunctionAdvance(t *testing.T) {
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

	martyTermSearcher, err := NewTermSearcher(context.TODO(), twoDocIndexReader, "marty", "name", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher, err := NewTermSearcher(context.TODO(), twoDocIndexReader, "dustin", "name", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	martyOrDustinSearcher, err := NewDisjunctionSearcher(context.TODO(), twoDocIndexReader, []search.Searcher{martyTermSearcher, dustinTermSearcher}, 0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}

	ctx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(martyOrDustinSearcher.DocumentMatchPoolSize(), 0),
	}
	match, err := martyOrDustinSearcher.Advance(ctx, index.IndexInternalID("3"))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if match == nil {
		t.Errorf("expected 3, got nil")
	}
}

func TestDisjunctionSearchTooMany(t *testing.T) {
	// set to max to a low non-zero value
	DisjunctionMaxClauseCount = 2
	defer func() {
		// reset it after the test
		DisjunctionMaxClauseCount = 0
	}()

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

	martyTermSearcher, err := NewTermSearcher(context.TODO(), twoDocIndexReader, "marty", "name", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	dustinTermSearcher, err := NewTermSearcher(context.TODO(), twoDocIndexReader, "dustin", "name", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	steveTermSearcher, err := NewTermSearcher(context.TODO(), twoDocIndexReader, "steve", "name", 1.0, explainTrue)
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewDisjunctionSearcher(context.TODO(), twoDocIndexReader, []search.Searcher{martyTermSearcher, dustinTermSearcher, steveTermSearcher}, 0, explainTrue)
	if err == nil {
		t.Fatal(err)
	}
}

func TestUnadornedDisjunctionAdvance(t *testing.T) {
	dir, _ := os.MkdirTemp("", "scorchTwoDoc")
	defer func() {
		_ = os.RemoveAll(dir)
	}()
	twoDocIndex := initTwoDocScorch(dir)
	twoDocIndexReader, err := twoDocIndex.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := twoDocIndexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	getNewOptimizedCompositeSearcher := func(t *testing.T) search.Searcher {
		optimizedCompositeSearcherOptions := search.SearcherOptions{Explain: false, IncludeTermVectors: false, Score: "none"}
		martyTermSearcher, err := NewTermSearcher(context.Background(), twoDocIndexReader, "marty", "name", 1.0, optimizedCompositeSearcherOptions)
		if err != nil {
			t.Fatal(err)
		}
		dustinTermSearcher, err := NewTermSearcher(context.Background(), twoDocIndexReader, "dustin", "name", 1.0, optimizedCompositeSearcherOptions)
		if err != nil {
			t.Fatal(err)
		}
		steveTermSearcher, err := NewTermSearcher(context.Background(), twoDocIndexReader, "steve", "name", 1.0, optimizedCompositeSearcherOptions)
		if err != nil {
			t.Fatal(err)
		}
		martyOrDustinOrSteveSearcher, err := NewDisjunctionSearcher(context.Background(), twoDocIndexReader, []search.Searcher{martyTermSearcher, dustinTermSearcher, steveTermSearcher}, 0, optimizedCompositeSearcherOptions)
		if err != nil {
			t.Fatal(err)
		}
		return martyOrDustinOrSteveSearcher
	}
	martyOrDustinOrSteveSearcher := getNewOptimizedCompositeSearcher(t)
	ctx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(martyOrDustinOrSteveSearcher.DocumentMatchPoolSize(), 0),
	}
	// get the correct order using only next calls
	dm, err := martyOrDustinOrSteveSearcher.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	expectedDocIDs := []index.IndexInternalID{}
	for dm != nil && err == nil {
		expectedDocIDs = append(expectedDocIDs, dm.IndexInternalID)
		dm, err = martyOrDustinOrSteveSearcher.Next(ctx)
	}
	if err != nil {
		t.Fatal(err)
	}
	if len(expectedDocIDs) != 3 {
		t.Fatalf("expected 3 results, got %d", len(expectedDocIDs))
	}
	// Test 1 - Advance in reverse direction after getting the correct order using only next calls
	// Next(->) - Next(->) - Next(->) - Advance(<-) - Advance(<-)
	for i := len(expectedDocIDs) - 1; i >= 0; i-- {
		xID := expectedDocIDs[i]
		dm, err = martyOrDustinOrSteveSearcher.Advance(ctx, xID)
		if err != nil {
			t.Fatal(err)
		}
		if dm == nil {
			t.Fatalf("expected to find %v", xID)
		}
		if !dm.IndexInternalID.Equals(xID) {
			t.Fatalf("expected %v, got %v", xID, dm.IndexInternalID)
		}
	}
	// Test 2 - Advance in forward direction after getting the correct order using only next calls
	// Next(->) - Next(->) - Next(->) - Advance(ResetTo0) - Advance(->) - Advance(->)
	martyOrDustinOrSteveSearcher = getNewOptimizedCompositeSearcher(t)
	for i := 0; i < len(expectedDocIDs); i++ {
		xID := expectedDocIDs[i]
		dm, err = martyOrDustinOrSteveSearcher.Advance(ctx, xID)
		if err != nil {
			t.Fatal(err)
		}
		if dm == nil {
			t.Fatalf("expected to find %v", xID)
		}
		if !dm.IndexInternalID.Equals(xID) {
			t.Fatalf("expected %v, got %v", xID, dm.IndexInternalID)
		}
	}
	// Test 3 - Alternate Next and Advance calls
	// Next(->) -> Next(->) -> Advance(<-) -> Next(->) -> Next(->) -> Advance(<-) -> Advance(<-) -> Next(->)
	martyOrDustinOrSteveSearcher = getNewOptimizedCompositeSearcher(t)
	goNext := func(expectedDocID index.IndexInternalID) {
		dm, err = martyOrDustinOrSteveSearcher.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if dm == nil {
			t.Fatal("expected a document, got nil")
		}
		if !dm.IndexInternalID.Equals(expectedDocID) {
			t.Fatalf("expected %v, got %v", expectedDocID, dm.IndexInternalID)
		}
	}
	goBack := func(goTo index.IndexInternalID) {
		dm, err = martyOrDustinOrSteveSearcher.Advance(ctx, goTo)
		if err != nil {
			t.Fatal(err)
		}
		if dm == nil {
			t.Fatalf("expected to find %v", goTo)
		}
		if !dm.IndexInternalID.Equals(goTo) {
			t.Fatalf("expected %v, got %v", goTo, dm.IndexInternalID)
		}
	}
	// Next		(->)
	goNext(expectedDocIDs[0])
	// Next		(->)
	goNext(expectedDocIDs[1])
	// Advance	(<-)
	goBack(expectedDocIDs[0])
	// Next		(->)
	goNext(expectedDocIDs[1])
	// Next		(->)
	goNext(expectedDocIDs[2])
	// Advance	(<-)
	goBack(expectedDocIDs[1])
	// Advance	(<-)
	goBack(expectedDocIDs[0])
	// Next		(->)
	goNext(expectedDocIDs[1])
}
