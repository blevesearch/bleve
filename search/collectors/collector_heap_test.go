//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package collectors

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/blevesearch/bleve/search"
)

func TestTop10Scores(t *testing.T) {

	// a stub search with more than 10 matches
	// the top-10 scores are > 10
	// everything else is less than 10
	searcher := &stubSearcher{
		matches: search.DocumentMatchCollection{
			&search.DocumentMatch{
				ID:    "a",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "b",
				Score: 9,
			},
			&search.DocumentMatch{
				ID:    "c",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "d",
				Score: 9,
			},
			&search.DocumentMatch{
				ID:    "e",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "f",
				Score: 9,
			},
			&search.DocumentMatch{
				ID:    "g",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "h",
				Score: 9,
			},
			&search.DocumentMatch{
				ID:    "i",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "j",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "k",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "l",
				Score: 99,
			},
			&search.DocumentMatch{
				ID:    "m",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "n",
				Score: 11,
			},
		},
	}

	collector := NewHeapCollector(10, 0, nil, nil)
	err := collector.Collect(context.Background(), searcher)
	if err != nil {
		t.Fatal(err)
	}

	maxScore := collector.MaxScore()
	if maxScore != 99.0 {
		t.Errorf("expected max score 99.0, got %f", maxScore)
	}

	total := collector.Total()
	if total != 14 {
		t.Errorf("expected 14 total results, got %d", total)
	}

	results := collector.Results()

	if len(results) != 10 {
		t.Fatalf("expected 10 results, got %d", len(results))
	}

	if results[0].ID != "l" {
		t.Errorf("expected first result to have ID 'l', got %s", results[0].ID)
	}

	if results[0].Score != 99.0 {
		t.Errorf("expected highest score to be 99.0, got %f", results[0].Score)
	}

	minScore := 1000.0
	for _, result := range results {
		if result.Score < minScore {
			minScore = result.Score
		}
	}

	if minScore < 10 {
		t.Errorf("expected minimum score to be higher than 10, got %f", minScore)
	}
}

func TestTop10ScoresSkip10(t *testing.T) {

	// a stub search with more than 10 matches
	// the top-10 scores are > 10
	// everything else is less than 10
	searcher := &stubSearcher{
		matches: search.DocumentMatchCollection{
			&search.DocumentMatch{
				ID:    "a",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "b",
				Score: 9.5,
			},
			&search.DocumentMatch{
				ID:    "c",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "d",
				Score: 9,
			},
			&search.DocumentMatch{
				ID:    "e",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "f",
				Score: 9,
			},
			&search.DocumentMatch{
				ID:    "g",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "h",
				Score: 9,
			},
			&search.DocumentMatch{
				ID:    "i",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "j",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "k",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "l",
				Score: 99,
			},
			&search.DocumentMatch{
				ID:    "m",
				Score: 11,
			},
			&search.DocumentMatch{
				ID:    "n",
				Score: 11,
			},
		},
	}

	collector := NewHeapCollector(10, 10, nil, nil)
	err := collector.Collect(context.Background(), searcher)
	if err != nil {
		t.Fatal(err)
	}

	maxScore := collector.MaxScore()
	if maxScore != 99.0 {
		t.Errorf("expected max score 99.0, got %f", maxScore)
	}

	total := collector.Total()
	if total != 14 {
		t.Errorf("expected 14 total results, got %d", total)
	}

	results := collector.Results()

	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}

	if results[0].ID != "b" {
		t.Errorf("expected first result to have ID 'b', got %s", results[0].ID)
	}

	if results[0].Score != 9.5 {
		t.Errorf("expected highest score to be 9.5ß, got %f", results[0].Score)
	}
}

func TestPaginationSameScores(t *testing.T) {

	// a stub search with more than 10 matches
	// all documents have the same score
	searcher := &stubSearcher{
		matches: search.DocumentMatchCollection{
			&search.DocumentMatch{
				ID:    "a",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "b",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "c",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "d",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "e",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "f",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "g",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "h",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "i",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "j",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "k",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "l",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "m",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "n",
				Score: 5,
			},
		},
	}

	// first get first 5 hits
	collector := NewHeapCollector(5, 0, nil, nil)
	err := collector.Collect(context.Background(), searcher)
	if err != nil {
		t.Fatal(err)
	}

	total := collector.Total()
	if total != 14 {
		t.Errorf("expected 14 total results, got %d", total)
	}

	results := collector.Results()

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}

	firstResults := make(map[string]struct{})
	for _, hit := range results {
		firstResults[hit.ID] = struct{}{}
	}

	// a stub search with more than 10 matches
	// all documents have the same score
	searcher = &stubSearcher{
		matches: search.DocumentMatchCollection{
			&search.DocumentMatch{
				ID:    "a",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "b",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "c",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "d",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "e",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "f",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "g",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "h",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "i",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "j",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "k",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "l",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "m",
				Score: 5,
			},
			&search.DocumentMatch{
				ID:    "n",
				Score: 5,
			},
		},
	}

	// now get next 5 hits
	collector = NewHeapCollector(5, 5, nil, nil)
	err = collector.Collect(context.Background(), searcher)
	if err != nil {
		t.Fatal(err)
	}

	total = collector.Total()
	if total != 14 {
		t.Errorf("expected 14 total results, got %d", total)
	}

	results = collector.Results()

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}

	// make sure that none of these hits repeat ones we saw in the top 5
	for _, hit := range results {
		if _, ok := firstResults[hit.ID]; ok {
			t.Errorf("doc ID %s is in top 5 and next 5 result sets", hit.ID)
		}
	}
}

func BenchmarkTop10of100000Scores(b *testing.B) {
	benchHelper(10000, NewHeapCollector(10, 0, nil, nil), b)
}

func BenchmarkTop100of100000Scores(b *testing.B) {
	benchHelper(10000, NewHeapCollector(100, 0, nil, nil), b)
}

func BenchmarkTop10of1000000Scores(b *testing.B) {
	benchHelper(100000, NewHeapCollector(10, 0, nil, nil), b)
}

func BenchmarkTop100of1000000Scores(b *testing.B) {
	benchHelper(100000, NewHeapCollector(100, 0, nil, nil), b)
}
