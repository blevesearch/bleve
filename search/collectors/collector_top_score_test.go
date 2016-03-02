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
	"math/rand"
	"strconv"
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

	collector := NewTopScorerCollector(10)
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

	collector := NewTopScorerSkipCollector(10, 10)
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

func BenchmarkTop10of100000Scores(b *testing.B) {

	matches := make(search.DocumentMatchCollection, 0, 100000)
	for i := 0; i < 100000; i++ {
		matches = append(matches, &search.DocumentMatch{
			ID:    strconv.Itoa(i),
			Score: rand.Float64(),
		})
	}
	searcher := &stubSearcher{
		matches: matches,
	}

	collector := NewTopScorerCollector(10)
	b.ResetTimer()

	err := collector.Collect(context.Background(), searcher)
	if err != nil {
		b.Fatal(err)
	}
	res := collector.Results()
	for _, dm := range res {
		b.Logf("%s - %f\n", dm.ID, dm.Score)
	}
}
