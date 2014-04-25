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

func TestTop10Scores(t *testing.T) {

	// a stub search with more than 10 matches
	// the top-10 scores are > 10
	// everything else is less than 10
	searcher := &stubSearcher{
		matches: DocumentMatchCollection{
			&DocumentMatch{
				ID:    "a",
				Score: 11,
			},
			&DocumentMatch{
				ID:    "b",
				Score: 9,
			},
			&DocumentMatch{
				ID:    "c",
				Score: 11,
			},
			&DocumentMatch{
				ID:    "d",
				Score: 9,
			},
			&DocumentMatch{
				ID:    "e",
				Score: 11,
			},
			&DocumentMatch{
				ID:    "f",
				Score: 9,
			},
			&DocumentMatch{
				ID:    "g",
				Score: 11,
			},
			&DocumentMatch{
				ID:    "h",
				Score: 9,
			},
			&DocumentMatch{
				ID:    "i",
				Score: 11,
			},
			&DocumentMatch{
				ID:    "j",
				Score: 11,
			},
			&DocumentMatch{
				ID:    "k",
				Score: 11,
			},
			&DocumentMatch{
				ID:    "l",
				Score: 99,
			},
			&DocumentMatch{
				ID:    "m",
				Score: 11,
			},
			&DocumentMatch{
				ID:    "n",
				Score: 11,
			},
		},
	}

	collector := NewTopScorerCollector(10)
	collector.Collect(searcher)

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
