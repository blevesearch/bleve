//  Copyright (c) 2024 Couchbase, Inc.
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

package aggregation

import (
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

func TestSignificantTermsAggregation(t *testing.T) {
	// Simulate a scenario where we're searching for documents about "databases"
	// and want to find terms that are uncommonly common in the results

	// Foreground: terms in search results (documents about databases)
	// Background: term frequencies in the entire corpus

	agg := NewSignificantTermsAggregation("tags", 10, 1, SignificanceAlgorithmJLH)

	// Set background stats (simulating pre-search data)
	// Total corpus has 1000 docs
	agg.SetBackgroundStats(&search.SignificantTermsStats{
		Field:     "tags",
		TotalDocs: 1000,
		TermDocFreqs: map[string]int64{
			"database":    100,  // appears in 10% of all docs
			"nosql":       50,   // appears in 5% of all docs
			"sql":         80,   // appears in 8% of all docs
			"scalability": 30,   // appears in 3% of all docs
			"performance": 200,  // appears in 20% of all docs (very common)
			"cloud":       150,  // appears in 15% of all docs
			"programming": 300,  // appears in 30% of all docs (very common, generic)
		},
	})

	// Simulate processing 50 documents from search results
	// These documents are specifically about databases
	foregroundDocs := []struct {
		tags []string
	}{
		// Many docs mention nosql and database together
		{[]string{"database", "nosql", "scalability"}},
		{[]string{"database", "nosql"}},
		{[]string{"database", "nosql", "performance"}},
		{[]string{"database", "sql"}},
		{[]string{"database", "sql", "performance"}},
		{[]string{"nosql", "scalability"}},
		{[]string{"nosql", "cloud"}},
		{[]string{"sql", "database"}},
		// Some docs mention programming (very common term)
		{[]string{"programming", "database"}},
		{[]string{"programming", "cloud"}},
	}

	for _, doc := range foregroundDocs {
		agg.StartDoc()
		for _, tag := range doc.tags {
			agg.UpdateVisitor("tags", []byte(tag))
		}
		agg.EndDoc()
	}

	// Get results
	result := agg.Result()

	// Verify result
	if result.Type != "significant_terms" {
		t.Errorf("Expected type 'significant_terms', got '%s'", result.Type)
	}

	if result.Field != "tags" {
		t.Errorf("Expected field 'tags', got '%s'", result.Field)
	}

	if len(result.Buckets) == 0 {
		t.Fatal("Expected some significant terms, got none")
	}

	// The most significant term should be "nosql" or "scalability"
	// because they appear frequently in results but infrequently in background
	mostSignificant := result.Buckets[0].Key.(string)
	if mostSignificant != "nosql" && mostSignificant != "scalability" {
		t.Logf("Warning: Expected 'nosql' or 'scalability' to be most significant, got '%s'", mostSignificant)
	}

	// Verify each bucket has required metadata
	for i, bucket := range result.Buckets {
		if bucket.Metadata == nil {
			t.Errorf("Bucket[%d] missing metadata", i)
			continue
		}

		if _, ok := bucket.Metadata["score"]; !ok {
			t.Errorf("Bucket[%d] metadata missing 'score'", i)
		}

		if _, ok := bucket.Metadata["bg_count"]; !ok {
			t.Errorf("Bucket[%d] metadata missing 'bg_count'", i)
		}

		// Verify scores are in descending order
		if i > 0 {
			prevScore := result.Buckets[i-1].Metadata["score"].(float64)
			currScore := bucket.Metadata["score"].(float64)
			if prevScore < currScore {
				t.Errorf("Buckets not sorted by score: bucket[%d].score=%.4f > bucket[%d].score=%.4f",
					i-1, prevScore, i, currScore)
			}
		}
	}

	// Verify result metadata
	if result.Metadata == nil {
		t.Error("Result metadata is nil")
	} else {
		if alg, ok := result.Metadata["algorithm"]; !ok || alg != "jlh" {
			t.Errorf("Expected algorithm 'jlh', got %v", alg)
		}
	}
}

func TestSignificantTermsAlgorithms(t *testing.T) {
	algorithms := []struct {
		name string
		alg  SignificanceAlgorithm
	}{
		{"JLH", SignificanceAlgorithmJLH},
		{"Mutual Information", SignificanceAlgorithmMutualInformation},
		{"Chi-Squared", SignificanceAlgorithmChiSquared},
		{"Percentage", SignificanceAlgorithmPercentage},
	}

	backgroundStats := &search.SignificantTermsStats{
		Field:     "category",
		TotalDocs: 1000,
		TermDocFreqs: map[string]int64{
			"common":    500, // 50% - very common
			"rare":      10,  // 1% - very rare
			"moderate":  100, // 10% - moderate
		},
	}

	for _, tc := range algorithms {
		t.Run(tc.name, func(t *testing.T) {
			agg := NewSignificantTermsAggregation("category", 10, 1, tc.alg)
			agg.SetBackgroundStats(backgroundStats)

			// Simulate 100 docs where "rare" appears often (significant!)
			// and "common" appears moderately (not significant)
			for i := 0; i < 50; i++ {
				agg.StartDoc()
				agg.UpdateVisitor("category", []byte("rare"))
				agg.EndDoc()
			}

			for i := 0; i < 30; i++ {
				agg.StartDoc()
				agg.UpdateVisitor("category", []byte("moderate"))
				agg.EndDoc()
			}

			for i := 0; i < 20; i++ {
				agg.StartDoc()
				agg.UpdateVisitor("category", []byte("common"))
				agg.EndDoc()
			}

			result := agg.Result()

			// All algorithms should rank "rare" as most significant
			if len(result.Buckets) == 0 {
				t.Fatal("Expected buckets, got none")
			}

			mostSignificant := result.Buckets[0].Key.(string)
			if mostSignificant != "rare" {
				t.Errorf("Expected 'rare' to be most significant with %s, got '%s'",
					tc.name, mostSignificant)
			}

			// Verify algorithm name in metadata
			if alg, ok := result.Metadata["algorithm"]; !ok || alg != string(tc.alg) {
				t.Errorf("Expected algorithm '%s', got %v", tc.alg, alg)
			}
		})
	}
}

func TestSignificantTermsMinDocCount(t *testing.T) {
	agg := NewSignificantTermsAggregation("field", 10, 5, SignificanceAlgorithmJLH)

	agg.SetBackgroundStats(&search.SignificantTermsStats{
		Field:     "field",
		TotalDocs: 1000,
		TermDocFreqs: map[string]int64{
			"frequent": 10,
			"rare":     5,
		},
	})

	// "frequent" appears 10 times (above threshold)
	for i := 0; i < 10; i++ {
		agg.StartDoc()
		agg.UpdateVisitor("field", []byte("frequent"))
		agg.EndDoc()
	}

	// "rare" appears 3 times (below threshold of 5)
	for i := 0; i < 3; i++ {
		agg.StartDoc()
		agg.UpdateVisitor("field", []byte("rare"))
		agg.EndDoc()
	}

	result := agg.Result()

	// Should only include "frequent" because "rare" is below minDocCount
	if len(result.Buckets) != 1 {
		t.Errorf("Expected 1 bucket (rare filtered out by minDocCount), got %d", len(result.Buckets))
	}

	if len(result.Buckets) > 0 && result.Buckets[0].Key != "frequent" {
		t.Errorf("Expected 'frequent' to be included, got '%v'", result.Buckets[0].Key)
	}
}

func TestSignificantTermsSizeLimit(t *testing.T) {
	agg := NewSignificantTermsAggregation("field", 3, 1, SignificanceAlgorithmJLH)

	// Create background stats with many terms
	termDocFreqs := make(map[string]int64)
	for i := 0; i < 10; i++ {
		termDocFreqs[string(rune('a'+i))] = int64(10 + i)
	}

	agg.SetBackgroundStats(&search.SignificantTermsStats{
		Field:        "field",
		TotalDocs:    1000,
		TermDocFreqs: termDocFreqs,
	})

	// Add docs with various terms
	for i := 0; i < 10; i++ {
		for j := 0; j < 5; j++ {
			agg.StartDoc()
			agg.UpdateVisitor("field", []byte(string(rune('a'+i))))
			agg.EndDoc()
		}
	}

	result := agg.Result()

	// Should only return top 3 most significant terms
	if len(result.Buckets) != 3 {
		t.Errorf("Expected 3 buckets (size limit), got %d", len(result.Buckets))
	}
}

func TestSignificantTermsNoBackgroundData(t *testing.T) {
	agg := NewSignificantTermsAggregation("field", 10, 1, SignificanceAlgorithmJLH)

	// Don't set background stats or index reader

	agg.StartDoc()
	agg.UpdateVisitor("field", []byte("term"))
	agg.EndDoc()

	result := agg.Result()

	// Should return empty result when no background data is available
	if len(result.Buckets) != 0 {
		t.Errorf("Expected 0 buckets when no background data, got %d", len(result.Buckets))
	}
}

func TestSignificantTermsClone(t *testing.T) {
	original := NewSignificantTermsAggregation("field", 10, 5, SignificanceAlgorithmChiSquared)

	// Process some docs in original
	original.StartDoc()
	original.UpdateVisitor("field", []byte("term"))
	original.EndDoc()

	// Clone
	cloned := original.Clone().(*SignificantTermsAggregation)

	// Verify clone has same configuration
	if cloned.field != original.field {
		t.Errorf("Cloned field doesn't match")
	}
	if cloned.size != original.size {
		t.Errorf("Cloned size doesn't match")
	}
	if cloned.minDocCount != original.minDocCount {
		t.Errorf("Cloned minDocCount doesn't match")
	}
	if cloned.algorithm != original.algorithm {
		t.Errorf("Cloned algorithm doesn't match")
	}

	// Verify clone has fresh state
	if len(cloned.foregroundTerms) != 0 {
		t.Errorf("Cloned aggregation should have empty foreground terms, got %d", len(cloned.foregroundTerms))
	}
	if cloned.foregroundDocCount != 0 {
		t.Errorf("Cloned aggregation should have zero doc count, got %d", cloned.foregroundDocCount)
	}
}

func TestScoringFunctions(t *testing.T) {
	tests := []struct {
		name      string
		fgCount   int64
		fgTotal   int64
		bgCount   int64
		bgTotal   int64
		algorithm SignificanceAlgorithm
		minScore  float64 // Minimum expected score (actual may be higher)
	}{
		{
			name:      "JLH - high significance",
			fgCount:   50,  // 50% in foreground
			fgTotal:   100,
			bgCount:   10,  // 1% in background
			bgTotal:   1000,
			algorithm: SignificanceAlgorithmJLH,
			minScore:  0.1, // Should be positive and significant
		},
		{
			name:      "JLH - low significance",
			fgCount:   10,  // 10% in foreground
			fgTotal:   100,
			bgCount:   100, // 10% in background (same rate)
			bgTotal:   1000,
			algorithm: SignificanceAlgorithmJLH,
			minScore:  0.0, // Should be close to zero
		},
		{
			name:      "Mutual Information",
			fgCount:   80,
			fgTotal:   100,
			bgCount:   100,
			bgTotal:   1000,
			algorithm: SignificanceAlgorithmMutualInformation,
			minScore:  0.0,
		},
		{
			name:      "Chi-Squared",
			fgCount:   60,
			fgTotal:   100,
			bgCount:   50,
			bgTotal:   1000,
			algorithm: SignificanceAlgorithmChiSquared,
			minScore:  0.0,
		},
		{
			name:      "Percentage",
			fgCount:   50,
			fgTotal:   100,
			bgCount:   10,
			bgTotal:   1000,
			algorithm: SignificanceAlgorithmPercentage,
			minScore:  0.0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			agg := NewSignificantTermsAggregation("field", 10, 1, tc.algorithm)
			score := agg.calculateScore(tc.fgCount, tc.fgTotal, tc.bgCount, tc.bgTotal)

			if score < tc.minScore {
				t.Errorf("Score %.6f is less than minimum expected %.6f", score, tc.minScore)
			}

			// Verify no NaN or Inf
			if score != score { // NaN check
				t.Error("Score is NaN")
			}
			if score > 1e308 { // Inf check
				t.Error("Score is Inf")
			}
		})
	}
}

func TestSignificantTermsEdgeCases(t *testing.T) {
	t.Run("Zero background frequency", func(t *testing.T) {
		agg := NewSignificantTermsAggregation("field", 10, 1, SignificanceAlgorithmJLH)
		score := agg.calculateScore(10, 100, 0, 1000)
		// Should handle gracefully (return 0)
		if score < 0 || score != score {
			t.Errorf("Expected valid score for zero background freq, got %.6f", score)
		}
	})

	t.Run("Empty foreground", func(t *testing.T) {
		agg := NewSignificantTermsAggregation("field", 10, 1, SignificanceAlgorithmJLH)
		agg.SetBackgroundStats(&search.SignificantTermsStats{
			Field:        "field",
			TotalDocs:    1000,
			TermDocFreqs: map[string]int64{"term": 100},
		})

		result := agg.Result()
		if len(result.Buckets) != 0 {
			t.Errorf("Expected no buckets for empty foreground, got %d", len(result.Buckets))
		}
	})

	t.Run("Single term", func(t *testing.T) {
		agg := NewSignificantTermsAggregation("field", 10, 1, SignificanceAlgorithmJLH)
		agg.SetBackgroundStats(&search.SignificantTermsStats{
			Field:        "field",
			TotalDocs:    1000,
			TermDocFreqs: map[string]int64{"only": 50},
		})

		agg.StartDoc()
		agg.UpdateVisitor("field", []byte("only"))
		agg.EndDoc()

		result := agg.Result()
		if len(result.Buckets) != 1 {
			t.Errorf("Expected 1 bucket for single term, got %d", len(result.Buckets))
		}
		if len(result.Buckets) > 0 && result.Buckets[0].Key != "only" {
			t.Errorf("Expected term 'only', got '%v'", result.Buckets[0].Key)
		}
	})
}
