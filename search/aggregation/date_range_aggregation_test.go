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
	"time"

	"github.com/blevesearch/bleve/v2/numeric"
)

func TestDateRangeAggregation(t *testing.T) {
	// Create test dates
	jan2023 := time.Date(2023, 1, 15, 0, 0, 0, 0, time.UTC)
	jun2023 := time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)
	jan2024 := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC)
	jun2024 := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)

	// Define date ranges
	ranges := map[string]*DateRange{
		"2023": {
			Name:  "2023",
			Start: &jan2023,
			End:   &jan2024,
		},
		"2024": {
			Name:  "2024",
			Start: &jan2024,
			End:   nil, // unbounded end
		},
	}

	agg := NewDateRangeAggregation("timestamp", ranges, nil)

	// Test metadata
	if agg.Field() != "timestamp" {
		t.Errorf("Expected field 'timestamp', got '%s'", agg.Field())
	}
	if agg.Type() != "date_range" {
		t.Errorf("Expected type 'date_range', got '%s'", agg.Type())
	}

	// Simulate documents with timestamps
	testDates := []time.Time{
		jan2023, // Should fall into "2023"
		jun2023, // Should fall into "2023"
		jan2024, // Should fall into "2024"
		jun2024, // Should fall into "2024"
	}

	for _, testDate := range testDates {
		agg.StartDoc()
		term := timeToTerm(testDate)
		agg.UpdateVisitor("timestamp", term)
		agg.EndDoc()
	}

	result := agg.Result()

	// Verify results
	if len(result.Buckets) != 2 {
		t.Fatalf("Expected 2 buckets, got %d", len(result.Buckets))
	}

	// Find buckets by name
	buckets := make(map[string]int64)
	for _, bucket := range result.Buckets {
		buckets[bucket.Key.(string)] = bucket.Count
	}

	if buckets["2023"] != 2 {
		t.Errorf("Expected 2 documents in 2023, got %d", buckets["2023"])
	}
	if buckets["2024"] != 2 {
		t.Errorf("Expected 2 documents in 2024, got %d", buckets["2024"])
	}
}

func TestDateRangeAggregationUnbounded(t *testing.T) {
	// Test unbounded ranges
	mid2023 := time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC)

	ranges := map[string]*DateRange{
		"before_mid_2023": {
			Name:  "before_mid_2023",
			Start: nil, // unbounded start
			End:   &mid2023,
		},
		"after_mid_2023": {
			Name:  "after_mid_2023",
			Start: &mid2023,
			End:   nil, // unbounded end
		},
	}

	agg := NewDateRangeAggregation("timestamp", ranges, nil)

	// Test dates
	testDates := []time.Time{
		time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC),  // before
		time.Date(2023, 3, 1, 0, 0, 0, 0, time.UTC),  // before
		time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC), // on boundary (should be in after)
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),  // after
	}

	for _, testDate := range testDates {
		agg.StartDoc()
		term := timeToTerm(testDate)
		agg.UpdateVisitor("timestamp", term)
		agg.EndDoc()
	}

	result := agg.Result()

	buckets := make(map[string]int64)
	for _, bucket := range result.Buckets {
		buckets[bucket.Key.(string)] = bucket.Count
	}

	if buckets["before_mid_2023"] != 2 {
		t.Errorf("Expected 2 documents before mid-2023, got %d", buckets["before_mid_2023"])
	}
	if buckets["after_mid_2023"] != 2 {
		t.Errorf("Expected 2 documents after mid-2023, got %d", buckets["after_mid_2023"])
	}
}

func TestDateRangeAggregationMetadata(t *testing.T) {
	// Test that metadata includes start/end timestamps
	jan2023 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	dec2023 := time.Date(2023, 12, 31, 23, 59, 59, 0, time.UTC)

	ranges := map[string]*DateRange{
		"2023": {
			Name:  "2023",
			Start: &jan2023,
			End:   &dec2023,
		},
	}

	agg := NewDateRangeAggregation("timestamp", ranges, nil)

	// Add a document
	agg.StartDoc()
	term := timeToTerm(time.Date(2023, 6, 15, 0, 0, 0, 0, time.UTC))
	agg.UpdateVisitor("timestamp", term)
	agg.EndDoc()

	result := agg.Result()

	if len(result.Buckets) != 1 {
		t.Fatalf("Expected 1 bucket, got %d", len(result.Buckets))
	}

	bucket := result.Buckets[0]
	if bucket.Metadata == nil {
		t.Fatal("Expected metadata to be present")
	}

	if _, ok := bucket.Metadata["start"]; !ok {
		t.Error("Expected 'start' in metadata")
	}
	if _, ok := bucket.Metadata["end"]; !ok {
		t.Error("Expected 'end' in metadata")
	}
}

func TestDateRangeAggregationClone(t *testing.T) {
	jan2023 := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
	dec2023 := time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC)

	ranges := map[string]*DateRange{
		"2023": {
			Name:  "2023",
			Start: &jan2023,
			End:   &dec2023,
		},
	}

	original := NewDateRangeAggregation("timestamp", ranges, nil)
	cloned := original.Clone().(*DateRangeAggregation)

	if cloned.field != original.field {
		t.Errorf("Cloned field doesn't match: %s vs %s", cloned.field, original.field)
	}

	if len(cloned.ranges) != len(original.ranges) {
		t.Errorf("Cloned ranges count doesn't match: %d vs %d", len(cloned.ranges), len(original.ranges))
	}

	// Verify ranges are deep copied
	if cloned.ranges["2023"] == original.ranges["2023"] {
		t.Error("Ranges should be deep copied, not share same reference")
	}
}

// Helper function to convert time to term bytes (same as date_histogram tests)
func timeToTerm(t time.Time) []byte {
	return numeric.MustNewPrefixCodedInt64(t.UnixNano(), 0)
}
