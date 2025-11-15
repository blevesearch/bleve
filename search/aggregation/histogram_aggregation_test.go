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
	"github.com/blevesearch/bleve/v2/search"
)

func TestHistogramAggregation(t *testing.T) {
	// Test data: product prices
	prices := []float64{
		15.99, 25.50, 45.00, 52.99, 75.00,
		95.00, 105.50, 125.00, 145.00, 175.99,
		205.00, 225.50, 245.00,
	}

	tests := []struct {
		name        string
		interval    float64
		minDocCount int64
		expected    struct {
			numBuckets int
			firstKey   float64
			lastKey    float64
		}
	}{
		{
			name:        "Interval 50",
			interval:    50.0,
			minDocCount: 0,
			expected: struct {
				numBuckets int
				firstKey   float64
				lastKey    float64
			}{
				numBuckets: 5, // 0-50, 50-100, 100-150, 150-200, 200-250
				firstKey:   0.0,
				lastKey:    200.0,
			},
		},
		{
			name:        "Interval 100",
			interval:    100.0,
			minDocCount: 0,
			expected: struct {
				numBuckets int
				firstKey   float64
				lastKey    float64
			}{
				numBuckets: 3, // 0-100, 100-200, 200-300
				firstKey:   0.0,
				lastKey:    200.0,
			},
		},
		{
			name:        "Min doc count 3",
			interval:    50.0,
			minDocCount: 3,
			expected: struct {
				numBuckets int
				firstKey   float64
				lastKey    float64
			}{
				numBuckets: 4, // Buckets with >= 3 docs: 0-50(3), 50-100(3), 100-150(3), 200-250(3)
				firstKey:   0.0,
				lastKey:    -1, // Don't check last key as it depends on distribution
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			agg := NewHistogramAggregation("price", tc.interval, tc.minDocCount, nil)

			// Process each price
			for _, price := range prices {
				agg.StartDoc()
				term := numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(price), 0)
				agg.UpdateVisitor("price", term)
				agg.EndDoc()
			}

			// Get result
			result := agg.Result()

			// Verify result
			if result.Type != "histogram" {
				t.Errorf("Expected type 'histogram', got '%s'", result.Type)
			}

			if result.Field != "price" {
				t.Errorf("Expected field 'price', got '%s'", result.Field)
			}

			numBuckets := len(result.Buckets)
			if tc.expected.numBuckets > 0 && numBuckets != tc.expected.numBuckets {
				t.Errorf("Expected %d buckets, got %d", tc.expected.numBuckets, numBuckets)
			}

			// Verify buckets are sorted by key (ascending)
			for i := 1; i < len(result.Buckets); i++ {
				prevKey := result.Buckets[i-1].Key.(float64)
				currKey := result.Buckets[i].Key.(float64)
				if prevKey >= currKey {
					t.Errorf("Buckets not sorted: bucket[%d].Key=%.2f >= bucket[%d].Key=%.2f",
						i-1, prevKey, i, currKey)
				}
			}

			// Verify first bucket key
			if len(result.Buckets) > 0 {
				firstKey := result.Buckets[0].Key.(float64)
				if firstKey != tc.expected.firstKey {
					t.Errorf("Expected first bucket key %.2f, got %.2f", tc.expected.firstKey, firstKey)
				}
			}

			// Verify last bucket key if specified
			if tc.expected.lastKey >= 0 && len(result.Buckets) > 0 {
				lastKey := result.Buckets[len(result.Buckets)-1].Key.(float64)
				if lastKey != tc.expected.lastKey {
					t.Errorf("Expected last bucket key %.2f, got %.2f", tc.expected.lastKey, lastKey)
				}
			}

			// Verify metadata contains interval
			if result.Metadata == nil {
				t.Error("Result metadata is nil")
			} else {
				if interval, ok := result.Metadata["interval"]; !ok || interval != tc.interval {
					t.Errorf("Expected interval %.2f in metadata, got %v", tc.interval, interval)
				}
			}

			// Verify each bucket meets minDocCount
			for i, bucket := range result.Buckets {
				if bucket.Count < tc.minDocCount {
					t.Errorf("Bucket[%d] count %d < minDocCount %d", i, bucket.Count, tc.minDocCount)
				}
			}
		})
	}
}

func TestHistogramWithSubAggregations(t *testing.T) {
	// Test data: products with prices and categories
	products := []struct {
		price    float64
		category string
	}{
		{15.99, "books"},
		{25.50, "books"},
		{45.00, "electronics"},
		{52.99, "electronics"},
		{105.50, "electronics"},
		{125.00, "furniture"},
	}

	// Create sub-aggregation for category terms
	subAggs := map[string]search.AggregationBuilder{
		"categories": NewTermsAggregation("category", 10, nil),
	}

	agg := NewHistogramAggregation("price", 50.0, 0, subAggs)

	for _, p := range products {
		agg.StartDoc()

		// Add price field
		priceTerm := numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(p.price), 0)
		agg.UpdateVisitor("price", priceTerm)

		// Add category field
		agg.UpdateVisitor("category", []byte(p.category))

		agg.EndDoc()
	}

	result := agg.Result()

	// Should have buckets for 0-50, 50-100, 100-150
	if len(result.Buckets) < 2 {
		t.Errorf("Expected at least 2 buckets, got %d", len(result.Buckets))
	}

	// Find the 0-50 bucket
	var bucket050 *search.Bucket
	for _, bucket := range result.Buckets {
		if bucket.Key.(float64) == 0.0 {
			bucket050 = bucket
			break
		}
	}

	if bucket050 == nil {
		t.Fatal("Could not find 0-50 bucket")
	}

	// Should have 3 documents in 0-50 range
	if bucket050.Count != 3 {
		t.Errorf("Expected 3 documents in 0-50 bucket, got %d", bucket050.Count)
	}

	// Check sub-aggregation
	if bucket050.Aggregations == nil {
		t.Fatal("0-50 bucket missing aggregations")
	}

	catResult, ok := bucket050.Aggregations["categories"]
	if !ok {
		t.Fatal("0-50 bucket missing 'categories' aggregation")
	}

	// Should have 2 category buckets (books, electronics)
	if len(catResult.Buckets) != 2 {
		t.Errorf("Expected 2 category buckets in 0-50 range, got %d", len(catResult.Buckets))
	}
}

func TestDateHistogramAggregation(t *testing.T) {
	// Test data: events at various times
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []time.Time{
		baseTime,                          // Jan 1, 2024 00:00
		baseTime.Add(2 * time.Hour),       // Jan 1, 2024 02:00
		baseTime.Add(6 * time.Hour),       // Jan 1, 2024 06:00
		baseTime.Add(25 * time.Hour),      // Jan 2, 2024 01:00
		baseTime.Add(48 * time.Hour),      // Jan 3, 2024 00:00
		baseTime.Add(72 * time.Hour),      // Jan 4, 2024 00:00
		baseTime.Add(30 * 24 * time.Hour), // Jan 31, 2024
		baseTime.Add(32 * 24 * time.Hour), // Feb 2, 2024
	}

	tests := []struct {
		name     string
		interval CalendarInterval
		expected struct {
			numBuckets int
			firstCount int64
		}
	}{
		{
			name:     "Hourly buckets",
			interval: CalendarIntervalHour,
			expected: struct {
				numBuckets int
				firstCount int64
			}{
				numBuckets: 8, // 8 different hours with events
				firstCount: 1, // First hour has 1 event
			},
		},
		{
			name:     "Daily buckets",
			interval: CalendarIntervalDay,
			expected: struct {
				numBuckets int
				firstCount int64
			}{
				numBuckets: 6, // Jan 1, 2, 3, 4, 31, Feb 2
				firstCount: 3, // Jan 1 has 3 events
			},
		},
		{
			name:     "Monthly buckets",
			interval: CalendarIntervalMonth,
			expected: struct {
				numBuckets int
				firstCount int64
			}{
				numBuckets: 2, // January and February
				firstCount: 7, // January has 7 events
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			agg := NewDateHistogramAggregation("timestamp", tc.interval, 0, nil)

			// Process each event
			for _, event := range events {
				agg.StartDoc()
				term := numeric.MustNewPrefixCodedInt64(event.UnixNano(), 0)
				agg.UpdateVisitor("timestamp", term)
				agg.EndDoc()
			}

			// Get result
			result := agg.Result()

			// Verify result
			if result.Type != "date_histogram" {
				t.Errorf("Expected type 'date_histogram', got '%s'", result.Type)
			}

			if result.Field != "timestamp" {
				t.Errorf("Expected field 'timestamp', got '%s'", result.Field)
			}

			numBuckets := len(result.Buckets)
			if numBuckets != tc.expected.numBuckets {
				t.Errorf("Expected %d buckets, got %d", tc.expected.numBuckets, numBuckets)
				for i, bucket := range result.Buckets {
					t.Logf("  Bucket[%d]: key=%v, count=%d", i, bucket.Key, bucket.Count)
				}
			}

			// Verify buckets are sorted by timestamp (ascending)
			for i := 1; i < len(result.Buckets); i++ {
				prevKey := result.Buckets[i-1].Key.(string)
				currKey := result.Buckets[i].Key.(string)
				if prevKey >= currKey {
					t.Errorf("Buckets not sorted: bucket[%d].Key=%s >= bucket[%d].Key=%s",
						i-1, prevKey, i, currKey)
				}
			}

			// Verify first bucket count
			if len(result.Buckets) > 0 && result.Buckets[0].Count != tc.expected.firstCount {
				t.Errorf("Expected first bucket count %d, got %d", tc.expected.firstCount, result.Buckets[0].Count)
			}

			// Verify metadata contains calendar_interval
			if result.Metadata == nil {
				t.Error("Result metadata is nil")
			} else {
				if interval, ok := result.Metadata["calendar_interval"]; !ok || interval != string(tc.interval) {
					t.Errorf("Expected calendar_interval '%s' in metadata, got %v", tc.interval, interval)
				}
			}

			// Verify each bucket has timestamp metadata
			for i, bucket := range result.Buckets {
				if bucket.Metadata == nil {
					t.Errorf("Bucket[%d] missing metadata", i)
					continue
				}
				if _, ok := bucket.Metadata["timestamp"]; !ok {
					t.Errorf("Bucket[%d] metadata missing 'timestamp'", i)
				}
			}
		})
	}
}

func TestDateHistogramWithFixedInterval(t *testing.T) {
	// Test with fixed duration interval
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	events := []time.Time{
		baseTime,
		baseTime.Add(10 * time.Minute),
		baseTime.Add(35 * time.Minute),
		baseTime.Add(90 * time.Minute),
	}

	agg := NewDateHistogramAggregationWithFixedInterval("timestamp", 30*time.Minute, 0, nil)

	for _, event := range events {
		agg.StartDoc()
		term := numeric.MustNewPrefixCodedInt64(event.UnixNano(), 0)
		agg.UpdateVisitor("timestamp", term)
		agg.EndDoc()
	}

	result := agg.Result()

	// Should have 3 buckets: 0-30min (2 events), 30-60min (1 event), 60-90min (0 events skipped), 90-120min (1 event)
	// Actually with minDocCount=0, should have all buckets including empty ones, but we only create buckets for observed values
	// So we'll have: 0-30 (2), 30-60 (1), 90-120 (1) = 3 buckets
	expectedBuckets := 3
	if len(result.Buckets) != expectedBuckets {
		t.Errorf("Expected %d buckets, got %d", expectedBuckets, len(result.Buckets))
	}

	// Verify first bucket has 2 events
	if len(result.Buckets) > 0 && result.Buckets[0].Count != 2 {
		t.Errorf("Expected first bucket count 2, got %d", result.Buckets[0].Count)
	}

	// Verify metadata contains interval
	if result.Metadata == nil {
		t.Error("Result metadata is nil")
	} else {
		if interval, ok := result.Metadata["interval"]; !ok {
			t.Error("Expected 'interval' in metadata")
		} else if interval != "30m0s" {
			t.Errorf("Expected interval '30m0s' in metadata, got %v", interval)
		}
	}
}

func TestDateHistogramWithSubAggregations(t *testing.T) {
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	events := []struct {
		time     time.Time
		severity string
	}{
		{baseTime, "info"},
		{baseTime.Add(2 * time.Hour), "warning"},
		{baseTime.Add(25 * time.Hour), "error"},
		{baseTime.Add(26 * time.Hour), "error"},
	}

	// Create sub-aggregation for severity terms
	subAggs := map[string]search.AggregationBuilder{
		"severities": NewTermsAggregation("severity", 10, nil),
	}

	agg := NewDateHistogramAggregation("timestamp", CalendarIntervalDay, 0, subAggs)

	for _, e := range events {
		agg.StartDoc()

		// Add timestamp field
		timeTerm := numeric.MustNewPrefixCodedInt64(e.time.UnixNano(), 0)
		agg.UpdateVisitor("timestamp", timeTerm)

		// Add severity field
		agg.UpdateVisitor("severity", []byte(e.severity))

		agg.EndDoc()
	}

	result := agg.Result()

	// Should have 2 buckets (Jan 1 and Jan 2)
	if len(result.Buckets) != 2 {
		t.Errorf("Expected 2 buckets, got %d", len(result.Buckets))
	}

	// Check first bucket (Jan 1)
	firstBucket := result.Buckets[0]
	if firstBucket.Count != 2 {
		t.Errorf("Expected 2 events in first bucket, got %d", firstBucket.Count)
	}

	if firstBucket.Aggregations == nil {
		t.Fatal("First bucket missing aggregations")
	}

	sevResult, ok := firstBucket.Aggregations["severities"]
	if !ok {
		t.Fatal("First bucket missing 'severities' aggregation")
	}

	// Should have 2 severity buckets in first day (info, warning)
	if len(sevResult.Buckets) != 2 {
		t.Errorf("Expected 2 severity buckets in first day, got %d", len(sevResult.Buckets))
	}
}

func TestHistogramClone(t *testing.T) {
	original := NewHistogramAggregation("price", 50.0, 1, nil)

	// Process a document
	original.StartDoc()
	term := numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(75.0), 0)
	original.UpdateVisitor("price", term)
	original.EndDoc()

	// Clone
	cloned := original.Clone().(*HistogramAggregation)

	// Verify clone has same configuration
	if cloned.field != original.field {
		t.Errorf("Cloned field doesn't match")
	}
	if cloned.interval != original.interval {
		t.Errorf("Cloned interval doesn't match")
	}
	if cloned.minDocCount != original.minDocCount {
		t.Errorf("Cloned minDocCount doesn't match")
	}

	// Verify clone has fresh state
	if len(cloned.bucketCounts) != 0 {
		t.Errorf("Cloned aggregation should have empty bucket counts, got %d", len(cloned.bucketCounts))
	}
}

func TestDateHistogramClone(t *testing.T) {
	original := NewDateHistogramAggregation("timestamp", CalendarIntervalDay, 1, nil)

	// Process a document
	original.StartDoc()
	term := numeric.MustNewPrefixCodedInt64(time.Now().UnixNano(), 0)
	original.UpdateVisitor("timestamp", term)
	original.EndDoc()

	// Clone
	cloned := original.Clone().(*DateHistogramAggregation)

	// Verify clone has same configuration
	if cloned.field != original.field {
		t.Errorf("Cloned field doesn't match")
	}
	if cloned.calendarInterval != original.calendarInterval {
		t.Errorf("Cloned calendarInterval doesn't match")
	}
	if cloned.minDocCount != original.minDocCount {
		t.Errorf("Cloned minDocCount doesn't match")
	}

	// Verify clone has fresh state
	if len(cloned.bucketCounts) != 0 {
		t.Errorf("Cloned aggregation should have empty bucket counts, got %d", len(cloned.bucketCounts))
	}
}
