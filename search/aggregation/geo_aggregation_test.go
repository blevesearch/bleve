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

	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/numeric"
	"github.com/blevesearch/bleve/v2/search"
)

func TestGeohashGridAggregation(t *testing.T) {
	// Test data: points around San Francisco
	locations := []struct {
		name string
		lon  float64
		lat  float64
	}{
		{"Golden Gate Bridge", -122.4783, 37.8199},
		{"Fisherman's Wharf", -122.4177, 37.8080},
		{"Alcatraz Island", -122.4230, 37.8267},
		{"Twin Peaks", -122.4474, 37.7544},
		{"Mission District", -122.4194, 37.7599},
		// Point in New York (different geohash)
		{"Times Square", -73.9855, 40.7580},
	}

	tests := []struct {
		name      string
		precision int
		size      int
		expected  struct {
			minBuckets int // minimum expected buckets
			maxBuckets int // maximum expected buckets
		}
	}{
		{
			name:      "Precision 3 (156km x 156km cells)",
			precision: 3,
			size:      10,
			expected: struct {
				minBuckets int
				maxBuckets int
			}{1, 2}, // SF points might be in 1-2 cells, NY in different cell
		},
		{
			name:      "Precision 5 (4.9km x 4.9km cells)",
			precision: 5,
			size:      10,
			expected: struct {
				minBuckets int
				maxBuckets int
			}{2, 6}, // More granular, more cells
		},
		{
			name:      "Size limit 2",
			precision: 5,
			size:      2,
			expected: struct {
				minBuckets int
				maxBuckets int
			}{2, 2}, // Limited to 2 buckets
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			agg := NewGeohashGridAggregation("location", tc.precision, tc.size, nil)

			// Process each location
			for _, loc := range locations {
				agg.StartDoc()

				// Encode location as Morton hash (same as GeoPointField)
				mhash := geo.MortonHash(loc.lon, loc.lat)
				term := numeric.MustNewPrefixCodedInt64(int64(mhash), 0)

				agg.UpdateVisitor("location", term)
				agg.EndDoc()
			}

			// Get result
			result := agg.Result()

			// Verify result
			if result.Type != "geohash_grid" {
				t.Errorf("Expected type 'geohash_grid', got '%s'", result.Type)
			}

			if result.Field != "location" {
				t.Errorf("Expected field 'location', got '%s'", result.Field)
			}

			numBuckets := len(result.Buckets)
			if numBuckets < tc.expected.minBuckets || numBuckets > tc.expected.maxBuckets {
				t.Errorf("Expected %d-%d buckets, got %d", tc.expected.minBuckets, tc.expected.maxBuckets, numBuckets)
			}

			// Verify buckets are sorted by count (descending)
			for i := 1; i < len(result.Buckets); i++ {
				if result.Buckets[i-1].Count < result.Buckets[i].Count {
					t.Errorf("Buckets not sorted by count: bucket[%d].Count=%d < bucket[%d].Count=%d",
						i-1, result.Buckets[i-1].Count, i, result.Buckets[i].Count)
				}
			}

			// Verify each bucket has geohash key and metadata
			for i, bucket := range result.Buckets {
				geohash, ok := bucket.Key.(string)
				if !ok || len(geohash) != tc.precision {
					t.Errorf("Bucket[%d] key should be geohash string of length %d, got %v (len=%d)",
						i, tc.precision, bucket.Key, len(geohash))
				}

				// Verify metadata contains lat/lon
				if bucket.Metadata == nil {
					t.Errorf("Bucket[%d] missing metadata", i)
					continue
				}
				if _, ok := bucket.Metadata["lat"]; !ok {
					t.Errorf("Bucket[%d] metadata missing 'lat'", i)
				}
				if _, ok := bucket.Metadata["lon"]; !ok {
					t.Errorf("Bucket[%d] metadata missing 'lon'", i)
				}
			}

			// Verify total count (note: when size limit is applied, we only count top N buckets)
			totalCount := int64(0)
			for _, bucket := range result.Buckets {
				totalCount += bucket.Count
			}
			// Total count should always be > 0
			if totalCount <= 0 {
				t.Errorf("Total count should be > 0, got %d", totalCount)
			}
			// For precision 3 or when size >= expected buckets, we should see all documents
			if tc.precision <= 3 || tc.name == "Precision 5 (4.9km x 4.9km cells)" {
				if totalCount != int64(len(locations)) {
					t.Errorf("Total count %d doesn't match number of locations %d", totalCount, len(locations))
				}
			}
		})
	}
}

func TestGeoDistanceAggregation(t *testing.T) {
	// Center point: San Francisco downtown (-122.4194, 37.7749)
	centerLon := -122.4194
	centerLat := 37.7749

	// Test locations at various distances
	locations := []struct {
		name string
		lon  float64
		lat  float64
		// Approximate distance in km from center
	}{
		{"Very close", -122.4184, 37.7750},   // ~100m
		{"Close", -122.4094, 37.7749},        // ~1km
		{"Medium", -122.3894, 37.7749},       // ~3km
		{"Far", -122.2694, 37.7749},          // ~15km
		{"Very far", -121.8894, 37.7749},     // ~50km
		{"New York", -73.9855, 40.7580},      // ~4000km
	}

	// Define distance ranges (in kilometers)
	from0 := 0.0
	to1 := 1.0
	from1 := 1.0
	to10 := 10.0
	from10 := 10.0
	to100 := 100.0
	from100 := 100.0

	ranges := map[string]*DistanceRange{
		"0-1km": {
			Name: "0-1km",
			From: &from0,
			To:   &to1,
		},
		"1-10km": {
			Name: "1-10km",
			From: &from1,
			To:   &to10,
		},
		"10-100km": {
			Name: "10-100km",
			From: &from10,
			To:   &to100,
		},
		"100km+": {
			Name: "100km+",
			From: &from100,
			To:   nil, // no upper bound
		},
	}

	agg := NewGeoDistanceAggregation("location", centerLon, centerLat, 1000, ranges, nil)

	// Process each location
	for _, loc := range locations {
		agg.StartDoc()

		// Encode location as Morton hash
		mhash := geo.MortonHash(loc.lon, loc.lat)
		term := numeric.MustNewPrefixCodedInt64(int64(mhash), 0)

		agg.UpdateVisitor("location", term)
		agg.EndDoc()
	}

	// Get result
	result := agg.Result()

	// Verify result
	if result.Type != "geo_distance" {
		t.Errorf("Expected type 'geo_distance', got '%s'", result.Type)
	}

	if result.Field != "location" {
		t.Errorf("Expected field 'location', got '%s'", result.Field)
	}

	// Verify we have 4 buckets (one per range)
	if len(result.Buckets) != 4 {
		t.Errorf("Expected 4 buckets, got %d", len(result.Buckets))
	}

	// Verify buckets are sorted by from distance
	for i := 1; i < len(result.Buckets); i++ {
		fromPrev := result.Buckets[i-1].Metadata["from"]
		fromCurr := result.Buckets[i].Metadata["from"]
		if fromPrev != nil && fromCurr != nil {
			if *fromPrev.(*float64) > *fromCurr.(*float64) {
				t.Errorf("Buckets not sorted by from distance")
			}
		}
	}

	// Verify specific bucket counts
	// Actual distances: Very close (0.09km), Close (0.88km), Medium (2.64km),
	//                   Far (13.18km), Very far (46.58km), New York (4128.86km)
	expectedCounts := map[string]int64{
		"0-1km":    2, // Very close + Close
		"1-10km":   1, // Medium
		"10-100km": 2, // Far + Very far
		"100km+":   1, // New York
	}

	for _, bucket := range result.Buckets {
		rangeName := bucket.Key.(string)
		expectedCount, ok := expectedCounts[rangeName]
		if !ok {
			t.Errorf("Unexpected bucket key: %s", rangeName)
			continue
		}
		if bucket.Count != expectedCount {
			t.Errorf("Bucket '%s': expected count %d, got %d", rangeName, expectedCount, bucket.Count)
		}
	}

	// Verify metadata contains center coordinates
	if result.Metadata == nil {
		t.Error("Result metadata is nil")
	} else {
		if lat, ok := result.Metadata["center_lat"]; !ok || lat != centerLat {
			t.Errorf("Expected center_lat %f, got %v", centerLat, lat)
		}
		if lon, ok := result.Metadata["center_lon"]; !ok || lon != centerLon {
			t.Errorf("Expected center_lon %f, got %v", centerLon, lon)
		}
	}
}

func TestGeohashGridWithSubAggregations(t *testing.T) {
	// Test geohash grid with sub-aggregations
	locations := []struct {
		lon   float64
		lat   float64
		price float64
	}{
		{-122.4783, 37.8199, 100.0}, // Golden Gate
		{-122.4783, 37.8199, 150.0}, // Golden Gate (same cell)
		{-73.9855, 40.7580, 200.0},  // Times Square
		{-73.9855, 40.7580, 250.0},  // Times Square (same cell)
	}

	// Create sub-aggregation for average price
	subAggs := map[string]search.AggregationBuilder{
		"avg_price": NewAvgAggregation("price"),
	}

	agg := NewGeohashGridAggregation("location", 5, 10, subAggs)

	for _, loc := range locations {
		agg.StartDoc()

		// Add location field
		mhash := geo.MortonHash(loc.lon, loc.lat)
		locTerm := numeric.MustNewPrefixCodedInt64(int64(mhash), 0)
		agg.UpdateVisitor("location", locTerm)

		// Add price field
		priceTerm := numeric.MustNewPrefixCodedInt64(numeric.Float64ToInt64(loc.price), 0)
		agg.UpdateVisitor("price", priceTerm)

		agg.EndDoc()
	}

	result := agg.Result()

	// Should have 2 buckets (one for SF area, one for NY)
	if len(result.Buckets) != 2 {
		t.Errorf("Expected 2 buckets, got %d", len(result.Buckets))
	}

	// Each bucket should have avg_price sub-aggregation
	for i, bucket := range result.Buckets {
		if bucket.Aggregations == nil {
			t.Errorf("Bucket[%d] missing aggregations", i)
			continue
		}

		avgResult, ok := bucket.Aggregations["avg_price"]
		if !ok {
			t.Errorf("Bucket[%d] missing 'avg_price' aggregation", i)
			continue
		}

		avgValue := avgResult.Value.(*search.AvgResult)
		// Each bucket has 2 documents, so average should be (x + x+50) / 2 = x + 25
		if bucket.Count == 2 {
			// Golden Gate: (100 + 150) / 2 = 125
			// Times Square: (200 + 250) / 2 = 225
			if avgValue.Avg != 125.0 && avgValue.Avg != 225.0 {
				t.Errorf("Bucket[%d] unexpected average: %f (expected 125 or 225)", i, avgValue.Avg)
			}
		}
	}
}

func TestGeoDistanceWithSubAggregations(t *testing.T) {
	centerLon := -122.4194
	centerLat := 37.7749

	locations := []struct {
		lon      float64
		lat      float64
		category string
	}{
		{-122.4184, 37.7750, "restaurant"}, // Close
		{-122.4094, 37.7749, "cafe"},       // Close
		{-122.2694, 37.7749, "hotel"},      // Far
		{-121.8894, 37.7749, "museum"},     // Very far
	}

	// Define ranges
	from0 := 0.0
	to10 := 10.0
	from10 := 10.0

	ranges := map[string]*DistanceRange{
		"0-10km": {
			Name: "0-10km",
			From: &from0,
			To:   &to10,
		},
		"10km+": {
			Name: "10km+",
			From: &from10,
			To:   nil,
		},
	}

	// Create sub-aggregation for category terms
	subAggs := map[string]search.AggregationBuilder{
		"categories": NewTermsAggregation("category", 10, nil),
	}

	agg := NewGeoDistanceAggregation("location", centerLon, centerLat, 1000, ranges, subAggs)

	for _, loc := range locations {
		agg.StartDoc()

		// Add location field
		mhash := geo.MortonHash(loc.lon, loc.lat)
		locTerm := numeric.MustNewPrefixCodedInt64(int64(mhash), 0)
		agg.UpdateVisitor("location", locTerm)

		// Add category field
		agg.UpdateVisitor("category", []byte(loc.category))

		agg.EndDoc()
	}

	result := agg.Result()

	// Should have 2 buckets
	if len(result.Buckets) != 2 {
		t.Errorf("Expected 2 buckets, got %d", len(result.Buckets))
	}

	// Find the "0-10km" bucket
	var closeRangeBucket *search.Bucket
	for _, bucket := range result.Buckets {
		if bucket.Key == "0-10km" {
			closeRangeBucket = bucket
			break
		}
	}

	if closeRangeBucket == nil {
		t.Fatal("Could not find '0-10km' bucket")
	}

	// Should have 2 documents in close range
	if closeRangeBucket.Count != 2 {
		t.Errorf("Expected 2 documents in close range, got %d", closeRangeBucket.Count)
	}

	// Check sub-aggregation
	if closeRangeBucket.Aggregations == nil {
		t.Fatal("Close range bucket missing aggregations")
	}

	catResult, ok := closeRangeBucket.Aggregations["categories"]
	if !ok {
		t.Fatal("Close range bucket missing 'categories' aggregation")
	}

	// Should have 2 category buckets
	if len(catResult.Buckets) != 2 {
		t.Errorf("Expected 2 category buckets, got %d", len(catResult.Buckets))
	}
}

func TestGeohashGridClone(t *testing.T) {
	original := NewGeohashGridAggregation("location", 5, 10, nil)

	// Process a document
	original.StartDoc()
	mhash := geo.MortonHash(-122.4194, 37.7749)
	term := numeric.MustNewPrefixCodedInt64(int64(mhash), 0)
	original.UpdateVisitor("location", term)
	original.EndDoc()

	// Clone
	cloned := original.Clone().(*GeohashGridAggregation)

	// Verify clone has same configuration
	if cloned.field != original.field {
		t.Errorf("Cloned field doesn't match: %s != %s", cloned.field, original.field)
	}
	if cloned.precision != original.precision {
		t.Errorf("Cloned precision doesn't match: %d != %d", cloned.precision, original.precision)
	}
	if cloned.size != original.size {
		t.Errorf("Cloned size doesn't match: %d != %d", cloned.size, original.size)
	}

	// Verify clone has fresh state (no cell counts from original)
	if len(cloned.cellCounts) != 0 {
		t.Errorf("Cloned aggregation should have empty cell counts, got %d", len(cloned.cellCounts))
	}
}

func TestGeoDistanceClone(t *testing.T) {
	from0 := 0.0
	to10 := 10.0
	ranges := map[string]*DistanceRange{
		"0-10km": {
			Name: "0-10km",
			From: &from0,
			To:   &to10,
		},
	}

	original := NewGeoDistanceAggregation("location", -122.4194, 37.7749, 1000, ranges, nil)

	// Process a document
	original.StartDoc()
	mhash := geo.MortonHash(-122.4184, 37.7750)
	term := numeric.MustNewPrefixCodedInt64(int64(mhash), 0)
	original.UpdateVisitor("location", term)
	original.EndDoc()

	// Clone
	cloned := original.Clone().(*GeoDistanceAggregation)

	// Verify clone has same configuration
	if cloned.field != original.field {
		t.Errorf("Cloned field doesn't match")
	}
	if cloned.centerLon != original.centerLon {
		t.Errorf("Cloned centerLon doesn't match")
	}
	if cloned.centerLat != original.centerLat {
		t.Errorf("Cloned centerLat doesn't match")
	}
	if len(cloned.ranges) != len(original.ranges) {
		t.Errorf("Cloned ranges count doesn't match")
	}

	// Verify clone has fresh state
	if len(cloned.rangeCounts) != 0 {
		t.Errorf("Cloned aggregation should have empty range counts, got %d", len(cloned.rangeCounts))
	}
}
