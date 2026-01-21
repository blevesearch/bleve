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
	"fmt"
	"testing"

	"github.com/axiomhq/hyperloglog"
	"github.com/blevesearch/bleve/v2/search"
)

func TestCardinalityAggregation(t *testing.T) {
	// Test basic cardinality counting
	values := []string{"alice", "bob", "charlie", "alice", "bob", "david", "alice"}
	expectedCardinality := int64(4) // alice, bob, charlie, david

	agg := NewCardinalityAggregation("user_id", 14)

	for _, val := range values {
		agg.StartDoc()
		agg.UpdateVisitor(agg.Field(), []byte(val))
		agg.EndDoc()
	}

	result := agg.Result()
	if result.Type != "cardinality" {
		t.Errorf("Expected type 'cardinality', got '%s'", result.Type)
	}

	cardResult := result.Value.(*search.CardinalityResult)

	// HyperLogLog gives approximate results, so allow small error
	if cardResult.Cardinality != expectedCardinality {
		// With only 4 unique values, HLL should be exact or very close
		if cardResult.Cardinality < expectedCardinality-1 || cardResult.Cardinality > expectedCardinality+1 {
			t.Errorf("Expected cardinality ~%d, got %d", expectedCardinality, cardResult.Cardinality)
		}
	}

	// Verify sketch bytes are serialized
	if len(cardResult.Sketch) == 0 {
		t.Error("Expected sketch bytes to be serialized")
	}

	// Verify HLL is present for local merging
	if cardResult.HLL == nil {
		t.Error("Expected HLL to be present for local merging")
	}
}

func TestCardinalityAggregationLargeSet(t *testing.T) {
	// Test with larger set to verify HyperLogLog accuracy
	numUnique := 10000
	agg := NewCardinalityAggregation("item_id", 14)

	for i := 0; i < numUnique; i++ {
		agg.StartDoc()
		val := fmt.Sprintf("item_%d", i)
		agg.UpdateVisitor(agg.Field(), []byte(val))
		agg.EndDoc()
	}

	result := agg.Result()
	cardResult := result.Value.(*search.CardinalityResult)

	// HyperLogLog with precision 14 should give ~0.81% standard error
	// For 10000 items, that's about +/- 81 items
	tolerance := int64(200) // Allow 2% error
	lowerBound := int64(numUnique) - tolerance
	upperBound := int64(numUnique) + tolerance

	if cardResult.Cardinality < lowerBound || cardResult.Cardinality > upperBound {
		t.Errorf("Expected cardinality ~%d (+/- %d), got %d", numUnique, tolerance, cardResult.Cardinality)
	}
}

func TestCardinalityAggregationPrecision(t *testing.T) {
	// Test different precision levels
	testCases := []struct {
		precision uint8
		maxSize   int // Maximum sketch size in bytes (when not using sparse mode)
	}{
		{10, 1024},     // 2^10 = 1KB
		{12, 4096},     // 2^12 = 4KB
		{14, 16384},    // 2^14 = 16KB
		{16, 65536},    // 2^16 = 64KB
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("precision_%d", tc.precision), func(t *testing.T) {
			agg := NewCardinalityAggregation("field", tc.precision)

			// Add some values
			for i := 0; i < 100; i++ {
				agg.StartDoc()
				agg.UpdateVisitor("field", []byte(fmt.Sprintf("val_%d", i)))
				agg.EndDoc()
			}

			result := agg.Result()
			cardResult := result.Value.(*search.CardinalityResult)

			// Sketch should serialize successfully
			if len(cardResult.Sketch) == 0 {
				t.Error("Expected sketch bytes to be serialized")
			}

			// With sparse mode enabled, sketch size can be much smaller than maxSize
			// Just verify it doesn't exceed maxSize
			if len(cardResult.Sketch) > tc.maxSize {
				t.Errorf("Sketch size %d exceeds max %d bytes", len(cardResult.Sketch), tc.maxSize)
			}

			// Verify precision is set correctly
			if agg.precision != tc.precision {
				t.Errorf("Expected precision %d, got %d", tc.precision, agg.precision)
			}
		})
	}
}

func TestCardinalityAggregationMerge(t *testing.T) {
	// Test merging two cardinality results (simulating multi-shard scenario)

	// Shard 1: alice, bob, charlie
	agg1 := NewCardinalityAggregation("user_id", 14)
	values1 := []string{"alice", "bob", "charlie", "alice"}
	for _, val := range values1 {
		agg1.StartDoc()
		agg1.UpdateVisitor("user_id", []byte(val))
		agg1.EndDoc()
	}
	result1 := agg1.Result()

	// Shard 2: bob, david, eve (bob overlaps with shard 1)
	agg2 := NewCardinalityAggregation("user_id", 14)
	values2 := []string{"bob", "david", "eve", "david"}
	for _, val := range values2 {
		agg2.StartDoc()
		agg2.UpdateVisitor("user_id", []byte(val))
		agg2.EndDoc()
	}
	result2 := agg2.Result()

	// Merge results
	results := search.AggregationResults{
		"unique_users": result1,
	}
	results.Merge(search.AggregationResults{
		"unique_users": result2,
	})

	// Expected unique users: alice, bob, charlie, david, eve = 5
	expectedCardinality := int64(5)
	mergedResult := results["unique_users"].Value.(*search.CardinalityResult)

	// Allow small error due to HyperLogLog approximation
	if mergedResult.Cardinality < expectedCardinality-1 || mergedResult.Cardinality > expectedCardinality+1 {
		t.Errorf("Expected merged cardinality ~%d, got %d", expectedCardinality, mergedResult.Cardinality)
	}
}

func TestCardinalityAggregationMergeLargeSet(t *testing.T) {
	// Test merging with larger overlapping sets
	numUniqueShard1 := 5000
	numUniqueShard2 := 5000
	overlapSize := 2000 // 2000 items appear in both shards

	// Shard 1: items 0-4999
	agg1 := NewCardinalityAggregation("item_id", 14)
	for i := 0; i < numUniqueShard1; i++ {
		agg1.StartDoc()
		agg1.UpdateVisitor("item_id", []byte(fmt.Sprintf("item_%d", i)))
		agg1.EndDoc()
	}
	result1 := agg1.Result()
	card1 := result1.Value.(*search.CardinalityResult)

	// Shard 2: items 3000-7999 (overlap: 3000-4999)
	agg2 := NewCardinalityAggregation("item_id", 14)
	for i := numUniqueShard1 - overlapSize; i < numUniqueShard1+numUniqueShard2-overlapSize; i++ {
		agg2.StartDoc()
		agg2.UpdateVisitor("item_id", []byte(fmt.Sprintf("item_%d", i)))
		agg2.EndDoc()
	}
	result2 := agg2.Result()
	card2 := result2.Value.(*search.CardinalityResult)

	t.Logf("Before merge - Shard1: %d, Shard2: %d, HLL1 nil: %v, HLL2 nil: %v",
		card1.Cardinality, card2.Cardinality, card1.HLL == nil, card2.HLL == nil)

	// Merge
	results := search.AggregationResults{
		"unique_items": result1,
	}
	results.Merge(search.AggregationResults{
		"unique_items": result2,
	})

	// Expected: 5000 + 5000 - 2000 (overlap) = 8000 unique items
	expectedCardinality := int64(8000)
	mergedResult := results["unique_items"].Value.(*search.CardinalityResult)

	// Allow 2% error
	tolerance := int64(160)
	if mergedResult.Cardinality < expectedCardinality-tolerance || mergedResult.Cardinality > expectedCardinality+tolerance {
		t.Errorf("Expected merged cardinality ~%d (+/- %d), got %d", expectedCardinality, tolerance, mergedResult.Cardinality)
	}
}

func TestCardinalityAggregationSketchSerialization(t *testing.T) {
	// Test that sketch can be serialized and deserialized
	agg := NewCardinalityAggregation("field", 14)

	values := []string{"a", "b", "c", "d", "e"}
	for _, val := range values {
		agg.StartDoc()
		agg.UpdateVisitor("field", []byte(val))
		agg.EndDoc()
	}

	result := agg.Result()
	cardResult := result.Value.(*search.CardinalityResult)

	// Deserialize sketch
	hll, err := hyperloglog.NewSketch(14, true)
	if err != nil {
		t.Fatalf("Failed to create HLL sketch: %v", err)
	}
	err = hll.UnmarshalBinary(cardResult.Sketch)
	if err != nil {
		t.Fatalf("Failed to deserialize sketch: %v", err)
	}

	// Estimate should match original
	deserializedEstimate := int64(hll.Estimate())
	if deserializedEstimate != cardResult.Cardinality {
		t.Errorf("Deserialized estimate %d doesn't match original %d", deserializedEstimate, cardResult.Cardinality)
	}
}

func TestCardinalityAggregationClone(t *testing.T) {
	// Test that Clone creates a fresh instance
	agg := NewCardinalityAggregation("field", 12)

	// Add some values to original
	agg.StartDoc()
	agg.UpdateVisitor("field", []byte("value1"))
	agg.EndDoc()

	// Clone should be fresh
	cloned := agg.Clone().(*CardinalityAggregation)

	if cloned.field != agg.field {
		t.Errorf("Cloned field doesn't match: expected %s, got %s", agg.field, cloned.field)
	}

	if cloned.precision != agg.precision {
		t.Errorf("Cloned precision doesn't match: expected %d, got %d", agg.precision, cloned.precision)
	}

	// Cloned HLL should be empty (fresh)
	clonedResult := cloned.Result()
	clonedCard := clonedResult.Value.(*search.CardinalityResult)

	if clonedCard.Cardinality != 0 {
		t.Errorf("Cloned aggregation should have cardinality 0, got %d", clonedCard.Cardinality)
	}
}
