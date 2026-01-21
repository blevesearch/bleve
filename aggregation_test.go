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

package bleve

import (
	"math"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

func TestAggregations(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	indexMapping := NewIndexMapping()
	index, err := New(tmpIndexPath, indexMapping)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Index documents with numeric fields
	docs := []struct {
		ID    string
		Price float64
		Count int
	}{
		{"doc1", 10.5, 5},
		{"doc2", 20.0, 10},
		{"doc3", 15.5, 7},
		{"doc4", 30.0, 15},
		{"doc5", 25.0, 12},
	}

	batch := index.NewBatch()
	for _, doc := range docs {
		data := map[string]interface{}{
			"price": doc.Price,
			"count": doc.Count,
		}
		err := batch.Index(doc.ID, data)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = index.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	// Test sum aggregation
	t.Run("Sum", func(t *testing.T) {
		query := NewMatchAllQuery()
		searchRequest := NewSearchRequest(query)
		searchRequest.Aggregations = AggregationsRequest{
			"total_price": NewAggregationRequest("sum", "price"),
		}
		searchRequest.Size = 0 // Don't need hits

		results, err := index.Search(searchRequest)
		if err != nil {
			t.Fatal(err)
		}

		if results.Aggregations == nil {
			t.Fatal("Expected aggregations in results")
		}

		sumAgg, ok := results.Aggregations["total_price"]
		if !ok {
			t.Fatal("Expected total_price aggregation")
		}

		expectedSum := 101.0 // 10.5 + 20.0 + 15.5 + 30.0 + 25.0
		if sumAgg.Value.(float64) != expectedSum {
			t.Fatalf("Expected sum %f, got %f", expectedSum, sumAgg.Value)
		}
	})

	// Test avg aggregation
	t.Run("Avg", func(t *testing.T) {
		query := NewMatchAllQuery()
		searchRequest := NewSearchRequest(query)
		searchRequest.Aggregations = AggregationsRequest{
			"avg_price": NewAggregationRequest("avg", "price"),
		}
		searchRequest.Size = 0

		results, err := index.Search(searchRequest)
		if err != nil {
			t.Fatal(err)
		}

		avgAgg := results.Aggregations["avg_price"]
		avgResult := avgAgg.Value.(*search.AvgResult)
		expectedAvg := 20.2 // 101.0 / 5
		if math.Abs(avgResult.Avg-expectedAvg) > 0.01 {
			t.Fatalf("Expected avg %f, got %f", expectedAvg, avgResult.Avg)
		}
	})

	// Test min aggregation
	t.Run("Min", func(t *testing.T) {
		query := NewMatchAllQuery()
		searchRequest := NewSearchRequest(query)
		searchRequest.Aggregations = AggregationsRequest{
			"min_price": NewAggregationRequest("min", "price"),
		}
		searchRequest.Size = 0

		results, err := index.Search(searchRequest)
		if err != nil {
			t.Fatal(err)
		}

		minAgg := results.Aggregations["min_price"]
		expectedMin := 10.5
		if minAgg.Value.(float64) != expectedMin {
			t.Fatalf("Expected min %f, got %f", expectedMin, minAgg.Value)
		}
	})

	// Test max aggregation
	t.Run("Max", func(t *testing.T) {
		query := NewMatchAllQuery()
		searchRequest := NewSearchRequest(query)
		searchRequest.Aggregations = AggregationsRequest{
			"max_price": NewAggregationRequest("max", "price"),
		}
		searchRequest.Size = 0

		results, err := index.Search(searchRequest)
		if err != nil {
			t.Fatal(err)
		}

		maxAgg := results.Aggregations["max_price"]
		expectedMax := 30.0
		if maxAgg.Value.(float64) != expectedMax {
			t.Fatalf("Expected max %f, got %f", expectedMax, maxAgg.Value)
		}
	})

	// Test count aggregation
	t.Run("Count", func(t *testing.T) {
		query := NewMatchAllQuery()
		searchRequest := NewSearchRequest(query)
		searchRequest.Aggregations = AggregationsRequest{
			"count_price": NewAggregationRequest("count", "price"),
		}
		searchRequest.Size = 0

		results, err := index.Search(searchRequest)
		if err != nil {
			t.Fatal(err)
		}

		countAgg := results.Aggregations["count_price"]
		expectedCount := int64(5)
		if countAgg.Value.(int64) != expectedCount {
			t.Fatalf("Expected count %d, got %d", expectedCount, countAgg.Value)
		}
	})

	// Test multiple aggregations at once
	t.Run("Multiple", func(t *testing.T) {
		query := NewMatchAllQuery()
		searchRequest := NewSearchRequest(query)
		searchRequest.Aggregations = AggregationsRequest{
			"total_price": NewAggregationRequest("sum", "price"),
			"avg_count":   NewAggregationRequest("avg", "count"),
			"min_price":   NewAggregationRequest("min", "price"),
			"max_count":   NewAggregationRequest("max", "count"),
		}
		searchRequest.Size = 0

		results, err := index.Search(searchRequest)
		if err != nil {
			t.Fatal(err)
		}

		if len(results.Aggregations) != 4 {
			t.Fatalf("Expected 4 aggregations, got %d", len(results.Aggregations))
		}

		// Verify all aggregations are present
		if _, ok := results.Aggregations["total_price"]; !ok {
			t.Fatal("Missing total_price aggregation")
		}
		if _, ok := results.Aggregations["avg_count"]; !ok {
			t.Fatal("Missing avg_count aggregation")
		}
		if _, ok := results.Aggregations["min_price"]; !ok {
			t.Fatal("Missing min_price aggregation")
		}
		if _, ok := results.Aggregations["max_count"]; !ok {
			t.Fatal("Missing max_count aggregation")
		}
	})

	// Test aggregations with filtered query
	t.Run("Filtered", func(t *testing.T) {
		// Query for price >= 20
		query := NewNumericRangeQuery(Float64Ptr(20.0), nil)
		query.SetField("price")
		searchRequest := NewSearchRequest(query)
		searchRequest.Aggregations = AggregationsRequest{
			"filtered_sum":   NewAggregationRequest("sum", "price"),
			"filtered_count": NewAggregationRequest("count", "price"),
		}
		searchRequest.Size = 0

		results, err := index.Search(searchRequest)
		if err != nil {
			t.Fatal(err)
		}

		// Should only aggregate docs with price >= 20: 20.0, 25.0, 30.0
		sumAgg := results.Aggregations["filtered_sum"]
		expectedSum := 75.0 // 20.0 + 25.0 + 30.0
		if sumAgg.Value.(float64) != expectedSum {
			t.Fatalf("Expected filtered sum %f, got %f", expectedSum, sumAgg.Value)
		}

		countAgg := results.Aggregations["filtered_count"]
		expectedCount := int64(3)
		if countAgg.Value.(int64) != expectedCount {
			t.Fatalf("Expected filtered count %d, got %d", expectedCount, countAgg.Value)
		}
	})
}

// Float64Ptr returns a pointer to a float64 value
func Float64Ptr(f float64) *float64 {
	return &f
}
