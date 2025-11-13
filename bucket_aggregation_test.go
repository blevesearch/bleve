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
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

func TestBucketAggregations(t *testing.T) {
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

	// Index documents with brand and price
	docs := []struct {
		ID    string
		Brand string
		Price float64
	}{
		{"doc1", "Apple", 999.00},
		{"doc2", "Apple", 1299.00},
		{"doc3", "Samsung", 799.00},
		{"doc4", "Samsung", 899.00},
		{"doc5", "Samsung", 599.00},
		{"doc6", "Google", 699.00},
		{"doc7", "Google", 799.00},
	}

	batch := index.NewBatch()
	for _, doc := range docs {
		data := map[string]interface{}{
			"brand": doc.Brand,
			"price": doc.Price,
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

	// Test terms aggregation with sub-aggregations
	t.Run("TermsWithSubAggs", func(t *testing.T) {
		query := NewMatchAllQuery()
		searchRequest := NewSearchRequest(query)

		// Create terms aggregation on brand with avg price sub-aggregation
		termsAgg := NewTermsAggregation("brand", 10)
		termsAgg.AddSubAggregation("avg_price", NewAggregationRequest("avg", "price"))
		termsAgg.AddSubAggregation("min_price", NewAggregationRequest("min", "price"))
		termsAgg.AddSubAggregation("max_price", NewAggregationRequest("max", "price"))

		searchRequest.Aggregations = AggregationsRequest{
			"by_brand": termsAgg,
		}
		searchRequest.Size = 0 // Don't need hits

		results, err := index.Search(searchRequest)
		if err != nil {
			t.Fatal(err)
		}

		byBrand, ok := results.Aggregations["by_brand"]
		if !ok {
			t.Fatal("Expected by_brand aggregation")
		}

		if len(byBrand.Buckets) != 3 {
			t.Fatalf("Expected 3 buckets, got %d", len(byBrand.Buckets))
		}

		// Check samsung bucket (should have 3 docs) - note: lowercase due to text analysis
		var samsungBucket *search.Bucket
		for _, bucket := range byBrand.Buckets {
			if bucket.Key == "samsung" {
				samsungBucket = bucket
				break
			}
		}

		if samsungBucket == nil {
			t.Fatal("samsung bucket not found")
		}

		if samsungBucket.Count != 3 {
			t.Fatalf("Expected samsung count 3, got %d", samsungBucket.Count)
		}

		// Check sub-aggregations
		if samsungBucket.Aggregations == nil {
			t.Fatal("Expected sub-aggregations in samsung bucket")
		}

		avgPrice := samsungBucket.Aggregations["avg_price"]
		if avgPrice == nil {
			t.Fatal("Expected avg_price sub-aggregation")
		}

		// samsung avg: (799 + 899 + 599) / 3 = 765.67
		expectedAvg := 765.67
		actualAvg := avgPrice.Value.(float64)
		if actualAvg < expectedAvg-1 || actualAvg > expectedAvg+1 {
			t.Fatalf("Expected samsung avg price around %f, got %f", expectedAvg, actualAvg)
		}

		minPrice := samsungBucket.Aggregations["min_price"]
		if minPrice.Value.(float64) != 599.00 {
			t.Fatalf("Expected samsung min price 599, got %f", minPrice.Value.(float64))
		}

		maxPrice := samsungBucket.Aggregations["max_price"]
		if maxPrice.Value.(float64) != 899.00 {
			t.Fatalf("Expected samsung max price 899, got %f", maxPrice.Value.(float64))
		}
	})

	// Test range aggregation with sub-aggregations
	t.Run("RangeWithSubAggs", func(t *testing.T) {
		query := NewMatchAllQuery()
		searchRequest := NewSearchRequest(query)

		// Create price ranges
		mid := 800.0
		high := 1000.0

		ranges := []*numericRange{
			{Name: "budget", Min: nil, Max: &mid},
			{Name: "mid-range", Min: &mid, Max: &high},
			{Name: "premium", Min: &high, Max: nil},
		}

		rangeAgg := NewRangeAggregation("price", ranges)
		rangeAgg.AddSubAggregation("doc_count", NewAggregationRequest("count", "price"))

		searchRequest.Aggregations = AggregationsRequest{
			"by_price_range": rangeAgg,
		}
		searchRequest.Size = 0

		results, err := index.Search(searchRequest)
		if err != nil {
			t.Fatal(err)
		}

		byRange, ok := results.Aggregations["by_price_range"]
		if !ok {
			t.Fatal("Expected by_price_range aggregation")
		}

		if len(byRange.Buckets) != 3 {
			t.Fatalf("Expected 3 range buckets, got %d", len(byRange.Buckets))
		}

		// Find budget bucket (< 800)
		// Should contain: Google 699, Google 799, Samsung 599, Samsung 799 = 4 docs
		var budgetBucket *search.Bucket
		for _, bucket := range byRange.Buckets {
			if bucket.Key == "budget" {
				budgetBucket = bucket
				break
			}
		}

		if budgetBucket == nil {
			t.Fatal("budget bucket not found")
		}

		if budgetBucket.Count != 4 {
			t.Fatalf("Expected budget count 4, got %d", budgetBucket.Count)
		}
	})
}

// Example: Average price per brand
func ExampleAggregationsRequest_termsWithSubAggregations() {
	// This example shows how to compute average price per brand
	query := NewMatchAllQuery()
	searchRequest := NewSearchRequest(query)

	// Group by brand, compute average price for each
	byBrand := NewTermsAggregation("brand", 10)
	byBrand.AddSubAggregation("avg_price", NewAggregationRequest("avg", "price"))
	byBrand.AddSubAggregation("total_revenue", NewAggregationRequest("sum", "price"))

	searchRequest.Aggregations = AggregationsRequest{
		"by_brand": byBrand,
	}

	// results, _ := index.Search(searchRequest)
	// for _, bucket := range results.Aggregations["by_brand"].Buckets {
	//     fmt.Printf("Brand: %s, Count: %d, Avg Price: %f, Total: %f\n",
	//         bucket.Key, bucket.Count,
	//         bucket.Aggregations["avg_price"].Value,
	//         bucket.Aggregations["total_revenue"].Value)
	// }
}

// Example: Filtered terms aggregation with prefix
func ExampleAggregationsRequest_filteredTerms() {
	// This example shows how to filter terms by prefix
	query := NewMatchAllQuery()
	searchRequest := NewSearchRequest(query)

	// Only aggregate brands starting with "sam" (e.g., samsung, samsonite)
	filteredBrands := NewTermsAggregationWithFilter("brand", 10, "sam", "")
	filteredBrands.AddSubAggregation("avg_price", NewAggregationRequest("avg", "price"))

	searchRequest.Aggregations = AggregationsRequest{
		"filtered_brands": filteredBrands,
	}

	// Or use regex for more complex patterns:
	// Pattern to match product codes like "PROD-1234"
	productCodes := NewTermsAggregationWithFilter("product_code", 20, "", "^PROD-[0-9]{4}$")

	searchRequest.Aggregations["product_codes"] = productCodes
}
