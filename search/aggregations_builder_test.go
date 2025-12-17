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

package search

import (
	"testing"
)

func TestAggregationResultsMerge(t *testing.T) {
	tests := []struct {
		name     string
		agg1     AggregationResults
		agg2     AggregationResults
		expected AggregationResults
	}{
		{
			name: "merge sum aggregations",
			agg1: AggregationResults{
				"total": &AggregationResult{
					Field: "price",
					Type:  "sum",
					Value: 100.0,
				},
			},
			agg2: AggregationResults{
				"total": &AggregationResult{
					Field: "price",
					Type:  "sum",
					Value: 50.0,
				},
			},
			expected: AggregationResults{
				"total": &AggregationResult{
					Field: "price",
					Type:  "sum",
					Value: 150.0,
				},
			},
		},
		{
			name: "merge count aggregations",
			agg1: AggregationResults{
				"count": &AggregationResult{
					Field: "items",
					Type:  "count",
					Value: int64(100),
				},
			},
			agg2: AggregationResults{
				"count": &AggregationResult{
					Field: "items",
					Type:  "count",
					Value: int64(50),
				},
			},
			expected: AggregationResults{
				"count": &AggregationResult{
					Field: "items",
					Type:  "count",
					Value: int64(150),
				},
			},
		},
		{
			name: "merge min aggregations",
			agg1: AggregationResults{
				"min": &AggregationResult{
					Field: "price",
					Type:  "min",
					Value: 10.0,
				},
			},
			agg2: AggregationResults{
				"min": &AggregationResult{
					Field: "price",
					Type:  "min",
					Value: 5.0,
				},
			},
			expected: AggregationResults{
				"min": &AggregationResult{
					Field: "price",
					Type:  "min",
					Value: 5.0,
				},
			},
		},
		{
			name: "merge max aggregations",
			agg1: AggregationResults{
				"max": &AggregationResult{
					Field: "price",
					Type:  "max",
					Value: 100.0,
				},
			},
			agg2: AggregationResults{
				"max": &AggregationResult{
					Field: "price",
					Type:  "max",
					Value: 150.0,
				},
			},
			expected: AggregationResults{
				"max": &AggregationResult{
					Field: "price",
					Type:  "max",
					Value: 150.0,
				},
			},
		},
		{
			name: "merge bucket aggregations",
			agg1: AggregationResults{
				"by_brand": &AggregationResult{
					Field: "brand",
					Type:  "terms",
					Buckets: []*Bucket{
						{Key: "Apple", Count: 10},
						{Key: "Samsung", Count: 5},
					},
				},
			},
			agg2: AggregationResults{
				"by_brand": &AggregationResult{
					Field: "brand",
					Type:  "terms",
					Buckets: []*Bucket{
						{Key: "Apple", Count: 5},
						{Key: "Google", Count: 3},
					},
				},
			},
			expected: AggregationResults{
				"by_brand": &AggregationResult{
					Field: "brand",
					Type:  "terms",
					Buckets: []*Bucket{
						{Key: "Apple", Count: 15},
						{Key: "Samsung", Count: 5},
						{Key: "Google", Count: 3},
					},
				},
			},
		},
		{
			name: "merge bucket aggregations with sub-aggregations",
			agg1: AggregationResults{
				"by_brand": &AggregationResult{
					Field: "brand",
					Type:  "terms",
					Buckets: []*Bucket{
						{
							Key:   "Apple",
							Count: 10,
							Aggregations: map[string]*AggregationResult{
								"total_price": {
									Field: "price",
									Type:  "sum",
									Value: 1000.0,
								},
							},
						},
					},
				},
			},
			agg2: AggregationResults{
				"by_brand": &AggregationResult{
					Field: "brand",
					Type:  "terms",
					Buckets: []*Bucket{
						{
							Key:   "Apple",
							Count: 5,
							Aggregations: map[string]*AggregationResult{
								"total_price": {
									Field: "price",
									Type:  "sum",
									Value: 500.0,
								},
							},
						},
					},
				},
			},
			expected: AggregationResults{
				"by_brand": &AggregationResult{
					Field: "brand",
					Type:  "terms",
					Buckets: []*Bucket{
						{
							Key:   "Apple",
							Count: 15,
							Aggregations: map[string]*AggregationResult{
								"total_price": {
									Field: "price",
									Type:  "sum",
									Value: 1500.0,
								},
							},
						},
					},
				},
			},
		},
		{
			name: "merge disjoint aggregations",
			agg1: AggregationResults{
				"sum1": &AggregationResult{
					Field: "price",
					Type:  "sum",
					Value: 100.0,
				},
			},
			agg2: AggregationResults{
				"sum2": &AggregationResult{
					Field: "cost",
					Type:  "sum",
					Value: 50.0,
				},
			},
			expected: AggregationResults{
				"sum1": &AggregationResult{
					Field: "price",
					Type:  "sum",
					Value: 100.0,
				},
				"sum2": &AggregationResult{
					Field: "cost",
					Type:  "sum",
					Value: 50.0,
				},
			},
		},
		{
			name: "merge avg aggregations properly using count and sum",
			agg1: AggregationResults{
				"avg": &AggregationResult{
					Field: "rating",
					Type:  "avg",
					Value: &AvgResult{
						Count: 2,
						Sum:   20.0,
						Avg:   10.0,
					},
				},
			},
			agg2: AggregationResults{
				"avg": &AggregationResult{
					Field: "rating",
					Type:  "avg",
					Value: &AvgResult{
						Count: 3,
						Sum:   60.0,
						Avg:   20.0,
					},
				},
			},
			expected: AggregationResults{
				"avg": &AggregationResult{
					Field: "rating",
					Type:  "avg",
					Value: &AvgResult{
						Count: 5,       // 2 + 3
						Sum:   80.0,    // 20 + 60
						Avg:   16.0,    // 80 / 5 (weighted average, not (10+20)/2)
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Make a copy of agg1 to merge into
			result := make(AggregationResults)
			for k, v := range tt.agg1 {
				result[k] = v
			}

			// Merge agg2 into result
			result.Merge(tt.agg2)

			// Check that all expected aggregations are present
			if len(result) != len(tt.expected) {
				t.Fatalf("Expected %d aggregations, got %d", len(tt.expected), len(result))
			}

			for name, expectedAgg := range tt.expected {
				actualAgg, exists := result[name]
				if !exists {
					t.Fatalf("Expected aggregation '%s' not found", name)
				}

				if actualAgg.Field != expectedAgg.Field {
					t.Errorf("Expected field %s, got %s", expectedAgg.Field, actualAgg.Field)
				}

				if actualAgg.Type != expectedAgg.Type {
					t.Errorf("Expected type %s, got %s", expectedAgg.Type, actualAgg.Type)
				}

				// Check values for metric aggregations
				if expectedAgg.Value != nil {
					// Special handling for avg and stats aggregations
					if expectedAgg.Type == "avg" {
						expectedAvg := expectedAgg.Value.(*AvgResult)
						actualAvg := actualAgg.Value.(*AvgResult)
						if expectedAvg.Count != actualAvg.Count {
							t.Errorf("Expected avg count %d, got %d", expectedAvg.Count, actualAvg.Count)
						}
						if expectedAvg.Sum != actualAvg.Sum {
							t.Errorf("Expected avg sum %f, got %f", expectedAvg.Sum, actualAvg.Sum)
						}
						if expectedAvg.Avg != actualAvg.Avg {
							t.Errorf("Expected avg value %f, got %f", expectedAvg.Avg, actualAvg.Avg)
						}
					} else if actualAgg.Value != expectedAgg.Value {
						t.Errorf("Expected value %v, got %v", expectedAgg.Value, actualAgg.Value)
					}
				}

				// Check buckets for bucket aggregations
				if len(expectedAgg.Buckets) > 0 {
					if len(actualAgg.Buckets) != len(expectedAgg.Buckets) {
						t.Fatalf("Expected %d buckets, got %d", len(expectedAgg.Buckets), len(actualAgg.Buckets))
					}

					// Build maps for easier comparison
					expectedBuckets := make(map[interface{}]*Bucket)
					for _, b := range expectedAgg.Buckets {
						expectedBuckets[b.Key] = b
					}

					for _, actualBucket := range actualAgg.Buckets {
						expectedBucket, exists := expectedBuckets[actualBucket.Key]
						if !exists {
							t.Errorf("Unexpected bucket key: %v", actualBucket.Key)
							continue
						}

						if actualBucket.Count != expectedBucket.Count {
							t.Errorf("Bucket %v: expected count %d, got %d",
								actualBucket.Key, expectedBucket.Count, actualBucket.Count)
						}

						// Check sub-aggregations
						if len(expectedBucket.Aggregations) > 0 {
							for subName, expectedSubAgg := range expectedBucket.Aggregations {
								actualSubAgg, exists := actualBucket.Aggregations[subName]
								if !exists {
									t.Errorf("Bucket %v: expected sub-aggregation '%s' not found",
										actualBucket.Key, subName)
									continue
								}

								if actualSubAgg.Value != expectedSubAgg.Value {
									t.Errorf("Bucket %v, sub-agg %s: expected value %v, got %v",
										actualBucket.Key, subName, expectedSubAgg.Value, actualSubAgg.Value)
								}
							}
						}
					}
				}
			}
		})
	}
}
