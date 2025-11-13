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
	"math"
	"testing"

	"github.com/blevesearch/bleve/v2/numeric"
)

func TestSumAggregation(t *testing.T) {
	values := []float64{10.5, 20.0, 15.5, 30.0, 25.0}
	expectedSum := 101.0

	agg := NewSumAggregation("price")

	for _, val := range values {
		agg.StartDoc()
		// Convert to prefix-coded bytes
		i64 := numeric.Float64ToInt64(val)
		prefixCoded := numeric.MustNewPrefixCodedInt64(i64, 0)
		agg.UpdateVisitor(agg.Field(), prefixCoded)
		agg.EndDoc()
	}

	result := agg.Result()
	if result.Type != "sum" {
		t.Errorf("Expected type 'sum', got '%s'", result.Type)
	}

	actualSum := result.Value.(float64)
	if actualSum != expectedSum {
		t.Errorf("Expected sum %f, got %f", expectedSum, actualSum)
	}
}

func TestAvgAggregation(t *testing.T) {
	values := []float64{10.0, 20.0, 30.0, 40.0, 50.0}
	expectedAvg := 30.0

	agg := NewAvgAggregation("rating")

	for _, val := range values {
		agg.StartDoc()
		i64 := numeric.Float64ToInt64(val)
		prefixCoded := numeric.MustNewPrefixCodedInt64(i64, 0)
		agg.UpdateVisitor(agg.Field(), prefixCoded)
		agg.EndDoc()
	}

	result := agg.Result()
	actualAvg := result.Value.(float64)
	if math.Abs(actualAvg-expectedAvg) > 0.0001 {
		t.Errorf("Expected avg %f, got %f", expectedAvg, actualAvg)
	}
}

func TestMinAggregation(t *testing.T) {
	values := []float64{10.5, 20.0, 5.5, 30.0, 25.0}
	expectedMin := 5.5

	agg := NewMinAggregation("price")

	for _, val := range values {
		agg.StartDoc()
		i64 := numeric.Float64ToInt64(val)
		prefixCoded := numeric.MustNewPrefixCodedInt64(i64, 0)
		agg.UpdateVisitor(agg.Field(), prefixCoded)
		agg.EndDoc()
	}

	result := agg.Result()
	actualMin := result.Value.(float64)
	if actualMin != expectedMin {
		t.Errorf("Expected min %f, got %f", expectedMin, actualMin)
	}
}

func TestMaxAggregation(t *testing.T) {
	values := []float64{10.5, 20.0, 15.5, 30.0, 25.0}
	expectedMax := 30.0

	agg := NewMaxAggregation("price")

	for _, val := range values {
		agg.StartDoc()
		i64 := numeric.Float64ToInt64(val)
		prefixCoded := numeric.MustNewPrefixCodedInt64(i64, 0)
		agg.UpdateVisitor(agg.Field(), prefixCoded)
		agg.EndDoc()
	}

	result := agg.Result()
	actualMax := result.Value.(float64)
	if actualMax != expectedMax {
		t.Errorf("Expected max %f, got %f", expectedMax, actualMax)
	}
}

func TestCountAggregation(t *testing.T) {
	values := []float64{10.5, 20.0, 15.5, 30.0, 25.0}
	expectedCount := int64(5)

	agg := NewCountAggregation("items")

	for _, val := range values {
		agg.StartDoc()
		i64 := numeric.Float64ToInt64(val)
		prefixCoded := numeric.MustNewPrefixCodedInt64(i64, 0)
		agg.UpdateVisitor(agg.Field(), prefixCoded)
		agg.EndDoc()
	}

	result := agg.Result()
	actualCount := result.Value.(int64)
	if actualCount != expectedCount {
		t.Errorf("Expected count %d, got %d", expectedCount, actualCount)
	}
}

func TestSumSquaresAggregation(t *testing.T) {
	values := []float64{2.0, 3.0, 4.0}
	expectedSumSquares := 29.0 // 4 + 9 + 16

	agg := NewSumSquaresAggregation("values")

	for _, val := range values {
		agg.StartDoc()
		i64 := numeric.Float64ToInt64(val)
		prefixCoded := numeric.MustNewPrefixCodedInt64(i64, 0)
		agg.UpdateVisitor(agg.Field(), prefixCoded)
		agg.EndDoc()
	}

	result := agg.Result()
	actualSumSquares := result.Value.(float64)
	if math.Abs(actualSumSquares-expectedSumSquares) > 0.0001 {
		t.Errorf("Expected sum of squares %f, got %f", expectedSumSquares, actualSumSquares)
	}
}

func TestStatsAggregation(t *testing.T) {
	values := []float64{2.0, 4.0, 6.0, 8.0, 10.0}
	expectedCount := int64(5)
	expectedSum := 30.0
	expectedAvg := 6.0
	expectedMin := 2.0
	expectedMax := 10.0

	agg := NewStatsAggregation("values")

	for _, val := range values {
		agg.StartDoc()
		i64 := numeric.Float64ToInt64(val)
		prefixCoded := numeric.MustNewPrefixCodedInt64(i64, 0)
		agg.UpdateVisitor(agg.Field(), prefixCoded)
		agg.EndDoc()
	}

	result := agg.Result()
	stats := result.Value.(*StatsResult)

	if stats.Count != expectedCount {
		t.Errorf("Expected count %d, got %d", expectedCount, stats.Count)
	}

	if math.Abs(stats.Sum-expectedSum) > 0.0001 {
		t.Errorf("Expected sum %f, got %f", expectedSum, stats.Sum)
	}

	if math.Abs(stats.Avg-expectedAvg) > 0.0001 {
		t.Errorf("Expected avg %f, got %f", expectedAvg, stats.Avg)
	}

	if stats.Min != expectedMin {
		t.Errorf("Expected min %f, got %f", expectedMin, stats.Min)
	}

	if stats.Max != expectedMax {
		t.Errorf("Expected max %f, got %f", expectedMax, stats.Max)
	}

	// Variance for [2, 4, 6, 8, 10] should be 8.0
	// Mean = 6, squared differences: 16, 4, 0, 4, 16 = 40, variance = 40/5 = 8
	expectedVariance := 8.0
	if math.Abs(stats.Variance-expectedVariance) > 0.0001 {
		t.Errorf("Expected variance %f, got %f", expectedVariance, stats.Variance)
	}

	expectedStdDev := math.Sqrt(expectedVariance)
	if math.Abs(stats.StdDev-expectedStdDev) > 0.0001 {
		t.Errorf("Expected stddev %f, got %f", expectedStdDev, stats.StdDev)
	}
}

func TestAggregationWithNoValues(t *testing.T) {
	agg := NewMinAggregation("empty")

	result := agg.Result()
	actualMin := result.Value.(float64)
	// When no values seen, should return 0
	if actualMin != 0 {
		t.Errorf("Expected min 0 for empty aggregation, got %f", actualMin)
	}
}

func TestAggregationIgnoresNonZeroShift(t *testing.T) {
	// Values with shift != 0 should be ignored
	agg := NewSumAggregation("price")

	// Add value with shift = 0 (should be counted)
	agg.StartDoc()
	i64 := numeric.Float64ToInt64(10.0)
	prefixCoded := numeric.MustNewPrefixCodedInt64(i64, 0)
	agg.UpdateVisitor(agg.Field(), prefixCoded)
	agg.EndDoc()

	// Add value with shift = 4 (should be ignored)
	agg.StartDoc()
	i64 = numeric.Float64ToInt64(20.0)
	prefixCoded = numeric.MustNewPrefixCodedInt64(i64, 4)
	agg.UpdateVisitor(agg.Field(), prefixCoded)
	agg.EndDoc()

	result := agg.Result()
	actualSum := result.Value.(float64)

	// Should only count the first value (10.0)
	if actualSum != 10.0 {
		t.Errorf("Expected sum 10.0 (ignoring non-zero shift), got %f", actualSum)
	}
}
