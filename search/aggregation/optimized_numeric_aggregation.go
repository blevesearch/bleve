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

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

// SegmentStatsProvider interface for accessing segment-level statistics
type SegmentStatsProvider interface {
	GetSegmentStats(field string) ([]SegmentStats, error)
}

// SegmentStats represents pre-computed stats for a segment
type SegmentStats struct {
	Count      int64
	Sum        float64
	Min        float64
	Max        float64
	SumSquares float64
}

// IsMatchAllQuery checks if a query matches all documents
func IsMatchAllQuery(q query.Query) bool {
	if q == nil {
		return false
	}

	switch q.(type) {
	case *query.MatchAllQuery:
		return true
	default:
		return false
	}
}

// TryOptimizedAggregation attempts to compute aggregations using segment-level stats
// Returns true if optimization was successful, false if fallback to normal aggregation is needed
func TryOptimizedAggregation(
	q query.Query,
	indexReader index.IndexReader,
	aggregationsBuilder *search.AggregationsBuilder,
) (map[string]*search.AggregationResult, bool) {
	// Only optimize for match-all queries
	if !IsMatchAllQuery(q) {
		return nil, false
	}

	// Check if the index reader supports segment stats
	statsProvider, ok := indexReader.(SegmentStatsProvider)
	if !ok {
		return nil, false
	}

	// Try to compute optimized results for all aggregations
	results := make(map[string]*search.AggregationResult)

	// We would need to access internal aggregation state, which isn't exposed
	// For now, return false to indicate we can't optimize
	// This is a placeholder for future enhancement where we can pass aggregation
	// configurations separately
	_ = statsProvider
	_ = results

	return nil, false
}

// OptimizedStatsAggregation is a wrapper that can use segment-level stats
type OptimizedStatsAggregation struct {
	*StatsAggregation
	useOptimization bool
	segmentStats    []SegmentStats
}

// NewOptimizedStatsAggregation creates an optimized stats aggregation
func NewOptimizedStatsAggregation(field string) *OptimizedStatsAggregation {
	return &OptimizedStatsAggregation{
		StatsAggregation: NewStatsAggregation(field),
	}
}

// EnableOptimization enables the use of pre-computed segment stats
func (osa *OptimizedStatsAggregation) EnableOptimization(stats []SegmentStats) {
	osa.useOptimization = true
	osa.segmentStats = stats
}

// Result returns the aggregation result, using optimized path if enabled
func (osa *OptimizedStatsAggregation) Result() *search.AggregationResult {
	if osa.useOptimization && len(osa.segmentStats) > 0 {
		return osa.optimizedResult()
	}
	return osa.StatsAggregation.Result()
}

func (osa *OptimizedStatsAggregation) optimizedResult() *search.AggregationResult {
	result := &StatsResult{}
	minInitialized := false
	maxInitialized := false

	// Merge all segment stats
	for _, stats := range osa.segmentStats {
		result.Count += stats.Count
		result.Sum += stats.Sum
		result.SumSquares += stats.SumSquares

		if stats.Count > 0 {
			// Use proper initialization tracking instead of checking for zero
			if !minInitialized || stats.Min < result.Min {
				result.Min = stats.Min
				minInitialized = true
			}
			if !maxInitialized || stats.Max > result.Max {
				result.Max = stats.Max
				maxInitialized = true
			}
		}
	}

	if result.Count > 0 {
		result.Avg = result.Sum / float64(result.Count)

		// Calculate variance and standard deviation
		avgSquares := result.SumSquares / float64(result.Count)
		result.Variance = avgSquares - (result.Avg * result.Avg)
		if result.Variance < 0 {
			result.Variance = 0
		}
		result.StdDev = math.Sqrt(result.Variance)
	}

	return &search.AggregationResult{
		Field: osa.field,
		Type:  "stats",
		Value: result,
	}
}
