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

package scorch

import (
	"fmt"
	"math"

	"github.com/blevesearch/bleve/v2/numeric"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// SegmentAggregationStats holds pre-computed aggregation statistics for a segment
type SegmentAggregationStats struct {
	Field      string  `json:"field"`
	Count      int64   `json:"count"`
	Sum        float64 `json:"sum"`
	Min        float64 `json:"min"`
	Max        float64 `json:"max"`
	SumSquares float64 `json:"sum_squares"`
}

// ComputeSegmentAggregationStats computes aggregation statistics for a numeric field in a segment
func ComputeSegmentAggregationStats(seg segment.Segment, field string, deleted []uint64) (*SegmentAggregationStats, error) {
	stats := &SegmentAggregationStats{
		Field: field,
		Min:   math.MaxFloat64,
		Max:   -math.MaxFloat64,
	}

	// Create a bitmap of deleted documents for quick lookup
	deletedMap := make(map[uint64]bool)
	for _, docNum := range deleted {
		deletedMap[docNum] = true
	}

	dict, err := seg.Dictionary(field)
	if err != nil {
		return nil, err
	}
	if dict == nil {
		return stats, nil
	}

	// Iterate through all terms in the dictionary
	var postings segment.PostingsList
	var postingsItr segment.PostingsIterator

	dictItr := dict.AutomatonIterator(nil, nil, nil)
	next, err := dictItr.Next()
	for err == nil && next != nil {
		// Only process full precision values (shift = 0)
		prefixCoded := numeric.PrefixCoded(next.Term)
		shift, shiftErr := prefixCoded.Shift()
		if shiftErr == nil && shift == 0 {
			i64, parseErr := prefixCoded.Int64()
			if parseErr == nil {
				f64 := numeric.Int64ToFloat64(i64)

				// Get posting list to count occurrences
				var err1 error
				postings, err1 = dict.PostingsList([]byte(next.Term), nil, postings)
				if err1 == nil {
					postingsItr = postings.Iterator(false, false, false, postingsItr)
					nextPosting, err2 := postingsItr.Next()
					for err2 == nil && nextPosting != nil {
						// Skip deleted documents
						if !deletedMap[nextPosting.Number()] {
							stats.Count++
							stats.Sum += f64
							stats.SumSquares += f64 * f64
							if f64 < stats.Min {
								stats.Min = f64
							}
							if f64 > stats.Max {
								stats.Max = f64
							}
						}
						nextPosting, err2 = postingsItr.Next()
					}
					if err2 != nil {
						return nil, err2
					}
				}
			}
		}
		next, err = dictItr.Next()
	}

	// If no values found, reset min/max to 0
	if stats.Count == 0 {
		stats.Min = 0
		stats.Max = 0
	}

	return stats, nil
}

// GetOrComputeSegmentStats retrieves cached stats or computes them if not available
func GetOrComputeSegmentStats(ss *SegmentSnapshot, field string) (*SegmentAggregationStats, error) {
	cacheKey := fmt.Sprintf("agg_stats_%s", field)

	// Try to fetch from cache
	if cached := ss.cachedMeta.fetchMeta(cacheKey); cached != nil {
		if stats, ok := cached.(*SegmentAggregationStats); ok {
			return stats, nil
		}
	}

	// Compute stats
	var deleted []uint64
	if ss.deleted != nil {
		deletedArray := ss.deleted.ToArray()
		deleted = make([]uint64, len(deletedArray))
		for i, d := range deletedArray {
			deleted[i] = uint64(d)
		}
	}

	stats, err := ComputeSegmentAggregationStats(ss.segment, field, deleted)
	if err != nil {
		return nil, err
	}

	// Cache the results
	ss.cachedMeta.updateMeta(cacheKey, stats)

	return stats, nil
}

// MergeSegmentStats merges multiple segment stats into a single result
func MergeSegmentStats(segmentStats []*SegmentAggregationStats) *SegmentAggregationStats {
	if len(segmentStats) == 0 {
		return &SegmentAggregationStats{}
	}

	merged := &SegmentAggregationStats{
		Field: segmentStats[0].Field,
		Min:   math.MaxFloat64,
		Max:   -math.MaxFloat64,
	}

	for _, stats := range segmentStats {
		if stats.Count > 0 {
			merged.Count += stats.Count
			merged.Sum += stats.Sum
			merged.SumSquares += stats.SumSquares
			if stats.Min < merged.Min {
				merged.Min = stats.Min
			}
			if stats.Max > merged.Max {
				merged.Max = stats.Max
			}
		}
	}

	// If no values found, reset min/max to 0
	if merged.Count == 0 {
		merged.Min = 0
		merged.Max = 0
	}

	return merged
}
