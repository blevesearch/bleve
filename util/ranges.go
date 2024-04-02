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

package util

import (
	"sort"
)

// Merge Overlapping intervals,
// where each interval is represented as [start, end]
//
// Returned intervals are sorted based on the start
func MergeIntervals(intervals [][2]uint64) [][2]uint64 {
	// sort the intervals based on the start
	sort.Slice(intervals, func(i, j int) bool {
		return intervals[i][0] < intervals[j][0]
	})

	// merged intervals
	rv := make([][2]uint64, 0)

	for i := 0; i < len(intervals); i++ {
		interval := intervals[i]
		if i == 0 {
			rv = append(rv, interval)
			continue
		}

		n := len(rv)
		prevStart := rv[n-1][0]
		prevEnd := rv[n-1][1]
		start := interval[0]
		end := interval[1]

		// Since intervals are sorted, thus
		// start >= prevStart, so we just need to check for "end"
		if start > prevEnd { // non-overlapping intervals
			rv = append(rv, interval)
			continue
		}

		// overlapping intervals
		rv[n-1] = [2]uint64{prevStart, max(end, prevEnd)}
	}

	return rv
}

// Compute the overlap ratio of the target range with the given ranges.
func OverlapRatio(ranges [][2]uint64, targetRange [2]uint64) float32 {
	// merge overlapping ranges to avoid double counting
	mergedRanges := MergeIntervals(ranges)

	var rv float32
	var overlap uint64

	var s, e uint64                          // range start and end
	ts, te := targetRange[0], targetRange[1] // target range start and end
	var maxS, minE uint64
	for _, r := range mergedRanges {
		s, e = r[0], r[1]

		maxS, minE = max(s, ts), min(e, te)
		if maxS < minE {
			overlap += minE - maxS
		}
	}

	rv = float32(overlap) / float32(te-ts)

	return rv
}
