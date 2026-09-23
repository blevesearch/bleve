//  Copyright (c) 2026 Couchbase, Inc.
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

// Package numericv2 holds the encoding and range evaluation for number_v2
// fields, which are indexed as a single sorted array of values per segment
// rather than as prefix-coded terms in the inverted index.
package numericv2

import (
	"math"

	"github.com/blevesearch/bleve/v2/numeric"
)

// signBit lifts an order-preserving int64 into an order-preserving uint64.
// Because it is addition of 2^63 modulo 2^64, it commutes with the +1 and -1
// steps Bounds applies for exclusive endpoints.
const signBit = uint64(1) << 63

// EncodeInt64 maps a sortable int64, as produced by numeric.Float64ToInt64, to
// a uint64 whose unsigned ordering matches. This is the same transform
// numeric.PrefixCoded applies internally before splitting into 7-bit bytes.
func EncodeInt64(i int64) uint64 {
	return uint64(i) ^ signBit
}

// Encode maps a float64 to a uint64 whose unsigned ordering matches the
// float64 ordering of the input.
func Encode(f float64) uint64 {
	return EncodeInt64(numeric.Float64ToInt64(f))
}

// Decode is the inverse of Encode.
func Decode(v uint64) float64 {
	return numeric.Int64ToFloat64(int64(v ^ signBit))
}

// Bounds converts a query range into the inclusive uint64 interval [lo, hi] to
// scan. It deliberately mirrors searcher.NewNumericRangeSearcher step for step:
// an absent endpoint becomes the corresponding infinity, min is inclusive by
// default and max is not, and the adjustments for exclusive endpoints are
// guarded at the int64 extremes so they cannot wrap.
//
// A ±1 step in this space moves to the adjacent representable float64, so
// exclusive endpoints here are exact rather than approximate. When the range is
// empty, lo comes back greater than hi.
func Bounds(min, max *float64, inclusiveMin, inclusiveMax *bool) (lo, hi uint64) {
	// account for unbounded edges
	if min == nil {
		negInf := math.Inf(-1)
		min = &negInf
	}
	if max == nil {
		inf := math.Inf(1)
		max = &inf
	}
	if inclusiveMin == nil {
		defaultInclusiveMin := true
		inclusiveMin = &defaultInclusiveMin
	}
	if inclusiveMax == nil {
		defaultInclusiveMax := false
		inclusiveMax = &defaultInclusiveMax
	}

	minInt64 := numeric.Float64ToInt64(*min)
	if !*inclusiveMin && minInt64 != math.MaxInt64 {
		minInt64++
	}

	maxInt64 := numeric.Float64ToInt64(*max)
	if !*inclusiveMax && maxInt64 != math.MinInt64 {
		maxInt64--
	}

	return EncodeInt64(minInt64), EncodeInt64(maxInt64)
}
