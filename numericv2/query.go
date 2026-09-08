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

package numericv2

import (
	"sort"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/blevesearch/bleve/v2/util"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// Query evaluates a numeric range against one segment's number_v2 data.
type Query struct {
	lo uint64
	hi uint64
}

// NewRangeQuery builds a query for the given range. A nil endpoint is
// unbounded; inclusiveMin defaults to true and inclusiveMax to false.
func NewRangeQuery(min, max *float64, inclusiveMin, inclusiveMax *bool) *Query {
	lo, hi := Bounds(min, max, inclusiveMin, inclusiveMax)
	return &Query{lo: lo, hi: hi}
}

// Evaluate returns the segment document numbers whose value falls in the range.
//
// The values are sorted, so two binary searches bound the matching run and
// every entry inside it is a confirmed hit -- there is no refinement pass. The
// bitset both de-duplicates (a multi-valued field can put one document in the
// run more than once) and yields document numbers in ascending order. Since the
// stored document numbers are already in segment space, the snapshot's deleted
// bitmap can be applied directly as the bitset's exclusion set.
//
// The returned bitset comes from a shared pool: the caller owns it and must
// Release it once done with it.
func (q *Query) Evaluate(data segment.NumericV2Data, deleted *roaring.Bitmap,
	numDocs uint64) *util.Bitset {
	hits := util.AcquireBitset(int(numDocs), deleted)
	if q.lo > q.hi {
		return hits
	}

	values := data.Values()
	docNums := data.DocNums()

	start := sort.Search(len(values), func(i int) bool { return values[i] >= q.lo })
	// search only the unscanned suffix
	end := start + sort.Search(len(values)-start,
		func(i int) bool { return values[start+i] > q.hi })

	for i := start; i < end; i++ {
		hits.Add(int(docNums[i]))
	}

	return hits
}
