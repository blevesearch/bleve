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

package query

import (
	"context"
	"fmt"
	"math"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

// NumericRangeV2 is the range itself. It is nested under a versioned key rather
// than living at the top level, because ParseQuery already claims top-level
// min/max for NumericRangeQuery and TermRangeQuery.
type NumericRangeV2 struct {
	Min          *float64 `json:"min,omitempty"`
	Max          *float64 `json:"max,omitempty"`
	InclusiveMin *bool    `json:"inclusive_min,omitempty"`
	InclusiveMax *bool    `json:"inclusive_max,omitempty"`
}

// NumericRangeV2Query searches a number_v2 field. The endpoint semantics match
// NumericRangeQuery exactly: either endpoint may be omitted for an unbounded
// range, the minimum is inclusive by default and the maximum is not.
type NumericRangeV2Query struct {
	RangeV2  NumericRangeV2 `json:"range_v2"`
	FieldVal string         `json:"field,omitempty"`
	BoostVal *Boost         `json:"boost,omitempty"`
}

// NewNumericRangeV2Query creates a new query for ranges of numeric values over
// a number_v2 field. Either, but not both, endpoints can be nil. The minimum
// value is inclusive; the maximum value is exclusive.
func NewNumericRangeV2Query(min, max *float64) *NumericRangeV2Query {
	return NewNumericRangeV2InclusiveQuery(min, max, nil, nil)
}

// NewNumericRangeV2InclusiveQuery creates a new query for ranges of numeric
// values over a number_v2 field, with explicit control over endpoint inclusion.
func NewNumericRangeV2InclusiveQuery(min, max *float64,
	minInclusive, maxInclusive *bool) *NumericRangeV2Query {
	return &NumericRangeV2Query{
		RangeV2: NumericRangeV2{
			Min:          min,
			Max:          max,
			InclusiveMin: minInclusive,
			InclusiveMax: maxInclusive,
		},
	}
}

func (q *NumericRangeV2Query) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *NumericRangeV2Query) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *NumericRangeV2Query) SetField(f string) {
	q.FieldVal = f
}

func (q *NumericRangeV2Query) Field() string {
	return q.FieldVal
}

func (q *NumericRangeV2Query) Validate() error {
	if q.RangeV2.Min == nil && q.RangeV2.Max == nil {
		return fmt.Errorf("number_v2 range query must specify min or max")
	}
	if q.RangeV2.Min != nil && math.IsNaN(*q.RangeV2.Min) {
		return fmt.Errorf("number_v2 range query min must not be NaN")
	}
	if q.RangeV2.Max != nil && math.IsNaN(*q.RangeV2.Max) {
		return fmt.Errorf("number_v2 range query max must not be NaN")
	}
	return nil
}

func (q *NumericRangeV2Query) Searcher(ctx context.Context, i index.IndexReader,
	m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}

	ctx = context.WithValue(ctx, search.QueryTypeKey, search.Numeric)

	return searcher.NewNumericV2Searcher(ctx, i, q.RangeV2.Min, q.RangeV2.Max,
		q.RangeV2.InclusiveMin, q.RangeV2.InclusiveMax, field,
		q.BoostVal.Value(), options)
}
