//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

import (
	"fmt"
	"math"
	"time"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/numeric_util"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searchers"
)

type dateRangeQuery struct {
	Start          *time.Time `json:"start,omitempty"`
	End            *time.Time `json:"end,omitempty"`
	InclusiveStart *bool      `json:"inclusive_start,omitempty"`
	InclusiveEnd   *bool      `json:"inclusive_end,omitempty"`
	FieldVal       string     `json:"field,omitempty"`
	BoostVal       float64    `json:"boost,omitempty"`
}

// NewDateRangeQuery creates a new Query for ranges
// of date values.
// The range matches time t as:  start >= t < end
// That is, the lower bound inclusive, and the upper bound is exclusive.
// Either, but not both endpoints can be the zero time, in which case the
// range becomes a one-sided greater-than-or-equal or less-than comparison.
func NewDateRangeQuery(start, end time.Time) *dateRangeQuery {
	return NewDateRangeInclusiveQuery(start, end, true, false)
}

// NewDateRangeInclusiveQuery creates a new Query for ranges
// of date values.
// Either, but not both endpoints can be the zero time.
// startInclusive and endInclusive control inclusion of the endpoints.
func NewDateRangeInclusiveQuery(start, end time.Time, startInclusive, endInclusive bool) *dateRangeQuery {

	q := &dateRangeQuery{
		BoostVal: 1.0,
	}

	if !start.IsZero() {
		q.Start = &start
		q.InclusiveStart = &startInclusive
	}
	if !end.IsZero() {
		q.End = &end
		q.InclusiveEnd = &endInclusive
	}

	return q
}

func (q *dateRangeQuery) Boost() float64 {
	return q.BoostVal
}

func (q *dateRangeQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *dateRangeQuery) Field() string {
	return q.FieldVal
}

func (q *dateRangeQuery) SetField(f string) Query {
	q.FieldVal = f
	return q
}

func (q *dateRangeQuery) Searcher(i index.IndexReader, m *IndexMapping, explain bool) (search.Searcher, error) {

	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultField
	}

	// use +/- infinity for missing endpoints
	min := math.Inf(-1)
	if q.Start != nil {
		min = numeric_util.Int64ToFloat64((*q.Start).UnixNano())
	}
	max := math.Inf(1)
	if q.End != nil {
		max = numeric_util.Int64ToFloat64((*q.End).UnixNano())
	}

	return searchers.NewNumericRangeSearcher(i, &min, &max, q.InclusiveStart, q.InclusiveEnd, field, q.BoostVal, explain)
}

func (q *dateRangeQuery) Validate() error {
	if q.Start == nil && q.Start == q.End {
		return fmt.Errorf("must specify start or end")
	}
	return nil
}
