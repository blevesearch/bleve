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
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searchers"
)

type numericRangeQuery struct {
	Min          *float64 `json:"min,omitempty"`
	Max          *float64 `json:"max,omitempty"`
	InclusiveMin *bool    `json:"inclusive_min,omitempty"`
	InclusiveMax *bool    `json:"inclusive_max,omitempty"`
	FieldVal     string   `json:"field,omitempty"`
	BoostVal     float64  `json:"boost,omitempty"`
}

// NewNumericRangeQuery creates a new Query for ranges
// of numeric values.
// Either, but not both endpoints can be nil.
// The minimum value is inclusive.
// The maximum value is exclusive.
func NewNumericRangeQuery(min, max *float64) *numericRangeQuery {
	return NewNumericRangeInclusiveQuery(min, max, nil, nil)
}

// NewNumericRangeInclusiveQuery creates a new Query for ranges
// of numeric values.
// Either, but not both endpoints can be nil.
// Control endpoint inclusion with inclusiveMin, inclusiveMax.
func NewNumericRangeInclusiveQuery(min, max *float64, minInclusive, maxInclusive *bool) *numericRangeQuery {
	return &numericRangeQuery{
		Min:          min,
		Max:          max,
		InclusiveMin: minInclusive,
		InclusiveMax: maxInclusive,
		BoostVal:     1.0,
	}
}

func (q *numericRangeQuery) Boost() float64 {
	return q.BoostVal
}

func (q *numericRangeQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *numericRangeQuery) Field() string {
	return q.FieldVal
}

func (q *numericRangeQuery) SetField(f string) Query {
	q.FieldVal = f
	return q
}

func (q *numericRangeQuery) Searcher(i index.IndexReader, m *IndexMapping, explain bool) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultField
	}
	return searchers.NewNumericRangeSearcher(i, q.Min, q.Max, q.InclusiveMin, q.InclusiveMax, field, q.BoostVal, explain)
}

func (q *numericRangeQuery) Validate() error {
	if q.Min == nil && q.Min == q.Max {
		return ErrorNumericQueryNoBounds
	}
	return nil
}
