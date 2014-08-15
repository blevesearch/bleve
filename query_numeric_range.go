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
	"github.com/couchbaselabs/bleve/search"
)

type NumericRangeQuery struct {
	Min      *float64 `json:"min,omitempty"`
	Max      *float64 `json:"max,omitempty"`
	FieldVal string   `json:"field,omitempty"`
	BoostVal float64  `json:"boost,omitempty"`
}

func NewNumericRangeQuery(min, max *float64) *NumericRangeQuery {
	return &NumericRangeQuery{
		Min:      min,
		Max:      max,
		BoostVal: 1.0,
	}
}

func (q *NumericRangeQuery) Boost() float64 {
	return q.BoostVal
}

func (q *NumericRangeQuery) SetBoost(b float64) *NumericRangeQuery {
	q.BoostVal = b
	return q
}

func (q *NumericRangeQuery) Field() string {
	return q.FieldVal
}

func (q *NumericRangeQuery) SetField(f string) *NumericRangeQuery {
	q.FieldVal = f
	return q
}

func (q *NumericRangeQuery) Searcher(i *indexImpl, explain bool) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = i.m.DefaultField
	}
	return search.NewNumericRangeSearcher(i.i, q.Min, q.Max, field, q.BoostVal, explain)
}

func (q *NumericRangeQuery) Validate() error {
	if q.Min == nil && q.Min == q.Max {
		return ERROR_NUMERIC_QUERY_NO_BOUNDS
	}
	return nil
}
