//  Copyright (c) 2017 Couchbase, Inc.
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
	"fmt"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

type TermRangeQuery struct {
	Min          string `json:"min,omitempty"`
	Max          string `json:"max,omitempty"`
	InclusiveMin *bool  `json:"inclusive_min,omitempty"`
	InclusiveMax *bool  `json:"inclusive_max,omitempty"`
	FieldVal     string `json:"field,omitempty"`
	BoostVal     *Boost `json:"boost,omitempty"`
}

// NewTermRangeQuery creates a new Query for ranges
// of text term values.
// Either, but not both endpoints can be nil.
// The minimum value is inclusive.
// The maximum value is exclusive.
func NewTermRangeQuery(min, max string) *TermRangeQuery {
	return NewTermRangeInclusiveQuery(min, max, nil, nil)
}

// NewTermRangeInclusiveQuery creates a new Query for ranges
// of numeric values.
// Either, but not both endpoints can be nil.
// Control endpoint inclusion with inclusiveMin, inclusiveMax.
func NewTermRangeInclusiveQuery(min, max string, minInclusive, maxInclusive *bool) *TermRangeQuery {
	return &TermRangeQuery{
		Min:          min,
		Max:          max,
		InclusiveMin: minInclusive,
		InclusiveMax: maxInclusive,
	}
}

func (q *TermRangeQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *TermRangeQuery) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *TermRangeQuery) SetField(f string) {
	q.FieldVal = f
}

func (q *TermRangeQuery) Field() string {
	return q.FieldVal
}

func (q *TermRangeQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}
	var minTerm []byte
	if q.Min != "" {
		minTerm = []byte(q.Min)
	}
	var maxTerm []byte
	if q.Max != "" {
		maxTerm = []byte(q.Max)
	}
	return searcher.NewTermRangeSearcher(i, minTerm, maxTerm, q.InclusiveMin, q.InclusiveMax, field, q.BoostVal.Value(), options)
}

func (q *TermRangeQuery) Validate() error {
	if q.Min == "" && q.Min == q.Max {
		return fmt.Errorf("term range query must specify min or max")
	}
	return nil
}
