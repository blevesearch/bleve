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

type fuzzyQuery struct {
	Term         string  `json:"term"`
	PrefixVal    int     `json:"prefix_length"`
	FuzzinessVal int     `json:"fuzziness"`
	FieldVal     string  `json:"field,omitempty"`
	BoostVal     float64 `json:"boost,omitempty"`
}

// NewFuzzyQuery creates a new Query which finds
// documents containing terms within a specific
// fuzziness of the specified term.
// The default fuzziness is 2.
//
// The current implementation uses Levenshtein edit
// distance as the fuzziness metric.
func NewFuzzyQuery(term string) *fuzzyQuery {
	return &fuzzyQuery{
		Term:         term,
		PrefixVal:    0,
		FuzzinessVal: 2,
		BoostVal:     1.0,
	}
}

func (q *fuzzyQuery) Boost() float64 {
	return q.BoostVal
}

func (q *fuzzyQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *fuzzyQuery) Field() string {
	return q.FieldVal
}

func (q *fuzzyQuery) SetField(f string) Query {
	q.FieldVal = f
	return q
}

func (q *fuzzyQuery) Fuzziness() int {
	return q.FuzzinessVal
}

func (q *fuzzyQuery) SetFuzziness(f int) Query {
	q.FuzzinessVal = f
	return q
}

func (q *fuzzyQuery) Prefix() int {
	return q.PrefixVal
}

func (q *fuzzyQuery) SetPrefix(p int) Query {
	q.PrefixVal = p
	return q
}

func (q *fuzzyQuery) Searcher(i index.IndexReader, m *IndexMapping, explain bool) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultField
	}
	return searchers.NewFuzzySearcher(i, q.Term, q.PrefixVal, q.FuzzinessVal, field, q.BoostVal, explain)
}

func (q *fuzzyQuery) Validate() error {
	return nil
}
