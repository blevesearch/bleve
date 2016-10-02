//  Copyright (c) 2014 Couchbase, Inc.
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
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searcher"
)

type FuzzyQuery struct {
	Term      string `json:"term"`
	Prefix    int    `json:"prefix_length"`
	Fuzziness int    `json:"fuzziness"`
	Field     string `json:"field,omitempty"`
	Boost     *Boost `json:"boost,omitempty"`
}

// NewFuzzyQuery creates a new Query which finds
// documents containing terms within a specific
// fuzziness of the specified term.
// The default fuzziness is 2.
//
// The current implementation uses Levenshtein edit
// distance as the fuzziness metric.
func NewFuzzyQuery(term string) *FuzzyQuery {
	return &FuzzyQuery{
		Term:      term,
		Fuzziness: 2,
	}
}

func (q *FuzzyQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.Boost = &boost
}

func (q *FuzzyQuery) SetField(f string) {
	q.Field = f
}

func (q *FuzzyQuery) SetFuzziness(f int) {
	q.Fuzziness = f
}

func (q *FuzzyQuery) SetPrefix(p int) {
	q.Prefix = p
}

func (q *FuzzyQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, explain bool) (search.Searcher, error) {
	field := q.Field
	if q.Field == "" {
		field = m.DefaultSearchField()
	}
	return searcher.NewFuzzySearcher(i, q.Term, q.Prefix, q.Fuzziness, field, q.Boost.Value(), explain)
}
