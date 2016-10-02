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

type TermQuery struct {
	Term  string `json:"term"`
	Field string `json:"field,omitempty"`
	Boost *Boost `json:"boost,omitempty"`
}

// NewTermQuery creates a new Query for finding an
// exact term match in the index.
func NewTermQuery(term string) *TermQuery {
	return &TermQuery{
		Term: term,
	}
}

func (q *TermQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.Boost = &boost
}

func (q *TermQuery) SetField(f string) {
	q.Field = f
}

func (q *TermQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, explain bool) (search.Searcher, error) {
	field := q.Field
	if q.Field == "" {
		field = m.DefaultSearchField()
	}
	return searcher.NewTermSearcher(i, q.Term, field, q.Boost.Value(), explain)
}
