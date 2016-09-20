//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package query

import (
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searchers"
)

type DisjunctionQuery struct {
	Disjuncts []Query `json:"disjuncts"`
	BoostVal  float64 `json:"boost,omitempty"`
	MinVal    float64 `json:"min"`
}

// NewDisjunctionQuery creates a new compound Query.
// Result documents satisfy at least one Query.
func NewDisjunctionQuery(disjuncts []Query) *DisjunctionQuery {
	return &DisjunctionQuery{
		Disjuncts: disjuncts,
		BoostVal:  1.0,
	}
}

// NewDisjunctionQueryMin creates a new compound Query.
// Result documents satisfy at least min Queries.
func NewDisjunctionQueryMin(disjuncts []Query, min float64) *DisjunctionQuery {
	return &DisjunctionQuery{
		Disjuncts: disjuncts,
		BoostVal:  1.0,
		MinVal:    min,
	}
}

func (q *DisjunctionQuery) Boost() float64 {
	return q.BoostVal
}

func (q *DisjunctionQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *DisjunctionQuery) AddQuery(aq Query) Query {
	q.Disjuncts = append(q.Disjuncts, aq)
	return q
}

func (q *DisjunctionQuery) Min() float64 {
	return q.MinVal
}

func (q *DisjunctionQuery) SetMin(m float64) Query {
	q.MinVal = m
	return q
}

func (q *DisjunctionQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, explain bool) (search.Searcher, error) {
	ss := make([]search.Searcher, len(q.Disjuncts))
	for in, disjunct := range q.Disjuncts {
		var err error
		ss[in], err = disjunct.Searcher(i, m, explain)
		if err != nil {
			return nil, err
		}
	}
	return searchers.NewDisjunctionSearcher(i, ss, q.MinVal, explain)
}

func (q *DisjunctionQuery) Validate() error {
	if int(q.MinVal) > len(q.Disjuncts) {
		return fmt.Errorf("disjunction query has fewer than the minimum number of clauses to satisfy")
	}
	for _, q := range q.Disjuncts {
		err := q.Validate()
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *DisjunctionQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Disjuncts []json.RawMessage `json:"disjuncts"`
		BoostVal  float64           `json:"boost,omitempty"`
		MinVal    float64           `json:"min"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	q.Disjuncts = make([]Query, len(tmp.Disjuncts))
	for i, term := range tmp.Disjuncts {
		query, err := ParseQuery(term)
		if err != nil {
			return err
		}
		q.Disjuncts[i] = query
	}
	q.BoostVal = tmp.BoostVal
	if q.BoostVal == 0 {
		q.BoostVal = 1
	}
	q.MinVal = tmp.MinVal
	return nil
}

func (q *DisjunctionQuery) Field() string {
	return ""
}

func (q *DisjunctionQuery) SetField(f string) Query {
	return q
}
