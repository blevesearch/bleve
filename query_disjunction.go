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
	"encoding/json"

	"github.com/blevesearch/bleve/search"
)

type disjunctionQuery struct {
	Disjuncts []Query `json:"disjuncts"`
	BoostVal  float64 `json:"boost,omitempty"`
	MinVal    float64 `json:"min"`
}

func NewDisjunctionQuery(disjuncts []Query) *disjunctionQuery {
	return &disjunctionQuery{
		Disjuncts: disjuncts,
		BoostVal:  1.0,
	}
}

func NewDisjunctionQueryMin(disjuncts []Query, min float64) *disjunctionQuery {
	if len(disjuncts) == 0 {
		return nil
	}
	return &disjunctionQuery{
		Disjuncts: disjuncts,
		BoostVal:  1.0,
		MinVal:    min,
	}
}

func (q *disjunctionQuery) Boost() float64 {
	return q.BoostVal
}

func (q *disjunctionQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *disjunctionQuery) AddQuery(aq Query) Query {
	q.Disjuncts = append(q.Disjuncts, aq)
	return q
}

func (q *disjunctionQuery) Min() float64 {
	return q.MinVal
}

func (q *disjunctionQuery) SetMin(m float64) Query {
	q.MinVal = m
	return q
}

func (q *disjunctionQuery) Searcher(i *indexImpl, explain bool) (search.Searcher, error) {
	searchers := make([]search.Searcher, len(q.Disjuncts))
	for in, disjunct := range q.Disjuncts {
		var err error
		searchers[in], err = disjunct.Searcher(i, explain)
		if err != nil {
			return nil, err
		}
	}
	return search.NewDisjunctionSearcher(i.i, searchers, q.MinVal, explain)
}

func (q *disjunctionQuery) Validate() error {
	if int(q.MinVal) > len(q.Disjuncts) {
		return ERROR_DISJUNCTION_FEWER_THAN_MIN_CLAUSES
	}
	return nil
}

func (q *disjunctionQuery) UnmarshalJSON(data []byte) error {
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

func (q *disjunctionQuery) Field() string {
	return ""
}

func (q *disjunctionQuery) SetField(f string) Query {
	return q
}
