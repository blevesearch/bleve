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

	"github.com/couchbaselabs/bleve/search"
)

type ConjunctionQuery struct {
	Conjuncts []Query `json:"terms"`
	BoostVal  float64 `json:"boost,omitempty"`
}

func NewConjunctionQuery(conjuncts []Query) *ConjunctionQuery {
	return &ConjunctionQuery{
		Conjuncts: conjuncts,
		BoostVal:  1.0,
	}
}

func (q *ConjunctionQuery) Boost() float64 {
	return q.BoostVal
}

func (q *ConjunctionQuery) SetBoost(b float64) *ConjunctionQuery {
	q.BoostVal = b
	return q
}

func (q *ConjunctionQuery) AddQuery(aq Query) *ConjunctionQuery {
	q.Conjuncts = append(q.Conjuncts, aq)
	return q
}

func (q *ConjunctionQuery) Searcher(i *indexImpl, explain bool) (search.Searcher, error) {
	searchers := make([]search.Searcher, len(q.Conjuncts))
	for in, conjunct := range q.Conjuncts {
		var err error
		searchers[in], err = conjunct.Searcher(i, explain)
		if err != nil {
			return nil, err
		}
	}
	return search.NewConjunctionSearcher(i.i, searchers, explain)
}

func (q *ConjunctionQuery) Validate() error {
	return nil
}

func (q *ConjunctionQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Conjuncts []json.RawMessage `json:"terms"`
		BoostVal  float64           `json:"boost,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	q.Conjuncts = make([]Query, len(tmp.Conjuncts))
	for i, term := range tmp.Conjuncts {
		query, err := ParseQuery(term)
		if err != nil {
			return err
		}
		q.Conjuncts[i] = query
	}
	q.BoostVal = tmp.BoostVal
	if q.BoostVal == 0 {
		q.BoostVal = 1
	}
	return nil
}
