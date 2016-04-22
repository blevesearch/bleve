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

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searchers"
)

type conjunctionQuery struct {
	Conjuncts []Query `json:"conjuncts"`
	BoostVal  float64 `json:"boost,omitempty"`
}

// NewConjunctionQuery creates a new compound Query.
// Result documents must satisfy all of the queries.
func NewConjunctionQuery(conjuncts []Query) *conjunctionQuery {
	return &conjunctionQuery{
		Conjuncts: conjuncts,
		BoostVal:  1.0,
	}
}

func (q *conjunctionQuery) Boost() float64 {
	return q.BoostVal
}

func (q *conjunctionQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *conjunctionQuery) AddQuery(aq Query) *conjunctionQuery {
	q.Conjuncts = append(q.Conjuncts, aq)
	return q
}

func (q *conjunctionQuery) Searcher(i index.IndexReader, m *IndexMapping, explain bool) (search.Searcher, error) {
	ss := make([]search.Searcher, len(q.Conjuncts))
	for in, conjunct := range q.Conjuncts {
		var err error
		ss[in], err = conjunct.Searcher(i, m, explain)
		if err != nil {
			return nil, err
		}
	}
	return searchers.NewConjunctionSearcher(i, ss, explain)
}

func (q *conjunctionQuery) Validate() error {
	for _, q := range q.Conjuncts {
		err := q.Validate()
		if err != nil {
			return err
		}
	}
	return nil
}

func (q *conjunctionQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Conjuncts []json.RawMessage `json:"conjuncts"`
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

func (q *conjunctionQuery) Field() string {
	return ""
}

func (q *conjunctionQuery) SetField(f string) Query {
	return q
}
