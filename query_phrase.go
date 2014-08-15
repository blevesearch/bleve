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

type PhraseQuery struct {
	Terms    []*TermQuery `json:"terms"`
	BoostVal float64      `json:"boost,omitempty"`
}

func NewPhraseQuery(terms []*TermQuery) *PhraseQuery {
	return &PhraseQuery{
		Terms:    terms,
		BoostVal: 1.0,
	}
}

func (q *PhraseQuery) Boost() float64 {
	return q.BoostVal
}

func (q *PhraseQuery) SetBoost(b float64) *PhraseQuery {
	q.BoostVal = b
	return q
}

func (q *PhraseQuery) Searcher(i *indexImpl, explain bool) (search.Searcher, error) {

	terms := make([]string, len(q.Terms))
	conjuncts := make([]Query, len(q.Terms))
	for i, term := range q.Terms {
		conjuncts[i] = term
		terms[i] = term.Term
	}

	conjunctionQuery := NewConjunctionQuery(conjuncts)
	conjunctionSearcher, err := conjunctionQuery.Searcher(i, explain)
	if err != nil {
		return nil, err
	}
	return search.NewPhraseSearcher(i.i, conjunctionSearcher, terms)
}

func (q *PhraseQuery) Validate() error {
	if len(q.Terms) < 1 {
		return ERROR_PHRASE_QUERY_NO_TERMS
	}
	return nil
}
