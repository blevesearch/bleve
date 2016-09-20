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

type PhraseQuery struct {
	Terms       []string `json:"terms"`
	FieldVal    string   `json:"field,omitempty"`
	BoostVal    float64  `json:"boost,omitempty"`
	termQueries []Query
}

// NewPhraseQuery creates a new Query for finding
// exact term phrases in the index.
// The provided terms must exist in the correct
// order, at the correct index offsets, in the
// specified field. Queried field must have been indexed with
// IncludeTermVectors set to true.
func NewPhraseQuery(terms []string, field string) *PhraseQuery {
	termQueries := make([]Query, 0)
	for _, term := range terms {
		if term != "" {
			termQueries = append(termQueries, NewTermQuery(term).SetField(field))
		}
	}
	return &PhraseQuery{
		Terms:       terms,
		FieldVal:    field,
		BoostVal:    1.0,
		termQueries: termQueries,
	}
}

func (q *PhraseQuery) Boost() float64 {
	return q.BoostVal
}

func (q *PhraseQuery) SetBoost(b float64) Query {
	q.BoostVal = b
	return q
}

func (q *PhraseQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, explain bool) (search.Searcher, error) {

	conjunctionQuery := NewConjunctionQuery(q.termQueries)
	conjunctionSearcher, err := conjunctionQuery.Searcher(i, m, explain)
	if err != nil {
		return nil, err
	}
	return searchers.NewPhraseSearcher(i, conjunctionSearcher.(*searchers.ConjunctionSearcher), q.Terms)
}

func (q *PhraseQuery) Validate() error {
	if len(q.termQueries) < 1 {
		return fmt.Errorf("phrase query must contain at least one term")
	}
	return nil
}

func (q *PhraseQuery) UnmarshalJSON(data []byte) error {
	type _phraseQuery PhraseQuery
	tmp := _phraseQuery{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	q.Terms = tmp.Terms
	q.FieldVal = tmp.FieldVal
	q.BoostVal = tmp.BoostVal
	if q.BoostVal == 0 {
		q.BoostVal = 1
	}
	q.termQueries = make([]Query, len(q.Terms))
	for i, term := range q.Terms {
		q.termQueries[i] = &TermQuery{Term: term, FieldVal: q.FieldVal, BoostVal: q.BoostVal}
	}
	return nil
}

func (q *PhraseQuery) Field() string {
	return ""
}

func (q *PhraseQuery) SetField(f string) Query {
	return q
}
