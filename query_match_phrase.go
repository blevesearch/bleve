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
	"fmt"

	"github.com/couchbaselabs/bleve/analysis"
	"github.com/couchbaselabs/bleve/search"
)

type MatchPhraseQuery struct {
	MatchPhrase string  `json:"match_phrase"`
	FieldVal    string  `json:"field,omitempty"`
	Analyzer    string  `json:"analyzer,omitempty"`
	BoostVal    float64 `json:"boost,omitempty"`
}

func NewMatchPhraseQuery(matchPhrase string) *MatchPhraseQuery {
	return &MatchPhraseQuery{
		MatchPhrase: matchPhrase,
		BoostVal:    1.0,
	}
}

func (q *MatchPhraseQuery) Boost() float64 {
	return q.BoostVal
}

func (q *MatchPhraseQuery) SetBoost(b float64) *MatchPhraseQuery {
	q.BoostVal = b
	return q
}

func (q *MatchPhraseQuery) Field() string {
	return q.FieldVal
}

func (q *MatchPhraseQuery) SetField(f string) *MatchPhraseQuery {
	q.FieldVal = f
	return q
}

func (q *MatchPhraseQuery) Searcher(i *indexImpl, explain bool) (search.Searcher, error) {

	var analyzer *analysis.Analyzer
	if q.Analyzer != "" {
		analyzer = Config.Analysis.Analyzers[q.Analyzer]
	} else {
		analyzer = i.m.analyzerForPath(q.FieldVal)
	}
	if analyzer == nil {
		return nil, fmt.Errorf("no analyzer named '%s' registered", q.Analyzer)
	}

	tokens := analyzer.Analyze([]byte(q.MatchPhrase))
	if len(tokens) > 0 {
		tqs := make([]*TermQuery, len(tokens))
		for i, token := range tokens {
			tqs[i] = NewTermQuery(string(token.Term)).
				SetField(q.FieldVal).
				SetBoost(q.BoostVal)
		}

		phraseQuery := NewPhraseQuery(tqs)

		return phraseQuery.Searcher(i, explain)
	} else {
		noneQuery := NewMatchNoneQuery()
		return noneQuery.Searcher(i, explain)
	}
}

func (q *MatchPhraseQuery) Validate() error {
	return nil
}
