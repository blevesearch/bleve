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

	"github.com/couchbaselabs/bleve/search"
)

type MatchQuery struct {
	Match    string  `json:"match"`
	FieldVal string  `json:"field,omitempty"`
	Analyzer string  `json:"analyzer,omitempty"`
	BoostVal float64 `json:"boost,omitempty"`
}

func NewMatchQuery(match string) *MatchQuery {
	return &MatchQuery{
		Match:    match,
		BoostVal: 1.0,
	}
}

func (q *MatchQuery) Boost() float64 {
	return q.BoostVal
}

func (q *MatchQuery) SetBoost(b float64) *MatchQuery {
	q.BoostVal = b
	return q
}

func (q *MatchQuery) Field() string {
	return q.FieldVal
}

func (q *MatchQuery) SetField(f string) *MatchQuery {
	q.FieldVal = f
	return q
}

func (q *MatchQuery) Searcher(i *indexImpl, explain bool) (search.Searcher, error) {

	analyzerName := ""
	if q.Analyzer != "" {
		analyzerName = q.Analyzer
	} else {
		analyzerName = i.m.analyzerNameForPath(q.FieldVal)
	}
	analyzer := i.m.AnalyzerNamed(analyzerName)

	if analyzer == nil {
		return nil, fmt.Errorf("no analyzer named '%s' registered", q.Analyzer)
	}

	field := q.FieldVal
	if q.FieldVal == "" {
		field = i.m.DefaultField
	}

	tokens := analyzer.Analyze([]byte(q.Match))
	if len(tokens) > 0 {

		tqs := make([]Query, len(tokens))
		for i, token := range tokens {
			tqs[i] = NewTermQuery(string(token.Term)).
				SetField(field).
				SetBoost(q.BoostVal)
		}

		shouldQuery := NewDisjunctionQuery(tqs).
			SetBoost(q.BoostVal).
			SetMin(1)

		return shouldQuery.Searcher(i, explain)
	} else {
		noneQuery := NewMatchNoneQuery()
		return noneQuery.Searcher(i, explain)
	}
}

func (q *MatchQuery) Validate() error {
	return nil
}
