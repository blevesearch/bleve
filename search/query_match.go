//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import (
	"encoding/json"

	"github.com/couchbaselabs/bleve/analysis"
	"github.com/couchbaselabs/bleve/document"
	"github.com/couchbaselabs/bleve/index"
)

type MatchQuery struct {
	Match    string  `json:"match"`
	Field    string  `json:"field,omitempty"`
	BoostVal float64 `json:"boost,omitempty"`
	Explain  bool    `json:"explain,omitempty"`
	Analyzer *analysis.Analyzer
	mapping  document.Mapping
}

func (q *MatchQuery) Boost() float64 {
	return q.BoostVal
}

func (q *MatchQuery) Searcher(index index.Index) (Searcher, error) {
	tokens := q.Analyzer.Analyze([]byte(q.Match))
	if len(tokens) > 0 {
		tqs := make([]Query, len(tokens))
		for i, token := range tokens {
			tqs[i] = &TermQuery{
				Term:     string(token.Term),
				Field:    q.Field,
				BoostVal: q.BoostVal,
				Explain:  q.Explain,
			}
		}
		boolQuery := &TermBooleanQuery{
			Should: &TermDisjunctionQuery{
				Terms:    tqs,
				BoostVal: q.BoostVal,
				Explain:  q.Explain,
				Min:      1,
			},
			BoostVal: q.BoostVal,
			Explain:  q.Explain,
		}
		return NewTermBooleanSearcher(index, boolQuery)
	} else {
		noneQuery := &MatchNoneQuery{BoostVal: q.BoostVal, Explain: q.Explain}
		return NewMatchNoneSearcher(index, noneQuery)
	}
}

func (q *MatchQuery) Validate() error {
	return nil
}

func (q *MatchQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Match    string  `json:"match"`
		Field    string  `json:"field,omitempty"`
		BoostVal float64 `json:"boost,omitempty"`
		Explain  bool    `json:"explain,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	q.Match = tmp.Match
	q.Field = tmp.Field
	q.BoostVal = tmp.BoostVal
	q.Explain = tmp.Explain
	q.Analyzer = q.mapping[q.Field].Analyzer
	return nil
}
