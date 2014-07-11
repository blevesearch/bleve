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

type MatchPhraseQuery struct {
	MatchPhrase string  `json:"match_phrase"`
	Field       string  `json:"field,omitempty"`
	BoostVal    float64 `json:"boost,omitempty"`
	Explain     bool    `json:"explain,omitempty"`
	Analyzer    *analysis.Analyzer
	mapping     document.Mapping
}

func (q *MatchPhraseQuery) Boost() float64 {
	return q.BoostVal
}

func (q *MatchPhraseQuery) Searcher(index index.Index) (Searcher, error) {
	tokens := q.Analyzer.Analyze([]byte(q.MatchPhrase))
	if len(tokens) > 0 {
		maxPos := 0
		// find the highest position index
		for _, token := range tokens {
			if int(token.Position) > maxPos {
				maxPos = int(token.Position)
			}
		}
		// use tokens to build phrase
		phraseTerms := make([]*TermQuery, maxPos)
		for _, token := range tokens {
			phraseTerms[int(token.Position)-1] = &TermQuery{
				Term:     string(token.Term),
				Field:    q.Field,
				BoostVal: q.BoostVal,
				Explain:  q.Explain,
			}
		}
		phraseQuery := &PhraseQuery{
			Terms:    phraseTerms,
			BoostVal: q.BoostVal,
			Explain:  q.Explain,
		}
		return NewPhraseSearcher(index, phraseQuery)
	} else {
		noneQuery := &MatchNoneQuery{BoostVal: q.BoostVal, Explain: q.Explain}
		return NewMatchNoneSearcher(index, noneQuery)
	}
}

func (q *MatchPhraseQuery) Validate() error {
	return nil
}

func (q *MatchPhraseQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		MatchPhrase string  `json:"match_phrase"`
		Field       string  `json:"field,omitempty"`
		BoostVal    float64 `json:"boost,omitempty"`
		Explain     bool    `json:"explain,omitempty"`
	}{}
	err := json.Unmarshal(data, &tmp)
	if err != nil {
		return err
	}
	q.MatchPhrase = tmp.MatchPhrase
	q.Field = tmp.Field
	q.BoostVal = tmp.BoostVal
	q.Explain = tmp.Explain
	q.Analyzer = q.mapping[q.Field].Analyzer
	return nil
}
