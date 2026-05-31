//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package query

import (
	"fmt"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
)

type MatchPhrasePrefixQuery struct {
	MatchPhrasePrefix string `json:"match_phrase_prefix"`
	FieldVal          string `json:"field,omitempty"`
	Analyzer          string `json:"analyzer,omitempty"`
	BoostVal          *Boost `json:"boost,omitempty"`
}

// NewMatchPhrasePrefixQuery creates a new Query
// for matching phrase prefix in the index.
// An Analyzer is chosen based on the field.
// Input text is analyzed using this analyzer.
// Token terms resulting from this analysis are
// used to build a search phrase.  Result documents
// must match this phrase prefix. Queried field must have been indexed with
// IncludeTermVectors set to true.
func NewMatchPhrasePrefixQuery(matchPhrasePrefix string) *MatchPhrasePrefixQuery {
	return &MatchPhrasePrefixQuery{
		MatchPhrasePrefix: matchPhrasePrefix,
	}
}

func (q *MatchPhrasePrefixQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *MatchPhrasePrefixQuery) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *MatchPhrasePrefixQuery) SetField(f string) {
	q.FieldVal = f
}

func (q *MatchPhrasePrefixQuery) Field() string {
	return q.FieldVal
}

func (q *MatchPhrasePrefixQuery) Searcher(i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}

	analyzerName := ""
	if q.Analyzer != "" {
		analyzerName = q.Analyzer
	} else {
		analyzerName = m.AnalyzerNameForPath(field)
	}
	analyzer := m.AnalyzerNamed(analyzerName)
	if analyzer == nil {
		return nil, fmt.Errorf("no analyzer named '%s' registered", q.Analyzer)
	}

	tokens := analyzer.Analyze([]byte(q.MatchPhrasePrefix))
	if len(tokens) > 0 {
		phrase := tokenStreamToPhrase(tokens)
		if len(phrase) > 0 {
			// expand tokens at last position to terms from dictionary
			var terms []string
			for _, prefix := range phrase[len(phrase)-1] {
				fieldDict, err := i.FieldDictPrefix(field, []byte(prefix))
				if err != nil {
					return nil, err
				}
				tfd, err := fieldDict.Next()
				for err == nil && tfd != nil {
					terms = append(terms, tfd.Term)
					tfd, err = fieldDict.Next()
				}
			}
			if len(terms) > 0 {
				phrase[len(phrase)-1] = terms
			}
		}
		phraseQuery := NewMultiPhraseQuery(phrase, field)
		phraseQuery.SetBoost(q.BoostVal.Value())
		return phraseQuery.Searcher(i, m, options)
	}

	noneQuery := NewMatchNoneQuery()
	return noneQuery.Searcher(i, m, options)
}
