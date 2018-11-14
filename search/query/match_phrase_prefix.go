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
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/mapping"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/searcher"
	"unicode/utf8"
)

type MatchPhrasePrefixQuery struct {
	MatchPhrasePrefix string `json:"match_phrase"`
	FieldVal          string `json:"field,omitempty"`
	Analyzer          string `json:"analyzer,omitempty"`
	MaxExpansions     int    `json:"max_expansions,omitempty"`
	BoostVal          *Boost `json:"boost,omitempty"`
}

// NewMatchPhrasePrefixQuery creates a new Query object
// for matching phrases in the index.
// An Analyzer is chosen based on the field.
// Input text is analyzed using this analyzer.
// Token terms resulting from this analysis are
// used to build a search phrase.  Result documents
// must match this phrase. Queried field must have been indexed with
// IncludeTermVectors set to true.
func NewMatchPhrasePrefixQuery(MatchPhrasePrefix string) *MatchPhrasePrefixQuery {
	return &MatchPhrasePrefixQuery{
		MatchPhrasePrefix: MatchPhrasePrefix,
	}
}

func (q *MatchPhrasePrefixQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *MatchPhrasePrefixQuery) SetMaxExpansions(m int) {
	q.MaxExpansions = m
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
		phrase, err := tokenStreamToPhrasePrefix(i, field, q.MaxExpansions, tokens)
		if err != nil {
			return nil, err
		}
		phraseQuery := NewMultiPhraseQuery(phrase, field)
		phraseQuery.SetBoost(q.BoostVal.Value())
		return phraseQuery.Searcher(i, m, options)
	}
	noneQuery := NewMatchNoneQuery()
	return noneQuery.Searcher(i, m, options)
}

func tokenStreamToPhrasePrefix(indexReader index.IndexReader, field string, me int, tokens analysis.TokenStream) ([][]string, error) {
	firstPosition := int(^uint(0) >> 1)
	lastPosition := 0
	for _, token := range tokens {
		if token.Position < firstPosition {
			firstPosition = token.Position
		}
		if token.Position > lastPosition {
			lastPosition = token.Position
		}
	}
	phraseLen := lastPosition - firstPosition + 1
	if phraseLen > 0 {
		rv := make([][]string, phraseLen)
		for _, token := range tokens {
			pos := token.Position - firstPosition
			rv[pos] = append(rv[pos], string(token.Term))
		}

		lastIndex := phraseLen - 1
		prefix := rv[lastIndex][0]

		// find the terms with this prefix
		fieldDict, err := indexReader.FieldDictPrefix(field, []byte(prefix))
		if err != nil {
			return nil, err
		}
		defer func() {
			if cerr := fieldDict.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}()

		tfd, err := fieldDict.Next()
		for err == nil && tfd != nil {
			if len(prefix) < len(tfd.Term) && (me == 0 || maxExpansions(prefix, tfd.Term, me)) {
				rv[lastIndex] = append(rv[lastIndex], tfd.Term)
				if tooManyClauses(len(rv[lastIndex])) {
					return nil, tooManyClausesErr(len(rv[lastIndex]))
				}
			}
			tfd, err = fieldDict.Next()
		}
		if err != nil {
			return nil, err
		}

		return rv, nil
	}
	return nil, nil
}
func maxExpansions(s1 string, s2 string, me int) bool {
	return utf8.RuneCountInString(s2)-utf8.RuneCountInString(s1) <= me
}

func tooManyClauses(count int) bool {
	if searcher.DisjunctionMaxClauseCount != 0 && count > searcher.DisjunctionMaxClauseCount {
		return true
	}
	return false
}

func tooManyClausesErr(count int) error {
	return fmt.Errorf("TooManyClauses[%d > maxClauseCount, which is set to %d]",
		count, searcher.DisjunctionMaxClauseCount)
}
