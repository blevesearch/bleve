//	Copyright (c) 2023 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package searcher

import (
	"context"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/analysis/token/synonym"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// closeSearchers closes the searchers passed to it.
// called when there is an error building the searchers.
// It returns the first error encountered while closing the searchers.
// If no error is encountered, it returns nil.
func closeSearchers(err *error, searchers map[int][]search.Searcher) {
	if *err != nil {
		for _, sa := range searchers {
			for _, s := range sa {
				s.Close()
			}
		}
	}
}

// splitOnSpace splits the phrase on space and returns a slice of strings.
// This is used to split the synonym phrase into individual words.
// For example, "a b c" is split into ["a", "b", "c"].
// It differs from strings.Split() in that it inserts a null character
// when two consecutive spaces are encountered.
// For example, "a  b c" is split into ["a", "", "b", "c"].
func tokenizeSynonym(phrase string) []string {
	var rv []string
	var tmp []rune
	for _, character := range phrase {
		if character == synonym.SeparatingCharacter {
			rv = append(rv, string(tmp))
			tmp = tmp[:0]
		} else {
			tmp = append(tmp, character)
		}
	}
	rv = append(rv, string(tmp))
	return rv
}

func tokenKey(token *analysis.Token) struct {
	Term     string
	Position int
} {
	return struct {
		Term     string
		Position int
	}{string(token.Term), token.Position}
}

func SynonymScorer(ctx *search.SearchContext, constituents []*search.DocumentMatch, options search.SearcherOptions) *search.DocumentMatch {
	var sum float64
	var childrenExplanations []*search.Explanation
	if options.Explain {
		childrenExplanations = make([]*search.Explanation, len(constituents))
	}

	for i, docMatch := range constituents {
		sum += docMatch.Score
		if options.Explain {
			childrenExplanations[i] = docMatch.Expl
		}
	}

	var expl *search.Explanation
	if options.Explain {
		expl = &search.Explanation{Value: sum, Message: "sum of synonyms:", Children: childrenExplanations}
	}

	// reuse constituents[0] as the return value
	rv := constituents[0]
	rv.Score = sum
	rv.Expl = expl
	rv.FieldTermLocations = search.MergeFieldTermLocations(
		rv.FieldTermLocations, constituents[1:])
	return rv
}

func NewSynonymSearcher(ctx context.Context, indexReader index.IndexReader,
	tokens analysis.TokenStream, field string, boost float64, fuzziness int,
	prefix int, operator int, options search.SearcherOptions) (search.Searcher, error) {

	uniqueTokens := make(map[struct {
		Term     string
		Position int
	}]analysis.TokenType)
	for _, token := range tokens {
		key := tokenKey(token)
		if _, exists := uniqueTokens[key]; !exists {
			uniqueTokens[key] = token.Type
		}
	}
	var err error
	searchersAtPosition := make(map[int][]search.Searcher)
	defer closeSearchers(&err, searchersAtPosition)

	for tok, typ := range uniqueTokens {
		var searcher search.Searcher
		if typ == analysis.Synonym {
			tokens := tokenizeSynonym(tok.Term)
			if len(tokens) > 1 {
				searcher, err = NewPhraseSearcher(ctx, indexReader, tokens, fuzziness, field, boost, options)
				if err != nil {
					return nil, err
				}
				searchersAtPosition[tok.Position] = append(searchersAtPosition[tok.Position], searcher)
				continue
			}
		}
		if fuzziness > 0 {
			searcher, err = NewFuzzySearcher(ctx, indexReader, tok.Term, prefix, fuzziness, field, boost, options)
			if err != nil {
				return nil, err
			}
		} else {
			searcher, err = NewTermSearcher(ctx, indexReader, tok.Term, field, boost, options)
			if err != nil {
				return nil, err
			}
		}
		searchersAtPosition[tok.Position] = append(searchersAtPosition[tok.Position], searcher)
	}

	searchers := make([]search.Searcher, len(searchersAtPosition))
	idx := 0
	overridedScorerCtx := context.WithValue(ctx, search.SynonymScorerKey, search.SynonymScorerCallbackFn(SynonymScorer))
	for _, searcher := range searchersAtPosition {
		var s search.Searcher
		if len(searcher) == 1 {
			s = searcher[0]
		} else if len(searcher) > 1 {
			s, err = NewDisjunctionSearcher(overridedScorerCtx, indexReader, searcher, 1, options)
			if err != nil {
				return nil, err
			}
		}
		searchers[idx] = s
		idx += 1
	}
	var rv search.Searcher
	switch operator {
	case 0:
		rv, err = NewDisjunctionSearcher(ctx, indexReader, searchers, 1, options)
	case 1:
		rv, err = NewConjunctionSearcher(ctx, indexReader, searchers, options)
	}
	if err != nil {
		return nil, err
	}
	return rv, nil
}
