package searcher

import (
	"context"
	"fmt"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/analysis/token/synonym"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// closeSearchers closes the searchers passed to it.
// called when there is an error building the searchers.
// It returns the first error encountered while closing the searchers.
// If no error is encountered, it returns nil.
func closeSearchers(err *error, searchers ...[]search.Searcher) {
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
func tokenizeSynonym(phrase []byte) []string {
	var rv []string
	var tmp string
	for _, character := range phrase {
		if character == synonym.SeparatingCharacter {
			rv = append(rv, tmp)
			tmp = ""
		} else {
			tmp = tmp + string(character)
		}
	}
	rv = append(rv, tmp)
	return rv
}

type synonymSearchCtxKey string

// SearchAtPosition is a struct that contains a searcher and the first and last position of the searcher in the query.
//   - Searcher is the main searcher for the token.
//   - FirstPos is the first position of the searcher.
//   - LastPos is the last position of the searcher.

// This is used for match phrase query support for synonyms, where we need to
// know the position of the first and last word in the synonym phrase,
// to ensure that the document matched has the same number of stop words
// as the query.

// Operator == 2 implements a specialized conjunction searcher.
// In addition to ensuring that all sub-searchers match a document, it also ensures that
// the sub-searchers match in the correct order in the document.
// The input sub-searchers must be in order, and must all have the same field.
// It uses two properties, first and last position, for each sub searcher,
// which ensures that the relative positioning of each sub searchers hits in the document is maintained.
//
// For example - if there are 3 sub-searchers with the following parameters
//   - searcher1: first position = 4, last position = 4
//   - searcher2: first position = 6, last position = 8
//   - searcher3: first position = 13, last position = 16
//
// Then any document hit must have
//   - searcher1 hit (extending positions [X,X])
//   - searcher2 hit 2 positions after searcher1 hit (extending positions [X+2,X+4])
//   - searcher3 hit 5 positions after searcher2 hit (extending positions [X+9,X+12])
//
// thus for each sub searcher:
//   - the hit in the document must be at position equal to its first position - the previous searcher's last position
//   - the hit for the first sub searcher in the sequence can be anywhere in the document.
type SearcherPosition struct {
	FirstPos uint64
	LastPos  uint64
}

// NewSynonymSearcher is an abstraction that returns either a disjunction searcher or a conjunction searcher
// or a specialized conjunction searcher based on the operator.
//
// The operator can be 0, 1 or 2. 0 is for disjunction, 1 is for conjunction and 2 is for specialized conjunction.
//
// arrangedTokens is a 2D slice of tokens arranged in the order of the query.
// Each element in the slice
// 		- contains only one token if it is not of type synonym.
//			-> this token is used to build basic term or fuzzy searchers.
// 		- contains multiple tokens if it is of type synonym.
//			-> these tokens are used to build phrase searchers, one per token,
//					since synonyms are generally considered to be phrases.
//			-> the phrase searchers are then used to build a disjunction searcher.
// Hence, for each element in the slice, there is one searcher, which is either a term or fuzzy searcher or a disjunction searcher.
// These searchers are then used to build a disjunction searcher or a conjunction searcher or a specialized conjunction searcher based on the operator.

func NewSynonymSearcher(ctx context.Context, indexReader index.IndexReader,
	arrangedTokens [][]*analysis.Token, field string, boost float64, fuzziness int,
	prefix int, operator int, options search.SearcherOptions) (search.Searcher, error) {

	if operator == 2 {
		options.IncludeTermVectors = true
	}
	var searcher search.Searcher
	var outerSearcher = make([]search.Searcher, len(arrangedTokens))
	var synonymPhrases []search.Searcher
	var err error
	defer closeSearchers(&err, outerSearcher, synonymPhrases, []search.Searcher{searcher})
	if fuzziness > MaxFuzziness {
		return nil, fmt.Errorf("fuzziness exceeds max (%d)", MaxFuzziness)
	}
	if fuzziness < 0 {
		return nil, fmt.Errorf("invalid fuzziness, negative")
	}
	for tokenPosition := 0; tokenPosition < len(arrangedTokens); tokenPosition++ {
		if len(arrangedTokens[tokenPosition]) == 1 {
			term := string(arrangedTokens[tokenPosition][0].Term)
			if fuzziness == 0 {
				searcher, err = NewTermSearcher(ctx, indexReader, term,
					field, boost, options)
			} else {
				searcher, err = NewFuzzySearcher(ctx, indexReader, term, prefix,
					fuzziness, field, boost, options)
			}
		} else {
			synonymPhrases = make([]search.Searcher, len(arrangedTokens[tokenPosition]))
			for synonymIndex, synonym := range arrangedTokens[tokenPosition] {
				phrase := tokenizeSynonym(synonym.Term)
				searcher, err = NewPhraseSearcher(ctx, indexReader, phrase, field, options)
				if err != nil {
					return nil, err
				}
				synonymPhrases[synonymIndex] = searcher
			}
			searcher, err = NewDisjunctionSearcher(ctx, indexReader, synonymPhrases, 1, options)
		}
		if err != nil {
			return nil, err
		}
		outerSearcher[tokenPosition] = searcher
	}
	if operator == 0 {
		searcher, err = NewDisjunctionSearcher(ctx, indexReader, outerSearcher, 1, options)
	} else if operator == 1 {
		searcher, err = NewConjunctionSearcher(ctx, indexReader, outerSearcher, options)
	} else if operator == 2 {
		var searchersPositions = make([]*SearcherPosition, len(arrangedTokens))
		for tokIndex, tok := range arrangedTokens {
			searchersPositions[tokIndex] = &SearcherPosition{
				FirstPos: uint64(tok[0].FirstPosition),
				LastPos:  uint64(tok[0].LastPosition),
			}
		}
		ctx = context.WithValue(ctx, synonymSearchCtxKey("searcherPositions"), searchersPositions)
		searcher, err = NewConjunctionSearcher(ctx, indexReader, outerSearcher, options)
	}
	if err != nil {
		return nil, err
	}
	return searcher, nil
}
