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
func splitOnSpace(phrase []byte) []string {
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

// NewSynonymSearcher is an abstraction that returns either a disjunction searcher or a conjunction searcher
// or a ordered conjunction searcher based on the operator.
//
// The operator can be 0, 1 or 2. 0 is for disjunction, 1 is for conjunction and 2 is for ordered conjunction.
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
// These searchers are then used to build a disjunction searcher or a conjunction searcher or a ordered conjunction searcher based on the operator.

func NewSynonymSearcher(ctx context.Context, indexReader index.IndexReader,
	arrangedTokens [][]*analysis.Token, field string, boost float64, fuzziness int,
	prefix int, operator int, options search.SearcherOptions) (search.Searcher, error) {

	options.IncludeTermVectors = true
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
				phrase := splitOnSpace(synonym.Term)
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
		var searchersWithPositions = make([]*SearchAtPosition, len(arrangedTokens))
		for searcherIndex, searcher := range outerSearcher {
			searchersWithPositions[searcherIndex] = &SearchAtPosition{
				Searcher: searcher,
				FirstPos: uint64(arrangedTokens[searcherIndex][0].FirstPosition),
				LastPos:  uint64(arrangedTokens[searcherIndex][0].LastPosition),
			}
		}
		searcher, err = NewOrderedConjunctionSearcher(ctx, indexReader, searchersWithPositions, options)
	}
	if err != nil {
		return nil, err
	}
	return searcher, nil
}
