package searcher

import (
	"context"
	"fmt"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/analysis/token/synonym"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

func closeSearchers(searchers ...[]search.Searcher) error {
	var err error
	for _, sa := range searchers {
		for _, s := range sa {
			err = s.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

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

func NewSynonymSearcher(ctx context.Context, indexReader index.IndexReader,
	arrangedTokens [][]*analysis.Token, field string, boost float64, fuzziness int,
	prefix int, operator int, options search.SearcherOptions) (search.Searcher, error) {

	options.IncludeTermVectors = true
	var searcher search.Searcher
	var outerSearcher = make([]search.Searcher, len(arrangedTokens))
	var synonymPhrases []search.Searcher
	var err error
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
					err2 := closeSearchers(outerSearcher, synonymPhrases, []search.Searcher{searcher})
					return nil, fmt.Errorf("error building term searcher: %v, close error: %v", err, err2)
				}
				synonymPhrases[synonymIndex] = searcher
			}
			searcher, err = NewDisjunctionSearcher(ctx, indexReader, synonymPhrases, 1, options)
			synonymPhrases = nil
		}
		if err != nil {
			err2 := closeSearchers(outerSearcher, synonymPhrases, []search.Searcher{searcher})
			return nil, fmt.Errorf("error building term searcher: %v, close error: %v", err, err2)
		}
		outerSearcher[tokenPosition] = searcher
	}
	if operator == 0 {
		searcher, err = NewDisjunctionSearcher(ctx, indexReader, outerSearcher, 1, options)
	} else if operator == 1 {
		searcher, err = NewConjunctionSearcher(ctx, indexReader, outerSearcher, options)
	} else if operator == 2 {
		var searchersWithPositions = make([]SearchAtPosition, len(arrangedTokens))
		for searcherIndex, searcher := range outerSearcher {
			searchersWithPositions[searcherIndex] = SearchAtPosition{
				Searcher: searcher,
				FirstPos: arrangedTokens[searcherIndex][0].FirstPosition,
				LastPos:  arrangedTokens[searcherIndex][0].LastPosition,
			}
		}
		searcher, err = NewSynonymPhraseSearcher(ctx, indexReader, searchersWithPositions, options)
	}
	if err != nil {
		err2 := closeSearchers(outerSearcher, synonymPhrases, []search.Searcher{searcher})
		return nil, fmt.Errorf("error building term searcher: %v, close error: %v", err, err2)
	}
	return searcher, nil
}
