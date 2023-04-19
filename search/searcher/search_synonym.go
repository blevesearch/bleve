package searcher

import (
	"context"
	"fmt"

	"github.com/blevesearch/bleve/v2/analysis"
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

func NewSynonymSearcher(ctx context.Context, indexReader index.IndexReader,
	graphNodes [][]*analysis.Token, field string, boost float64, fuzziness int,
	prefix int, operator int, options search.SearcherOptions) (search.Searcher, error) {

	options.IncludeTermVectors = true
	var outerSearcher []search.Searcher
	var innerSearcher []search.Searcher
	var matchPhrase []string
	var curNode = 1
	var newCurnode int
	var searcher search.Searcher
	var err error
	var term string
	if fuzziness > MaxFuzziness {
		return nil, fmt.Errorf("fuzziness exceeds max (%d)", MaxFuzziness)
	}
	if fuzziness < 0 {
		return nil, fmt.Errorf("invalid fuzziness, negative")
	}
	for len(graphNodes[curNode]) != 0 {
		if len(graphNodes[curNode]) == 1 {
			term = string(graphNodes[curNode][0].Term)
			if fuzziness == 0 {
				searcher, err = NewTermSearcher(ctx, indexReader, term,
					field, boost, options)
			} else {
				searcher, err = NewFuzzySearcher(ctx, indexReader, term, prefix,
					fuzziness, field, boost, options)
			}
			if err != nil {
				err2 := closeSearchers(outerSearcher, innerSearcher)
				return nil, fmt.Errorf("error building term searcher: %v, close error: %v", err, err2)
			}
			outerSearcher = append(outerSearcher, searcher)
			curNode++
		} else {
			innerSearcher = nil
			for _, neighborNode := range graphNodes[curNode] {
				if !neighborNode.FinalNode {
					matchPhrase = nil
					matchPhrase = append(matchPhrase,
						string(neighborNode.Term))
					innerCur := neighborNode.NextNode
					for !graphNodes[innerCur][0].FinalNode {
						matchPhrase = append(matchPhrase,
							string(graphNodes[innerCur][0].Term))
						innerCur = graphNodes[innerCur][0].NextNode
					}
					matchPhrase = append(matchPhrase,
						string(graphNodes[innerCur][0].Term))
					newCurnode = graphNodes[innerCur][0].NextNode
					searcher, err = NewPhraseSearcher(ctx, indexReader,
						matchPhrase, field, options)
					if err != nil {
						err2 := closeSearchers(outerSearcher, innerSearcher)
						return nil, fmt.Errorf("error building term searcher: %v, close error: %v", err, err2)
					}
					innerSearcher = append(innerSearcher, searcher)
				} else {
					term = string(neighborNode.Term)
					searcher, err = NewTermSearcher(ctx, indexReader, term,
						field, boost, options)
					if err != nil {
						err2 := closeSearchers(outerSearcher, innerSearcher)
						return nil, fmt.Errorf("error building term searcher: %v, close error: %v", err, err2)
					}
					innerSearcher = append(innerSearcher, searcher)
					newCurnode = neighborNode.NextNode
				}
			}
			searcher, err = NewDisjunctionSearcher(ctx, indexReader, innerSearcher, 1, options)
			if err != nil {
				err2 := closeSearchers(outerSearcher, innerSearcher)
				return nil, fmt.Errorf("error building term searcher: %v, close error: %v", err, err2)
			}
			outerSearcher = append(outerSearcher, searcher)
			curNode = newCurnode
		}
	}
	if operator == 0 {
		searcher, err = NewDisjunctionSearcher(ctx, indexReader, outerSearcher, 1, options)
	} else if operator == 1 {
		searcher, err = NewConjunctionSearcher(ctx, indexReader, outerSearcher, options)
	}
	if err != nil {
		err2 := closeSearchers(outerSearcher, innerSearcher)
		return nil, fmt.Errorf("error building term searcher: %v, close error: %v", err, err2)
	}
	return searcher, nil
}
