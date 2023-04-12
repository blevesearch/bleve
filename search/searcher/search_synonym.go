package searcher

import (
	"context"
	"fmt"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

func NewSynonymSearcher(ctx context.Context, indexReader index.IndexReader, graphNodes [][]*analysis.Token,
	field string, boost float64, fuzziness int, prefix int, operator int,
	options search.SearcherOptions) (search.Searcher, error) {
	options.IncludeTermVectors = true
	var outerSearcher []search.Searcher
	var innerSearcher []search.Searcher
	var matchPhrase []string
	var curNode = 1
	var newCurnode int
	var searcher search.Searcher
	var err error
	var term string
	for len(graphNodes[curNode]) != 0 {
		if len(graphNodes[curNode]) == 1 {
			term = string(graphNodes[curNode][0].Term)
			searcher, err = NewTermSearcher(ctx, indexReader, term, field, boost, options)
			if err != nil {
				for _, searcher = range outerSearcher {
					_ = searcher.Close()
				}
				return nil, fmt.Errorf("phrase searcher error building term searcher: %v", err)
			}
			outerSearcher = append(outerSearcher, searcher)
			curNode++
		} else {
			innerSearcher = nil
			newCurnode = curNode + 1
			for _, neighborNode := range graphNodes[curNode] {
				if neighborNode.NextNode != neighborNode.FinalNode {
					matchPhrase = nil
					matchPhrase = append(matchPhrase, string(neighborNode.Term))
					innerCur := neighborNode.NextNode
					for innerCur != neighborNode.FinalNode {
						matchPhrase = append(matchPhrase, string(graphNodes[innerCur][0].Term))
						innerCur = graphNodes[innerCur][0].NextNode
					}
					newCurnode = neighborNode.FinalNode
					searcher, err = NewPhraseSearcher(ctx, indexReader, matchPhrase, field, options)
					if err != nil {
						for _, searcher = range outerSearcher {
							_ = searcher.Close()
						}
						for _, searcher = range innerSearcher {
							_ = searcher.Close()
						}
						return nil, fmt.Errorf("phrase searcher error building term searcher: %v", err)
					}
					innerSearcher = append(innerSearcher, searcher)
				} else {
					term = string(neighborNode.Term)
					searcher, err = NewTermSearcher(ctx, indexReader, term, field, boost, options)
					if err != nil {
						for _, searcher = range outerSearcher {
							_ = searcher.Close()
						}
						for _, searcher = range innerSearcher {
							_ = searcher.Close()
						}
						return nil, fmt.Errorf("phrase searcher error building term searcher: %v", err)
					}
					innerSearcher = append(innerSearcher, searcher)
					newCurnode = neighborNode.FinalNode
				}
			}
			searcher, err = NewDisjunctionSearcher(ctx, indexReader, innerSearcher, 1, options)
			if err != nil {
				for _, searcher = range outerSearcher {
					_ = searcher.Close()
				}
				for _, searcher = range innerSearcher {
					_ = searcher.Close()
				}
				return nil, fmt.Errorf("phrase searcher error building term searcher: %v", err)
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
		for _, ts := range outerSearcher {
			_ = ts.Close()
		}
		for _, ts := range innerSearcher {
			_ = ts.Close()
		}
		return nil, fmt.Errorf("phrase searcher error building term searcher: %v", err)
	}
	return searcher, nil
}
