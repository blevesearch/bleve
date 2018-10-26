package examples

import (
	"fmt"
	"github.com/blevesearch/bleve"
)

func PrintResults(searchResult *bleve.SearchResult) {
	fmt.Println("Matches: ", searchResult.Hits.Len(), " took ", searchResult.Took)
	if searchResult.Hits.Len() > 0 {
		fmt.Println("Results: ")
		for i, hit := range searchResult.Hits {
			fmt.Printf("  %d: %s, (score: %f)\n", i, hit.ID, hit.Score)
		}
	}

}
