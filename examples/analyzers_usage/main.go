package main

import (
	"fmt"
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/analysis/analyzer/simple"
	"github.com/blevesearch/bleve/analysis/analyzer/standard"
)

func main() {
	mapping := bleve.NewIndexMapping()

	documentMapping := bleve.NewDocumentMapping()
	textFieldMapping := bleve.NewTextFieldMapping()
	textFieldMapping.Analyzer = simple.Name
	textFieldMapping.Store = false

	rankFieldMapping := bleve.NewTextFieldMapping()
	rankFieldMapping.Analyzer = standard.Name
	rankFieldMapping.Store = false

	documentMapping.AddFieldMappingsAt("Text", textFieldMapping)
	documentMapping.AddFieldMappingsAt("Rank", rankFieldMapping)
	mapping.AddDocumentMapping("object", documentMapping)

	// mem only index for example is used
	index, err := bleve.NewMemOnly(mapping)
	if err != nil {
		panic(err)
	}

	index.Index("average", map[string]string{
		"Text": "Bleve is average",
		"Rank": "100",
	})

	index.Index("best", map[string]string{
		"Text": "Bleve is the best",
		"Rank": "900",
	})

	index.Index("good", map[string]string{
		"Text": "Bleve is Good",
		"Rank": "300",
	})

	query := bleve.NewQueryStringQuery("bleve")
	searchRequest := bleve.NewSearchRequestOptions(query, 10, 0, false)
	searchRequest.SortBy([]string{"-Rank"})
	searchResult, _ := index.Search(searchRequest)

	fmt.Println("Matches: ", searchResult.Hits.Len(), " took ", searchResult.Took)
	for i, hit := range searchResult.Hits {
		fmt.Printf("%d: %s, (score: %f)\n", i, hit.ID, hit.Score)

	}

}
