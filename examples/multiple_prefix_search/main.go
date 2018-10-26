package main

import (
	"github.com/blevesearch/bleve"
	"github.com/blevesearch/bleve/examples"
	blave_query "github.com/blevesearch/bleve/search/query"
)

func main() {
	message := struct {
		Id   string
		From string
		Body string
	}{
		Id:   "example",
		From: "marty.schoch@gmail.com",
		Body: "bleve indexing is easy",
	}

	mapping := bleve.NewIndexMapping()

	// mem only index for example is used
	index, err := bleve.NewMemOnly(mapping)
	if err != nil {
		panic(err)
	}
	index.Index(message.Id, message)

	var queries []blave_query.Query

	queries = append(queries, bleve.NewPrefixQuery("eas"))
	queries = append(queries, bleve.NewPrefixQuery("ind"))
	query := bleve.NewConjunctionQuery(queries...)

	searchRequest := bleve.NewSearchRequest(query)
	searchResult, _ := index.Search(searchRequest)

	examples.PrintResults(searchResult)
}
