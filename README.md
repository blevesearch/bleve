# ![bleve](docs/bleve.png) bleve

modern text indexing in go - [blevesearch.com](http://www.blevesearch.com/)

Try out bleve live by [searching our wiki](http://wikisearch.blevesearch.com/search/).

## Features
* Index any go data structure (including JSON)
* Intelligent defaults backed up by powerful configuration
* Supported field types:
    * Text, Numeric, Date
* Supported query types:
    * Term, Phrase, Match, Match Phrase, Prefix
    * Conjunction, Disjunction, Boolean
    * Numeric Range, Date Range
    * Simple query [syntax](https://github.com/blevesearch/bleve/wiki/Query-String-Query) for human entry
* tf-idf Scoring
* Search result match highlighting
* Supports Aggregating Facets:
    * Terms Facet
    * Numeric Range Facet
    * Date Range Facet

## Discussion

Discuss usage and development of bleve in the [google group](https://groups.google.com/forum/#!forum/bleve).

## Indexing

		message := struct{
			From: "marty.schoch@gmail.com",
			Body: "bleve indexing is easy",
		}

		mapping := bleve.NewIndexMapping()
		index, _ := bleve.New("example.bleve", mapping)
		index.Index(message)

## Querying

		index, _ := bleve.Open("example.bleve")
		query := bleve.NewSyntaxQuery("bleve")
		searchRequest := bleve.NewSearchRequest(query)
		searchResult, _ := index.Search(searchRequest)
		
## License

Apache License Version 2.0

## Status

[![Build Status](https://drone.io/github.com/blevesearch/bleve/status.png)](https://drone.io/github.com/blevesearch/bleve/latest)
[![Coverage Status](https://coveralls.io/repos/blevesearch/bleve/badge.png?branch=master)](https://coveralls.io/r/blevesearch/bleve?branch=master)
