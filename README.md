# ![bleve](docs/bleve.png) bleve

modern text indexing in go

## Features
* Index any go data structure (including JSON)
* Intelligent defaults backed up by powerful configuration
* Supported field types:
    * Text
* Supported query types:
    * Term, Phrase, Match, Match Phrase
    * Conjunction, Disjunction, Boolean
    * Simple query syntax for human entry
* Search result match highlighting

## Indexing

		message := struct{
			From: "marty.schoch@gmail.com",
			Body: "bleve indexing is easy",
		}

		mapping := bleve.NewIndexMapping()
		index, _ := bleve.Open("example.bleve", mapping)
		index.IndexId(message)

## Querying

		mapping := bleve.NewIndexMapping()
		index, _ := bleve.Open("example.bleve", mapping)
		query := bleve.NewSyntaxQuery("bleve")
		searchRequest := bleve.NewSearchRequest(query)
		searchResult, _ := index.Search(searchRequest)


## Status

[![Build Status](https://drone.io/github.com/couchbaselabs/bleve/status.png)](https://drone.io/github.com/couchbaselabs/bleve/latest)
[![Coverage Status](https://coveralls.io/repos/couchbaselabs/bleve/badge.png?branch=master)](https://coveralls.io/r/couchbaselabs/bleve?branch=master)