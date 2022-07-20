# ![bleve](docs/bleve.png) bleve

[![Tests](https://github.com/blevesearch/bleve/workflows/Tests/badge.svg?branch=master&event=push)](https://github.com/blevesearch/bleve/actions?query=workflow%3ATests+event%3Apush+branch%3Amaster)
[![Coverage Status](https://coveralls.io/repos/github/blevesearch/bleve/badge.svg?branch=master)](https://coveralls.io/github/blevesearch/bleve?branch=master)
[![GoDoc](https://godoc.org/github.com/blevesearch/bleve?status.svg)](https://godoc.org/github.com/blevesearch/bleve)
[![Join the chat at https://gitter.im/blevesearch/bleve](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/blevesearch/bleve?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![codebeat](https://codebeat.co/badges/38a7cbc9-9cf5-41c0-a315-0746178230f4)](https://codebeat.co/projects/github-com-blevesearch-bleve)
[![Go Report Card](https://goreportcard.com/badge/blevesearch/bleve)](https://goreportcard.com/report/blevesearch/bleve)
[![Sourcegraph](https://sourcegraph.com/github.com/blevesearch/bleve/-/badge.svg)](https://sourcegraph.com/github.com/blevesearch/bleve?badge)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

modern text indexing in go - [blevesearch.com](http://www.blevesearch.com/)

## Features

* Index any go data structure (including JSON)
* Intelligent defaults backed up by powerful configuration
* Supported field types:
    * Text, Numeric, Date
* Supported query types:
    * Term, Phrase, Match, Match Phrase, Prefix
    * Conjunction, Disjunction, Boolean
    * Numeric Range, Date Range
    * [Geo Spatial](https://github.com/blevesearch/bleve/blob/master/geo/README.md)
    * Simple query [syntax](http://www.blevesearch.com/docs/Query-String-Query/) for human entry
* tf-idf Scoring
* Search result match highlighting
* Supports Aggregating Facets:
    * Terms Facet
    * Numeric Range Facet
    * Date Range Facet

## Discussion

Discuss usage and development of bleve in the [google group](https://groups.google.com/forum/#!forum/bleve).

## Indexing

```go
message := struct{
	Id   string
	From string
	Body string
}{
	Id:   "example",
	From: "marty.schoch@gmail.com",
	Body: "bleve indexing is easy",
}

mapping := bleve.NewIndexMapping()
index, err := bleve.New("example.bleve", mapping)
if err != nil {
	panic(err)
}
index.Index(message.Id, message)
```

## Querying

```go
index, _ := bleve.Open("example.bleve")
query := bleve.NewQueryStringQuery("bleve")
searchRequest := bleve.NewSearchRequest(query)
searchResult, _ := index.Search(searchRequest)
```

## Command Line Interface

To install the CLI for the latest release of bleve, run:

```bash
$ go install github.com/blevesearch/bleve/v2/cmd/bleve@latest
```

```
$ bleve --help
Bleve is a command-line tool to interact with a bleve index.

Usage:
  bleve [command]

Available Commands:
  bulk        bulk loads from newline delimited JSON files
  check       checks the contents of the index
  count       counts the number documents in the index
  create      creates a new index
  dictionary  prints the term dictionary for the specified field in the index
  dump        dumps the contents of the index
  fields      lists the fields in this index
  help        Help about any command
  index       adds the files to the index
  mapping     prints the mapping used for this index
  query       queries the index
  registry    registry lists the bleve components compiled into this executable
  scorch      command-line tool to interact with a scorch index

Flags:
  -h, --help   help for bleve

Use "bleve [command] --help" for more information about a command.
```

## Text Analysis Wizard

https://bleveanalysis.couchbase.com

## License

Apache License Version 2.0
