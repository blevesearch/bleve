//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"strings"

	"github.com/blevesearch/bleve"
	_ "github.com/blevesearch/bleve/config"
	_ "github.com/blevesearch/bleve/index/store/metrics"
)

var indexPath = flag.String("index", "", "index path")
var limit = flag.Int("limit", 10, "limit to first N results")
var skip = flag.Int("skip", 0, "skip the first N results")
var explain = flag.Bool("explain", false, "explain scores")
var includeHighlights = flag.Bool("highlight", true, "highlight matches")
var includeStoredFields = flag.Bool("fields", false, "return stored fields")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var repeat = flag.Int("repeat", 1, "repeat query n times")
var qtype = flag.String("queryType", "query_string", "type of query to execute: query_string, prefix")
var qfield = flag.String("field", "", "the field to query, not applicable to query_string queries")

func main() {

	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal(err)
		}
		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer pprof.StopCPUProfile()
	}

	if *indexPath == "" {
		log.Fatal("Specify index to query")
	}

	if flag.NArg() < 1 {
		log.Fatal("Specify search query")
	}

	// open index
	index, err := bleve.Open(*indexPath)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		cerr := index.Close()
		if cerr != nil {
			log.Fatalf("error closing index: %v", err)
		}
	}()

	for i := 0; i < *repeat; i++ {
		var query bleve.Query

		switch *qtype {
		case "prefix":
			pquery := bleve.NewPrefixQuery(strings.Join(flag.Args(), " "))
			if *qfield != "" {
				pquery.SetField(*qfield)
			}
			query = pquery
		case "term":
			pquery := bleve.NewTermQuery(strings.Join(flag.Args(), " "))
			if *qfield != "" {
				pquery.SetField(*qfield)
			}
			query = pquery
		default:
			// build a search with the provided parameters
			queryString := strings.Join(flag.Args(), " ")
			query = bleve.NewQueryStringQuery(queryString)
		}

		searchRequest := bleve.NewSearchRequestOptions(query, *limit, *skip, *explain)

		// enable highlights if requested
		if *includeHighlights {
			searchRequest.Highlight = bleve.NewHighlightWithStyle("ansi")
		}

		if *includeStoredFields {
			searchRequest.Fields = []string{"*"}
		}

		// execute the search
		searchResult, err := index.Search(searchRequest)
		if err != nil {
			log.Fatalf("search error: %v", err)
		}
		fmt.Println(searchResult)
	}
}
