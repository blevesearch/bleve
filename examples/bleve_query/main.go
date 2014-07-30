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
	"strings"

	"github.com/couchbaselabs/bleve"
)

var field = flag.String("field", "_all", "field to query")
var indexDir = flag.String("indexDir", "index", "index directory")
var limit = flag.Int("limit", 10, "limit to first N results")
var skip = flag.Int("skip", 0, "skip the first N results")
var explain = flag.Bool("explain", false, "explain scores")
var includeHighlights = flag.Bool("highlight", true, "highlight matches")

func main() {

	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatal("Specify search query")
	}

	// don't create an index if it doesn't exist
	bleve.Config.CreateIfMissing = false

	// create a new default mapping
	mapping := bleve.NewIndexMapping()

	// open index
	index, err := bleve.Open(*indexDir, mapping)
	if err != nil {
		log.Fatal(err)
	}
	defer index.Close()

	// build a search with the provided parameters
	queryString := strings.Join(flag.Args(), " ")
	query := bleve.NewSyntaxQuery(queryString)
	searchRequest := bleve.NewSearchRequest(query, *limit, *skip, *explain)

	// enable highlights if requested
	if *includeHighlights {
		searchRequest.Highlight = bleve.NewHighlightWithStyle("ansi")
	}

	// execute the search
	searchResult, err := index.Search(searchRequest)
	if err != nil {
		log.Fatalf("search error: %v", err)
	}
	fmt.Println(searchResult)
}
