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

	"github.com/couchbaselabs/bleve/index/upside_down"
	"github.com/couchbaselabs/bleve/search"
)

var field = flag.String("field", "description", "field to query")
var indexDir = flag.String("indexDir", "index", "index directory")
var limit = flag.Int("limit", 10, "limit to first N results")

func main() {

	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatal("Specify search term")
	}

	// open index
	index := upside_down.NewUpsideDownCouch(*indexDir)
	err := index.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer index.Close()

	tq := search.TermQuery{
		Term:     flag.Arg(0),
		Field:    *field,
		BoostVal: 1.0,
		Explain:  true,
	}
	collector := search.NewTopScorerCollector(*limit)
	searcher, err := tq.Searcher(index)
	if err != nil {
		log.Fatalf("searcher error: %v", err)
		return
	}
	err = collector.Collect(searcher)
	if err != nil {
		log.Fatalf("search error: %v", err)
		return
	}
	results := collector.Results()
	if len(results) == 0 {
		fmt.Printf("No matches\n")
	} else {
		last := uint64(*limit)
		if searcher.Count() < last {
			last = searcher.Count()
		}
		fmt.Printf("%d matches, showing %d through %d\n", searcher.Count(), 1, last)
		for i, result := range results {
			fmt.Printf("%2d. %s (%f)\n", i+1, result.ID, result.Score)
		}
	}
}
