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
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/blevesearch/bleve"
	_ "github.com/blevesearch/bleve/config"
	_ "github.com/blevesearch/bleve/index/store/metrics"
	"github.com/blevesearch/bleve/index/upside_down"
)

var indexPath = flag.String("index", "", "index path")

var fieldsOnly = flag.Bool("fields", false, "print only field definitions")
var docID = flag.String("docID", "", "print only rows related to specified document")
var mappingOnly = flag.Bool("mapping", false, "print only index mappings")

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, strings.TrimSpace(`
bleve_dump prints the properties and binary representations of all rows in the
index specified by -index.
`)+"\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	if len(flag.Args()) > 0 {
		log.Fatalf("unexpected argument '%s', use -help to see possible options",
			flag.Args()[0])
	}
	if *indexPath == "" {
		log.Fatal("specify index to dump")
	}

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

	if *mappingOnly {
		if *docID != "" || *fieldsOnly {
			log.Fatal("-mapping cannot be used with -docID or -fields")
		}
		mapping := index.Mapping()
		jsonBytes, err := json.MarshalIndent(mapping, "", "  ")
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s\n", jsonBytes)
		return
	}

	var dumpChan chan interface{}
	if *docID != "" {
		if *fieldsOnly {
			log.Fatal("-docID cannot be used with -fields")
		}
		dumpChan = index.DumpDoc(*docID)
	} else if *fieldsOnly {
		dumpChan = index.DumpFields()
	} else {
		dumpChan = index.DumpAll()
	}

	for rowOrErr := range dumpChan {
		switch rowOrErr := rowOrErr.(type) {
		case error:
			log.Printf("error dumping: %v", rowOrErr)
		case upside_down.UpsideDownCouchRow:
			fmt.Printf("%v\n", rowOrErr)
			fmt.Printf("Key:   % -100x\nValue: % -100x\n\n", rowOrErr.Key(), rowOrErr.Value())
		}
	}
}
