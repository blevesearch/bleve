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

	"github.com/couchbaselabs/bleve"
	"github.com/couchbaselabs/bleve/index/upside_down"
)

var indexDir = flag.String("indexDir", "index", "index directory")

var fieldsOnly = flag.Bool("fields", false, "fields only")
var docId = flag.String("docId", "", "docId to dump")

func main() {
	flag.Parse()

	bleve.Config.CreateIfMissing = false
	index, err := bleve.Open(*indexDir, bleve.NewIndexMapping())
	if err != nil {
		log.Fatal(err)
	}
	defer index.Close()

	var dumpChan chan interface{}
	if *docId != "" {
		dumpChan = index.DumpDoc(*docId)
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
