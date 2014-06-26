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
	"io/ioutil"
	"log"

	"github.com/couchbaselabs/bleve/document"
	"github.com/couchbaselabs/bleve/index/store/leveldb"
	"github.com/couchbaselabs/bleve/index/upside_down"
	"github.com/couchbaselabs/bleve/shredder"
)

var jsonDir = flag.String("jsonDir", "json", "json directory")
var indexDir = flag.String("indexDir", "index", "index directory")
var storeFields = flag.Bool("storeFields", false, "store field data")
var includeTermVectors = flag.Bool("includeTermVectors", false, "include term vectors")

func main() {

	flag.Parse()

	indexOptions := document.INDEX_FIELD
	if *storeFields {
		indexOptions |= document.STORE_FIELD
	}
	if *includeTermVectors {
		indexOptions |= document.INCLUDE_TERM_VECTORS
	}

	// create a automatic JSON document shredder
	jsonShredder := shredder.NewAutoJsonShredderWithOptions(indexOptions)

	// create a new index
	store, err := leveldb.Open(*indexDir)
	if err != nil {
		log.Fatal(err)
	}
	index := upside_down.NewUpsideDownCouch(store)
	err = index.Open()
	if err != nil {
		log.Fatal(err)
	}
	defer index.Close()

	// open the directory
	dirEntries, err := ioutil.ReadDir(*jsonDir)
	if err != nil {
		log.Fatal(err)
	}

	// walk the directory entries
	for _, dirEntry := range dirEntries {
		// read the bytes
		jsonBytes, err := ioutil.ReadFile(*jsonDir + "/" + dirEntry.Name())
		if err != nil {
			log.Fatal(err)
		}
		// shred them into a document
		doc, err := jsonShredder.Shred(dirEntry.Name(), jsonBytes)
		if err != nil {
			log.Fatal(err)
		}
		//log.Printf("%+v", doc)
		// update the index
		err = index.Update(doc)
		if err != nil {
			log.Fatal(err)
		}
	}
}
