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
	"io/ioutil"
	"log"

	"github.com/blevesearch/bleve"
	_ "github.com/blevesearch/bleve/config"
)

var indexPath = flag.String("index", "", "index path")
var mappingFile = flag.String("mapping", "", "mapping file")
var storeType = flag.String("store", bleve.Config.DefaultKVStore, "store type")
var indexType = flag.String("indexType", bleve.Config.DefaultIndexType, "index type")

func main() {

	flag.Parse()

	if *indexPath == "" {
		log.Fatal("must specify index path")
	}

	// create a new default mapping
	mapping := bleve.NewIndexMapping()
	if *mappingFile != "" {
		mappingBytes, err := ioutil.ReadFile(*mappingFile)
		if err != nil {
			log.Fatal(err)
		}
		err = json.Unmarshal(mappingBytes, &mapping)
		if err != nil {
			log.Fatal(err)
		}
	}

	// create the index
	index, err := bleve.NewUsing(*indexPath, mapping, *indexType, *storeType, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		cerr := index.Close()
		if cerr != nil {
			log.Fatalf("error closing index: %v", err)
		}
	}()

	log.Printf("Created bleve index at: %s", *indexPath)
}
