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
	"log"

	"github.com/couchbaselabs/bleve/index/store/leveldb"
	"github.com/couchbaselabs/bleve/index/upside_down"
)

var indexDir = flag.String("indexDir", "index", "index directory")

var fieldsOnly = flag.Bool("fields", false, "fields only")

func main() {
	flag.Parse()

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

	if !*fieldsOnly {
		index.Dump()
	} else {
		index.DumpFields()
	}
}
