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

	"github.com/blevesearch/bleve"
)

var jsonDir = flag.String("jsonDir", "json", "json directory")
var indexPath = flag.String("index", "index.bleve", "index path")

func main() {

	flag.Parse()

	// create a new default mapping
	mapping := bleve.NewIndexMapping()

	// open the index
	index, err := bleve.New(*indexPath, mapping)
	if err != nil {
		log.Fatal(err)
	}
	defer index.Close()

	for jsonFile := range walkDirectory(*jsonDir) {
		// index the json files
		err = index.Index(jsonFile.filename, jsonFile.contents)
		if err != nil {
			log.Fatal(err)
		}
	}
}

type jsonFile struct {
	filename string
	contents []byte
}

func walkDirectory(dir string) chan jsonFile {
	rv := make(chan jsonFile)
	go func() {
		defer close(rv)

		// open the directory
		dirEntries, err := ioutil.ReadDir(dir)
		if err != nil {
			log.Fatal(err)
		}

		// walk the directory entries
		for _, dirEntry := range dirEntries {
			// read the bytes
			jsonBytes, err := ioutil.ReadFile(dir + "/" + dirEntry.Name())
			if err != nil {
				log.Fatal(err)
			}

			rv <- jsonFile{
				filename: dirEntry.Name(),
				contents: jsonBytes,
			}
		}
	}()
	return rv
}
