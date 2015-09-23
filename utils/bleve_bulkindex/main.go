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
	"bufio"
	"flag"
	"log"
	"math/rand"
	"os"

	"github.com/blevesearch/bleve"
	_ "github.com/blevesearch/bleve/config"
)

var indexPath = flag.String("index", "", "index path")
var batchSize = flag.Int("size", 1000, "size of a single batch to index")

func main() {

	flag.Parse()

	if *indexPath == "" {
		log.Fatal("must specify index path")
	}

	// open the index
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

	if flag.NArg() < 1 {
		log.Fatal("must specify at least one path to index")
	}

	i := 0
	batch := index.NewBatch()

	for _, file := range flag.Args() {

		file, err := os.Open(file)
		defer func() {
			cerr := file.Close()
			if cerr != nil {
				log.Fatalf("error closing file: %v", cerr)
			}
		}()
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Indexing: %s\n", file.Name())
		r := bufio.NewReader(file)

		for {
			if i%*batchSize == 0 {
				log.Printf("Indexing batch (%d docs)...\n", i)
				err := index.Batch(batch)
				if err != nil {
					log.Fatal(err)
				}
				batch = index.NewBatch()
			}

			b, _ := r.ReadBytes('\n')
			if len(b) == 0 {
				break
			}
			docID := randomString(5)
			err := batch.Index(docID, b)
			if err != nil {
				log.Fatal(err)
			}
			i++
		}
		err = index.Batch(batch)
		if err != nil {
			log.Fatal(err)
		}
	}
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
