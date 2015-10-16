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
	"os"
	"path/filepath"

	"github.com/blevesearch/bleve"
	_ "github.com/blevesearch/bleve/config"
)

var (
	indexPath = flag.String("index", "", "index path")
	keepExt   = flag.Bool("keepExt", false, "keep extension in doc id")
	keepDir   = flag.Bool("keepDir", false, "keep dir in doc id")
)

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

	for file := range handleArgs(flag.Args()) {
		// index the files
		docID := file.filename
		if !*keepDir {
			_, docID = filepath.Split(docID)
		}
		if !*keepExt {
			ext := filepath.Ext(docID)
			docID = docID[0 : len(docID)-len(ext)]
		}
		log.Printf("Indexing: %s", docID)
		err = index.Index(docID, file.contents)
		if err != nil {
			log.Fatal(err)
		}
	}
}

type file struct {
	filename string
	contents []byte
}

func handleArgs(args []string) chan file {
	rv := make(chan file)
	go getAllFiles(args, rv)
	return rv
}

func getAllFiles(args []string, rv chan file) {
	for _, arg := range args {
		arg = filepath.Clean(arg)
		err := filepath.Walk(arg, func(path string, finfo os.FileInfo, err error) error {
			if err != nil {
				log.Print(err)
				return err
			}
			if finfo.IsDir() {
				return nil
			}

			bytes, err := ioutil.ReadFile(path)
			if err != nil {
				log.Fatal(err)
			}
			rv <- file{
				filename: filepath.Base(path),
				contents: bytes,
			}
			return nil
		})
		if err != nil {
			log.Fatal(err)
		}
	}
	close(rv)
}
