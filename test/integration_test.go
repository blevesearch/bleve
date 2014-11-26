//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package test

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"

	"github.com/blevesearch/bleve"
)

var dataset = flag.String("dataset", "", "only test datasets matching this regex")
var keepIndex = flag.Bool("keepIndex", false, "keep the index after testing")

func TestIntegration(t *testing.T) {

	flag.Parse()

	var err error
	var datasetRegexp *regexp.Regexp
	if *dataset != "" {
		datasetRegexp, err = regexp.Compile(*dataset)
		if err != nil {
			t.Fatal(err)
		}
	}

	fis, err := ioutil.ReadDir("tests")
	if err != nil {
		t.Fatal(err)
	}
	for _, fi := range fis {
		if datasetRegexp != nil {
			if !datasetRegexp.MatchString(fi.Name()) {
				continue
			}
		}
		if fi.IsDir() {
			t.Logf("Running test: %s", fi.Name())
			runTestDir(t, "tests"+string(filepath.Separator)+fi.Name())
		}
	}
}

func runTestDir(t *testing.T, dir string) {
	// read the mapping
	mappingBytes, err := ioutil.ReadFile(dir + string(filepath.Separator) + "mapping.json")
	if err != nil {
		t.Errorf("error reading mapping: %v", err)
		return
	}
	var mapping bleve.IndexMapping
	err = json.Unmarshal(mappingBytes, &mapping)
	if err != nil {
		t.Errorf("error unmarshalling mapping: %v", err)
		return
	}

	// open new index
	if !*keepIndex {
		defer os.RemoveAll("test.bleve")
	}
	index, err := bleve.New("test.bleve", &mapping)
	if err != nil {
		t.Errorf("error creating new index: %v", err)
		return
	}
	defer index.Close()

	//index data
	fis, err := ioutil.ReadDir(dir + string(filepath.Separator) + "data")
	if err != nil {
		t.Errorf("error reading data dir: %v", err)
		return
	}
	for _, fi := range fis {
		fileBytes, err := ioutil.ReadFile(dir + string(filepath.Separator) + "data" + string(filepath.Separator) + fi.Name())
		if err != nil {
			t.Errorf("error reading data file: %v", err)
			return
		}
		filename := fi.Name()
		ext := filepath.Ext(filename)
		id := filename[0 : len(filename)-len(ext)]
		err = index.Index(id, fileBytes)
		if err != nil {
			t.Errorf("error indexing data: %v", err)
			return
		}
	}

	// read the searches
	searchBytes, err := ioutil.ReadFile(dir + string(filepath.Separator) + "searches.json")
	if err != nil {
		t.Errorf("error reading searches: %v", err)
		return
	}
	var searches SearchTests
	err = json.Unmarshal(searchBytes, &searches)
	if err != nil {
		t.Errorf("error unmarshalling searches: %v", err)
		return
	}

	// run the searches
	for _, search := range searches {
		res, err := index.Search(search.Search)
		if err != nil {
			t.Errorf("error running search: %v", err)
		}
		if res.Total != search.Result.Total {
			t.Errorf("expected total: %d got %d", search.Result.Total, res.Total)
			continue
		}
		if len(res.Hits) != len(search.Result.Hits) {
			t.Errorf("expected hits len: %d got %d", len(search.Result.Hits), len(res.Hits))
			continue
		}
		for hi, hit := range search.Result.Hits {
			if hit.ID != res.Hits[hi].ID {
				t.Errorf("expected hit %d to have ID %s got %s", hi, hit.ID, res.Hits[hi].ID)
			}
			if hit.Fields != nil {
				if !reflect.DeepEqual(hit.Fields, res.Hits[hi].Fields) {
					t.Errorf("expected hit %d to have fields %#v got %#v", hi, hit.Fields, res.Hits[hi].Fields)
				}
			}
			if hit.Fragments != nil {
				if !reflect.DeepEqual(hit.Fragments, res.Hits[hi].Fragments) {
					t.Errorf("expected hit %d to have fragments %#v got %#v", hi, hit.Fragments, res.Hits[hi].Fragments)
				}
			}
		}
		if search.Result.Facets != nil {
			if !reflect.DeepEqual(search.Result.Facets, res.Facets) {
				t.Errorf("expected facets: %#v got %#v", search.Result.Facets, res.Facets)
			}
		}
	}
}
