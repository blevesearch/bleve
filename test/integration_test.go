//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/mapping"

	// allow choosing alternate kvstores
	_ "github.com/blevesearch/bleve/v2/config"
)

var dataset = flag.String("dataset", "", "only test datasets matching this regex")
var onlynum = flag.Int("testnum", -1, "only run the test with this number")
var keepIndex = flag.Bool("keepIndex", false, "keep the index after testing")

var indexType = flag.String("indexType", bleve.Config.DefaultIndexType, "index type to build")
var kvType = flag.String("kvType", bleve.Config.DefaultKVStore, "kv store type to build")
var segType = flag.String("segType", "", "force scorch segment type")
var segVer = flag.Int("segVer", 0, "force scorch segment version")

func TestIntegration(t *testing.T) {

	flag.Parse()

	t.Logf("using index type %s and kv type %s", *indexType, *kvType)
	if *segType != "" {
		t.Logf("forcing segment type: %s", *segType)
	}
	if *segVer != 0 {
		t.Logf("forcing segment version: %d", *segVer)
	}

	var err error
	var datasetRegexp *regexp.Regexp
	if *dataset != "" {
		datasetRegexp, err = regexp.Compile(*dataset)
		if err != nil {
			t.Fatal(err)
		}
	}

	entries, err := os.ReadDir("tests")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range entries {
		if datasetRegexp != nil {
			if !datasetRegexp.MatchString(f.Name()) {
				continue
			}
		}
		if f.IsDir() {
			t.Logf("Running test: %s", f.Name())
			runTestDir(t, "tests"+string(filepath.Separator)+f.Name(), f.Name())
		}
	}
}

func runTestDir(t *testing.T, dir, datasetName string) {
	// read the mapping
	mappingBytes, err := os.ReadFile(dir + string(filepath.Separator) + "mapping.json")
	if err != nil {
		t.Errorf("error reading mapping: %v", err)
		return
	}
	var mapping mapping.IndexMappingImpl
	err = json.Unmarshal(mappingBytes, &mapping)
	if err != nil {
		t.Errorf("error unmarshalling mapping: %v", err)
		return
	}

	var index bleve.Index
	var cleanup func()

	// if there is a dir named 'data' open single index
	_, err = os.Stat(dir + string(filepath.Separator) + "data")
	if !os.IsNotExist(err) {

		index, cleanup, err = loadDataSet(t, datasetName, mapping, dir+string(filepath.Separator)+"data")
		if err != nil {
			t.Errorf("error loading dataset: %v", err)
			return
		}
		defer cleanup()
	} else {
		// if there is a dir named 'datasets' build alias over each index
		_, err = os.Stat(dir + string(filepath.Separator) + "datasets")
		if !os.IsNotExist(err) {
			index, cleanup, err = loadDataSets(t, datasetName, mapping, dir+string(filepath.Separator)+"datasets")
			if err != nil {
				t.Errorf("error loading dataset: %v", err)
				return
			}
			defer cleanup()
		}
	}

	// read the searches
	searchBytes, err := os.ReadFile(dir + string(filepath.Separator) + "searches.json")
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
	for testNum, search := range searches {
		if *onlynum < 0 || (*onlynum > 0 && testNum == *onlynum) {
			res, err := index.Search(search.Search)
			if err != nil {
				t.Errorf("error running search: %v", err)
			}
			if res.Total != search.Result.Total {
				t.Errorf("test error - %s", search.Comment)
				t.Errorf("test %d - expected total: %d got %d", testNum, search.Result.Total, res.Total)
				continue
			}
			if len(res.Hits) != len(search.Result.Hits) {
				t.Errorf("test error - %s", search.Comment)
				t.Errorf("test %d - expected hits len: %d got %d", testNum, len(search.Result.Hits), len(res.Hits))
				t.Errorf("got hits: %v", res.Hits)
				continue
			}
			for hi, hit := range search.Result.Hits {
				if hit.ID != res.Hits[hi].ID {
					t.Errorf("test error - %s", search.Comment)
					t.Errorf("test %d - expected hit %d to have ID %s got %s", testNum, hi, hit.ID, res.Hits[hi].ID)
				}
				if hit.Fields != nil {
					if !reflect.DeepEqual(hit.Fields, res.Hits[hi].Fields) {
						t.Errorf("test error - %s", search.Comment)
						t.Errorf("test  %d - expected hit %d to have fields %#v got %#v", testNum, hi, hit.Fields, res.Hits[hi].Fields)
					}
				}
				if hit.Fragments != nil {
					if !reflect.DeepEqual(hit.Fragments, res.Hits[hi].Fragments) {
						t.Errorf("test error - %s", search.Comment)
						t.Errorf("test %d - expected hit %d to have fragments %#v got %#v", testNum, hi, hit.Fragments, res.Hits[hi].Fragments)
					}
				}
				if hit.Locations != nil {
					if !reflect.DeepEqual(hit.Locations, res.Hits[hi].Locations) {
						t.Errorf("test error - %s", search.Comment)
						t.Errorf("test %d - expected hit %d to have locations %#v got %#v", testNum, hi, hit.Locations, res.Hits[hi].Locations)
					}
				}
				// assert that none of the scores were NaN,+Inf,-Inf
				if math.IsInf(res.Hits[hi].Score, 0) || math.IsNaN(res.Hits[hi].Score) {
					t.Errorf("test error - %s", search.Comment)
					t.Errorf("test %d - invalid score %f", testNum, res.Hits[hi].Score)
				}
			}
			if search.Result.Facets != nil {
				if !reflect.DeepEqual(search.Result.Facets, res.Facets) {
					t.Errorf("test error - %s", search.Comment)
					t.Errorf("test %d - expected facets: %#v got %#v", testNum, search.Result.Facets, res.Facets)
				}
			}
			if _, ok := index.(bleve.IndexAlias); !ok {
				// check that custom index name is in results
				for _, hit := range res.Hits {
					if hit.Index != datasetName {
						t.Fatalf("expected name: %s, got: %s", datasetName, hit.Index)
					}
				}
			}
		}
	}
}

func loadDataSet(t *testing.T, datasetName string, mapping mapping.IndexMappingImpl, path string) (bleve.Index, func(), error) {
	idxPath := fmt.Sprintf("test-%s.bleve", datasetName)
	cfg := map[string]interface{}{}
	if *segType != "" {
		cfg["forceSegmentType"] = *segType
	}
	if *segVer != 0 {
		cfg["forceSegmentVersion"] = *segVer
	}

	index, err := bleve.NewUsing(idxPath, &mapping, *indexType, *kvType, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("error creating new index: %v", err)
	}
	// set a custom index name
	index.SetName(datasetName)

	// index data
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading data dir: %v", err)
	}
	for _, f := range entries {
		fileBytes, err := os.ReadFile(path + string(filepath.Separator) + f.Name())
		if err != nil {
			return nil, nil, fmt.Errorf("error reading data file: %v", err)
		}
		var fileDoc interface{}
		err = json.Unmarshal(fileBytes, &fileDoc)
		if err != nil {
			return nil, nil, fmt.Errorf("error parsing data file as json: %v", err)
		}
		filename := f.Name()
		ext := filepath.Ext(filename)
		id := filename[0 : len(filename)-len(ext)]
		err = index.Index(id, fileDoc)
		if err != nil {
			return nil, nil, fmt.Errorf("error indexing data: %v", err)
		}
	}
	cleanup := func() {
		err := index.Close()
		if err != nil {
			t.Fatalf("error closing index: %v", err)
		}
		if !*keepIndex {
			err := os.RemoveAll(idxPath)
			if err != nil {
				t.Fatalf("error removing index: %v", err)
			}
		}
	}
	return index, cleanup, nil
}

func loadDataSets(t *testing.T, datasetName string, mapping mapping.IndexMappingImpl, path string) (bleve.Index, func(), error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, nil, fmt.Errorf("error reading datasets dir: %v", err)
	}
	var cleanups []func()
	alias := bleve.NewIndexAlias()
	for _, f := range entries {
		idx, idxCleanup, err := loadDataSet(t, f.Name(), mapping, path+string(filepath.Separator)+f.Name())
		if err != nil {
			return nil, nil, fmt.Errorf("error loading dataset: %v", err)
		}
		cleanups = append(cleanups, idxCleanup)
		alias.Add(idx)
	}
	alias.SetName(datasetName)

	cleanupAll := func() {
		for _, cleanup := range cleanups {
			cleanup()
		}
	}

	return alias, cleanupAll, nil
}
