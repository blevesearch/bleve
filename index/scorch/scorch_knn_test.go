//  Copyright (c) 2025 Couchbase, Inc.
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

//go:build vectors
// +build vectors

package scorch

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/mapping"
	index "github.com/blevesearch/bleve_index_api"
)

func TestIndexSnapshotHighestCardinalityCentroids(t *testing.T) {
	cfg := CreateConfig("TestIndexSnapshotHighestCardinalityCentroids")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := DestroyTest(cfg)
		if err != nil {
			t.Log(err)
		}
	}()

	testConfig := cfg
	mp := mapping.NewIndexMapping()

	vectorDims := 5

	vecMapping := mapping.NewVectorFieldMapping()
	vecMapping.Dims = vectorDims
	vecMapping.Similarity = index.CosineSimilarity

	docMapping := mapping.NewDocumentStaticMapping()
	docMapping.AddFieldMappingsAt("vec", vecMapping)
	mp.DefaultMapping = docMapping

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch("storeName", testConfig, analysisQueue)
	if err != nil {
		log.Fatalln(err)
	}
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	rand.Seed(time.Now().UnixNano())
	min, max := float32(-10.0), float32(10.0)
	genRandomVector := func() []float32 {
		vec := make([]float32, vectorDims)
		for i := range vec {
			vec[i] = min + rand.Float32()*(max-min)
		}
		return vec
	}

	var batch *index.Batch
	for i := 1; i <= 20000; i++ {
		doc := document.NewDocument(fmt.Sprintf("doc-%d", i))
		err = mp.MapDocument(doc, map[string]interface{}{
			"vec": genRandomVector(),
		})
		if err != nil {
			t.Errorf("error mapping doc: %v", err)
		}
		if batch == nil {
			batch = index.NewBatch()
		}
		batch.Update(doc)

		if i%200 == 0 {
			err = idx.Batch(batch)
			if err != nil {
				t.Errorf("Error adding batch to index: %v", err)
			}
			batch = nil
		}
	}

	if batch != nil {
		// In case doc count is not a multiple of 200, we need to add the final batch
		err = idx.Batch(batch)
		if err != nil {
			t.Errorf("Error adding final batch to index: %v", err)
		}
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	limit := 5
	if snap, ok := reader.(*IndexSnapshot); ok {
		centroids, err := snap.HighestCardinalityCentroids("vec", limit)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println(centroids)
	}
}
