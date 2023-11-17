//  Copyright (c) 2023 Couchbase, Inc.
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

//go:build vectors && vectors
// +build vectors,vectors

package bleve

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
)

// Test to see if KNN Operators get added right to the query.
func TestKNNOperator(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	dataset := make([]map[string]interface{}, 10)

	// Indexing just a few docs to populate index.
	for i := 0; i < 10; i++ {
		docVec := []float32{}
		for i := 0; i < 5; i++ {
			docVec = append(docVec, rand.Float32())
		}
		dataset = append(dataset, map[string]interface{}{
			"type":    "vectorStuff",
			"content": strconv.Itoa(i),
			"vector":  docVec,
		})
	}

	indexMapping := NewIndexMapping()
	indexMapping.TypeField = "type"
	indexMapping.DefaultAnalyzer = "en"
	documentMapping := NewDocumentMapping()
	indexMapping.AddDocumentMapping("vectorStuff", documentMapping)

	contentFieldMapping := NewTextFieldMapping()
	contentFieldMapping.Index = true
	contentFieldMapping.Store = true
	documentMapping.AddFieldMappingsAt("content", contentFieldMapping)

	vecFieldMapping := mapping.NewVectorFieldMapping()
	vecFieldMapping.Index = true
	vecFieldMapping.Dims = 5
	vecFieldMapping.Similarity = "dot_product"
	documentMapping.AddFieldMappingsAt("vector", vecFieldMapping)

	index, err := New(tmpIndexPath, indexMapping)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	batch := index.NewBatch()
	for i := 0; i < len(dataset); i++ {
		batch.Index(strconv.Itoa(i), dataset[i])
	}

	err = index.Batch(batch)
	if err != nil {
		log.Fatal(err)
	}

	termQuery := query.NewTermQuery("world")

	searchRequest := NewSearchRequest(termQuery)
	queryVec2 := getQueryVec("hilly region worldwide")
	searchRequest.AddKNN("vector", queryVec2, 3, 2.0)
	searchRequest.AddKNN("vector", queryVec2, 2, 1.5)
	searchRequest.Fields = []string{"content", "vector"}

	// Conjunction
	searchRequest.AddKNNOperator(knnOperatorAnd)
	conjunction, err := queryWithKNN(searchRequest)
	if err != nil {
		log.Fatal(fmt.Errorf("unexpected error for AND knn operator"))
	}

	conj, ok := conjunction.(*query.ConjunctionQuery)
	if !ok {
		log.Fatal(fmt.Errorf("expected conjunction query"))
	}

	if len(conj.Conjuncts) == 3 {
		_, ok := conj.Conjuncts[0].(*query.TermQuery)
		if !ok {
			log.Fatal(fmt.Errorf("expected first query to be a term query,"+
				" but it's %T", conj.Conjuncts[0]))
		}
	} else {
		log.Fatal(fmt.Errorf("expected 3 conjuncts"))
	}

	// Disjunction
	searchRequest.AddKNNOperator(knnOperatorOr)
	disjunction, err := queryWithKNN(searchRequest)
	if err != nil {
		log.Fatal(fmt.Errorf("unexpected error for OR knn operator"))
	}

	disj, ok := disjunction.(*query.DisjunctionQuery)
	if !ok {
		log.Fatal(fmt.Errorf("expected disjunction query"))
	}

	if len(disj.Disjuncts) == 3 {
		_, ok := disj.Disjuncts[0].(*query.TermQuery)
		if !ok {
			log.Fatal(fmt.Errorf("expected first query to be a term query,"+
				" but it's %T", conj.Conjuncts[0]))
		}
	} else {
		log.Fatal(fmt.Errorf("expected 3 disjuncts"))
	}

	// Incorrect operator.
	searchRequest.AddKNNOperator("bs_op")
	searchRequest.Query, err = queryWithKNN(searchRequest)
	if err == nil {
		log.Fatal(fmt.Errorf("expected error for incorrect knn operator"))
	}
}
