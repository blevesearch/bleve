//  Copyright (c) 2023 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build vectors
// +build vectors

package bleve

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/blevesearch/bleve/v2/analysis/lang/en"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

const testInputCompressedFile = "test/knn/knn_dataset_queries.zip"
const testDatasetFileName = "knn_dataset.json"
const testQueryFileName = "knn_queries.json"

const testDatasetDims = 384

var knnOperators []knnOperator = []knnOperator{knnOperatorAnd, knnOperatorOr}

func TestSimilaritySearchPartitionedIndex(t *testing.T) {
	dataset, searchRequests, err := readDatasetAndQueries(testInputCompressedFile)
	if err != nil {
		t.Fatal(err)
	}
	documents := makeDatasetIntoDocuments(dataset)
	contentFieldMapping := NewTextFieldMapping()
	contentFieldMapping.Analyzer = en.AnalyzerName

	vecFieldMappingL2 := mapping.NewVectorFieldMapping()
	vecFieldMappingL2.Dims = testDatasetDims
	vecFieldMappingL2.Similarity = index.EuclideanDistance

	indexMappingL2Norm := NewIndexMapping()
	indexMappingL2Norm.DefaultMapping.AddFieldMappingsAt("content", contentFieldMapping)
	indexMappingL2Norm.DefaultMapping.AddFieldMappingsAt("vector", vecFieldMappingL2)

	vecFieldMappingDot := mapping.NewVectorFieldMapping()
	vecFieldMappingDot.Dims = testDatasetDims
	vecFieldMappingDot.Similarity = index.InnerProduct

	indexMappingDotProduct := NewIndexMapping()
	indexMappingDotProduct.DefaultMapping.AddFieldMappingsAt("content", contentFieldMapping)
	indexMappingDotProduct.DefaultMapping.AddFieldMappingsAt("vector", vecFieldMappingDot)

	vecFieldMappingCosine := mapping.NewVectorFieldMapping()
	vecFieldMappingCosine.Dims = testDatasetDims
	vecFieldMappingCosine.Similarity = index.CosineSimilarity

	indexMappingCosine := NewIndexMapping()
	indexMappingCosine.DefaultMapping.AddFieldMappingsAt("content", contentFieldMapping)
	indexMappingCosine.DefaultMapping.AddFieldMappingsAt("vector", vecFieldMappingCosine)

	type testCase struct {
		testType           string
		queryIndex         int
		numIndexPartitions int
		mapping            mapping.IndexMapping
	}

	testCases := []testCase{
		// l2 norm similarity
		{
			testType:           "multi_partition:match_none:oneKNNreq:k=3",
			queryIndex:         0,
			numIndexPartitions: 4,
			mapping:            indexMappingL2Norm,
		},
		{
			testType:           "multi_partition:match_none:oneKNNreq:k=2",
			queryIndex:         0,
			numIndexPartitions: 10,
			mapping:            indexMappingL2Norm,
		},
		{
			testType:           "multi_partition:match:oneKNNreq:k=2",
			queryIndex:         1,
			numIndexPartitions: 5,
			mapping:            indexMappingL2Norm,
		},
		{
			testType:           "multi_partition:disjunction:twoKNNreq:k=2,2",
			queryIndex:         2,
			numIndexPartitions: 4,
			mapping:            indexMappingL2Norm,
		},
		// dot product similarity
		{
			testType:           "multi_partition:match_none:oneKNNreq:k=3",
			queryIndex:         0,
			numIndexPartitions: 4,
			mapping:            indexMappingDotProduct,
		},
		{
			testType:           "multi_partition:match_none:oneKNNreq:k=2",
			queryIndex:         0,
			numIndexPartitions: 10,
			mapping:            indexMappingDotProduct,
		},
		{
			testType:           "multi_partition:match:oneKNNreq:k=2",
			queryIndex:         1,
			numIndexPartitions: 5,
			mapping:            indexMappingDotProduct,
		},
		{
			testType:           "multi_partition:disjunction:twoKNNreq:k=2,2",
			queryIndex:         2,
			numIndexPartitions: 4,
			mapping:            indexMappingDotProduct,
		},
		// cosine similarity
		{
			testType:           "multi_partition:match_none:oneKNNreq:k=3",
			queryIndex:         0,
			numIndexPartitions: 7,
			mapping:            indexMappingCosine,
		},
		{
			testType:           "multi_partition:match_none:oneKNNreq:k=2",
			queryIndex:         0,
			numIndexPartitions: 5,
			mapping:            indexMappingCosine,
		},
		{
			testType:           "multi_partition:match:oneKNNreq:k=2",
			queryIndex:         1,
			numIndexPartitions: 3,
			mapping:            indexMappingCosine,
		},
		{
			testType:           "multi_partition:disjunction:twoKNNreq:k=2,2",
			queryIndex:         2,
			numIndexPartitions: 9,
			mapping:            indexMappingCosine,
		},
	}

	index := NewIndexAlias()
	var reqSort = search.SortOrder{&search.SortScore{Desc: true}, &search.SortDocID{Desc: true}, &search.SortField{Desc: false, Field: "content"}}
	for testCaseNum, testCase := range testCases {
		originalRequest := searchRequests[testCase.queryIndex]
		for _, operator := range knnOperators {

			index.indexes = make([]Index, 0)
			query := copySearchRequest(originalRequest, nil)
			query.AddKNNOperator(operator)
			query.Sort = reqSort.Copy()
			query.Explain = true

			nameToIndex := createPartitionedIndex(documents, index, 1, testCase.mapping, t, false)
			controlResult, err := index.Search(query)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}
			if !finalHitsHaveValidIndex(controlResult.Hits, nameToIndex) {
				cleanUp(t, nameToIndex)
				t.Fatalf("test case #%d failed: expected control result hits to have valid `Index`", testCaseNum)
			}
			cleanUp(t, nameToIndex)

			index.indexes = make([]Index, 0)
			query = copySearchRequest(originalRequest, nil)
			query.AddKNNOperator(operator)
			query.Sort = reqSort.Copy()
			query.Explain = true

			nameToIndex = createPartitionedIndex(documents, index, testCase.numIndexPartitions, testCase.mapping, t, false)
			experimentalResult, err := index.Search(query)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}
			if !finalHitsHaveValidIndex(experimentalResult.Hits, nameToIndex) {
				cleanUp(t, nameToIndex)
				t.Fatalf("test case #%d failed: expected experimental Result hits to have valid `Index`", testCaseNum)
			}
			verifyResult(t, controlResult, experimentalResult, testCaseNum, true)
			cleanUp(t, nameToIndex)

			index.indexes = make([]Index, 0)
			query = copySearchRequest(originalRequest, nil)
			query.AddKNNOperator(operator)
			query.Sort = reqSort.Copy()
			query.Explain = true

			nameToIndex = createPartitionedIndex(documents, index, testCase.numIndexPartitions, testCase.mapping, t, true)
			multiLevelIndexResult, err := index.Search(query)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}
			if !finalHitsHaveValidIndex(multiLevelIndexResult.Hits, nameToIndex) {
				cleanUp(t, nameToIndex)
				t.Fatalf("test case #%d failed: expected experimental Result hits to have valid `Index`", testCaseNum)
			}
			verifyResult(t, multiLevelIndexResult, experimentalResult, testCaseNum, false)
			cleanUp(t, nameToIndex)

		}
	}

	var facets = map[string]*FacetRequest{
		"content": {
			Field: "content",
			Size:  10,
		},
	}

	index = NewIndexAlias()
	for testCaseNum, testCase := range testCases {
		index.indexes = make([]Index, 0)
		nameToIndex := createPartitionedIndex(documents, index, testCase.numIndexPartitions, testCase.mapping, t, false)
		originalRequest := searchRequests[testCase.queryIndex]
		for _, operator := range knnOperators {
			from, size := originalRequest.From, originalRequest.Size
			query := copySearchRequest(originalRequest, nil)
			query.AddKNNOperator(operator)
			query.Explain = true
			query.From = from
			query.Size = size

			// Three types of queries to run wrt sort and facet fields that require fields.
			// 1. Sort And Facet are there
			// 2. Sort is there, Facet is not there
			// 3. Sort is not there, Facet is there
			// The case where both sort and facet are not there is already covered in the previous tests.

			// 1. Sort And Facet are there
			query.Facets = facets
			query.Sort = reqSort.Copy()

			res1, err := index.Search(query)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}
			if !finalHitsHaveValidIndex(res1.Hits, nameToIndex) {
				cleanUp(t, nameToIndex)
				t.Fatalf("test case #%d failed: expected experimental Result hits to have valid `Index`", testCaseNum)
			}

			facetRes1 := res1.Facets
			facetRes1Str, err := json.Marshal(facetRes1)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}

			// 2. Sort is there, Facet is not there
			query.Facets = nil
			query.Sort = reqSort.Copy()

			res2, err := index.Search(query)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}
			if !finalHitsHaveValidIndex(res2.Hits, nameToIndex) {
				cleanUp(t, nameToIndex)
				t.Fatalf("test case #%d failed: expected experimental Result hits to have valid `Index`", testCaseNum)
			}

			// 3. Sort is not there, Facet is there
			query.Facets = facets
			query.Sort = nil
			res3, err := index.Search(query)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}
			if !finalHitsHaveValidIndex(res3.Hits, nameToIndex) {
				cleanUp(t, nameToIndex)
				t.Fatalf("test case #%d failed: expected experimental Result hits to have valid `Index`", testCaseNum)
			}

			facetRes3 := res3.Facets
			facetRes3Str, err := json.Marshal(facetRes3)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}

			// Verify the facet results
			if string(facetRes1Str) != string(facetRes3Str) {
				cleanUp(t, nameToIndex)
				t.Fatalf("test case #%d failed: expected facet results to be equal", testCaseNum)
			}

			// Verify the results
			verifyResult(t, res1, res2, testCaseNum, false)
			verifyResult(t, res2, res3, testCaseNum, true)

			// Test early exit fail case -> matchNone + facetRequest
			query.Query = NewMatchNoneQuery()
			query.Sort = reqSort.Copy()
			// control case
			query.Facets = nil
			res4Ctrl, err := index.Search(query)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}
			if !finalHitsHaveValidIndex(res4Ctrl.Hits, nameToIndex) {
				cleanUp(t, nameToIndex)
				t.Fatalf("test case #%d failed: expected control Result hits to have valid `Index`", testCaseNum)
			}

			// experimental case
			query.Facets = facets
			res4Exp, err := index.Search(query)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}
			if !finalHitsHaveValidIndex(res4Exp.Hits, nameToIndex) {
				cleanUp(t, nameToIndex)
				t.Fatalf("test case #%d failed: expected experimental Result hits to have valid `Index`", testCaseNum)
			}

			if !(operator == knnOperatorAnd && res4Ctrl.Total == 0 && res4Exp.Total == 0) {
				// catch case where no hits are returned
				// due to matchNone query with a KNN request with operator AND
				// where no hits are part of the intersection in multi knn request
				verifyResult(t, res4Ctrl, res4Exp, testCaseNum, false)
			}
		}
		cleanUp(t, nameToIndex)
	}

	// Test Pagination with multi partitioned index
	index = NewIndexAlias()
	index.indexes = make([]Index, 0)
	nameToIndex := createPartitionedIndex(documents, index, 8, indexMappingL2Norm, t, true)

	// Test From + Size pagination for Hybrid Search (2-Phase)
	query := copySearchRequest(searchRequests[4], nil)
	query.Sort = reqSort.Copy()
	query.Facets = facets
	query.Explain = true

	testFromSizePagination(t, query, index, nameToIndex)

	// Test From + Size pagination for Early Exit Hybrid Search (1-Phase)
	query = copySearchRequest(searchRequests[4], nil)
	query.Query = NewMatchNoneQuery()
	query.Sort = reqSort.Copy()
	query.Facets = nil
	query.Explain = true

	testFromSizePagination(t, query, index, nameToIndex)

	cleanUp(t, nameToIndex)
}

func testFromSizePagination(t *testing.T, query *SearchRequest, index Index, nameToIndex map[string]Index) {
	query.From = 0
	query.Size = 30

	resCtrl, err := index.Search(query)
	if err != nil {
		cleanUp(t, nameToIndex)
		t.Fatal(err)
	}

	ctrlHitIds := make([]string, len(resCtrl.Hits))
	for i, doc := range resCtrl.Hits {
		ctrlHitIds[i] = doc.ID
	}
	// experimental case

	fromValues := []int{0, 5, 10, 15, 20, 25}
	size := 5
	for fromIdx := 0; fromIdx < len(fromValues); fromIdx++ {
		from := fromValues[fromIdx]
		query.From = from
		query.Size = size
		resExp, err := index.Search(query)
		if err != nil {
			cleanUp(t, nameToIndex)
			t.Fatal(err)
		}
		if from >= len(ctrlHitIds) {
			if len(resExp.Hits) != 0 {
				cleanUp(t, nameToIndex)
				t.Fatalf("expected 0 hits, got %d", len(resExp.Hits))
			}
			continue
		}
		numHitsExp := len(resExp.Hits)
		numHitsCtrl := min(len(ctrlHitIds)-from, size)
		if numHitsExp != numHitsCtrl {
			cleanUp(t, nameToIndex)
			t.Fatalf("expected %d hits, got %d", numHitsCtrl, numHitsExp)
		}
		for i := 0; i < numHitsExp; i++ {
			doc := resExp.Hits[i]
			startOffset := from + i
			if doc.ID != ctrlHitIds[startOffset] {
				cleanUp(t, nameToIndex)
				t.Fatalf("expected %s at index %d, got %s", ctrlHitIds[startOffset], i, doc.ID)
			}
		}
	}
}

func TestVectorBase64Index(t *testing.T) {
	dataset, searchRequests, err := readDatasetAndQueries(testInputCompressedFile)
	if err != nil {
		t.Fatal(err)
	}
	documents := makeDatasetIntoDocuments(dataset)

	_, searchRequestsCopy, err := readDatasetAndQueries(testInputCompressedFile)
	if err != nil {
		t.Fatal(err)
	}

	for _, doc := range documents {
		vec, ok := doc["vector"].([]float32)
		if !ok {
			t.Fatal("Typecasting vector to float array failed")
		}

		buf := new(bytes.Buffer)
		for _, v := range vec {
			err := binary.Write(buf, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}

		doc["vectorEncoded"] = base64.StdEncoding.EncodeToString(buf.Bytes())
	}

	for _, sr := range searchRequestsCopy {
		for _, kr := range sr.KNN {
			kr.Field = "vectorEncoded"
		}
	}

	contentFM := NewTextFieldMapping()
	contentFM.Analyzer = en.AnalyzerName

	vecFML2 := mapping.NewVectorFieldMapping()
	vecFML2.Dims = testDatasetDims
	vecFML2.Similarity = index.EuclideanDistance

	vecBFML2 := mapping.NewVectorBase64FieldMapping()
	vecBFML2.Dims = testDatasetDims
	vecBFML2.Similarity = index.EuclideanDistance

	vecFMDot := mapping.NewVectorFieldMapping()
	vecFMDot.Dims = testDatasetDims
	vecFMDot.Similarity = index.InnerProduct

	vecBFMDot := mapping.NewVectorBase64FieldMapping()
	vecBFMDot.Dims = testDatasetDims
	vecBFMDot.Similarity = index.InnerProduct

	indexMappingL2 := NewIndexMapping()
	indexMappingL2.DefaultMapping.AddFieldMappingsAt("content", contentFM)
	indexMappingL2.DefaultMapping.AddFieldMappingsAt("vector", vecFML2)
	indexMappingL2.DefaultMapping.AddFieldMappingsAt("vectorEncoded", vecBFML2)

	indexMappingDot := NewIndexMapping()
	indexMappingDot.DefaultMapping.AddFieldMappingsAt("content", contentFM)
	indexMappingDot.DefaultMapping.AddFieldMappingsAt("vector", vecFMDot)
	indexMappingDot.DefaultMapping.AddFieldMappingsAt("vectorEncoded", vecBFMDot)

	tmpIndexPathL2 := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPathL2)

	tmpIndexPathDot := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPathDot)

	indexL2, err := New(tmpIndexPathL2, indexMappingL2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := indexL2.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	indexDot, err := New(tmpIndexPathDot, indexMappingDot)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := indexDot.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	batchL2 := indexL2.NewBatch()
	batchDot := indexDot.NewBatch()

	for _, doc := range documents {
		err = batchL2.Index(doc["id"].(string), doc)
		if err != nil {
			t.Fatal(err)
		}
		err = batchDot.Index(doc["id"].(string), doc)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = indexL2.Batch(batchL2)
	if err != nil {
		t.Fatal(err)
	}

	err = indexDot.Batch(batchDot)
	if err != nil {
		t.Fatal(err)
	}

	for i, _ := range searchRequests {
		for _, operator := range knnOperators {
			controlQuery := searchRequests[i]
			testQuery := searchRequestsCopy[i]

			controlQuery.AddKNNOperator(operator)
			testQuery.AddKNNOperator(operator)

			controlResultL2, err := indexL2.Search(controlQuery)
			if err != nil {
				t.Fatal(err)
			}
			testResultL2, err := indexL2.Search(testQuery)
			if err != nil {
				t.Fatal(err)
			}

			if controlResultL2 != nil && testResultL2 != nil {
				if len(controlResultL2.Hits) == len(testResultL2.Hits) {
					for j, _ := range controlResultL2.Hits {
						if controlResultL2.Hits[j].ID != testResultL2.Hits[j].ID {
							t.Fatalf("testcase %d failed: expected hit id %s, got hit id %s", i, controlResultL2.Hits[j].ID, testResultL2.Hits[j].ID)
						}
					}
				}
			} else if (controlResultL2 == nil && testResultL2 != nil) ||
				(controlResultL2 != nil && testResultL2 == nil) {
				t.Fatalf("testcase %d failed: expected result %s, got result %s", i, controlResultL2, testResultL2)
			}

			controlResultDot, err := indexDot.Search(controlQuery)
			if err != nil {
				t.Fatal(err)
			}
			testResultDot, err := indexDot.Search(testQuery)
			if err != nil {
				t.Fatal(err)
			}

			if controlResultDot != nil && testResultDot != nil {
				if len(controlResultDot.Hits) == len(testResultDot.Hits) {
					for j, _ := range controlResultDot.Hits {
						if controlResultDot.Hits[j].ID != testResultDot.Hits[j].ID {
							t.Fatalf("testcase %d failed: expected hit id %s, got hit id %s", i, controlResultDot.Hits[j].ID, testResultDot.Hits[j].ID)
						}
					}
				}
			} else if (controlResultDot == nil && testResultDot != nil) ||
				(controlResultDot != nil && testResultDot == nil) {
				t.Fatalf("testcase %d failed: expected result %s, got result %s", i, controlResultDot, testResultDot)
			}
		}
	}
}

type testDocument struct {
	ID      string    `json:"id"`
	Content string    `json:"content"`
	Vector  []float32 `json:"vector"`
}

func readDatasetAndQueries(fileName string) ([]testDocument, []*SearchRequest, error) {
	// Open the zip archive for reading
	r, err := zip.OpenReader(fileName)
	if err != nil {
		return nil, nil, err
	}
	var dataset []testDocument
	var queries []*SearchRequest

	defer r.Close()
	for _, f := range r.File {
		jsonFile, err := f.Open()
		if err != nil {
			return nil, nil, err
		}
		defer jsonFile.Close()
		if f.Name == testDatasetFileName {
			err = json.NewDecoder(jsonFile).Decode(&dataset)
			if err != nil {
				return nil, nil, err
			}
		} else if f.Name == testQueryFileName {
			err = json.NewDecoder(jsonFile).Decode(&queries)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return dataset, queries, nil
}

func makeDatasetIntoDocuments(dataset []testDocument) []map[string]interface{} {
	documents := make([]map[string]interface{}, len(dataset))
	for i := 0; i < len(dataset); i++ {
		document := make(map[string]interface{})
		document["id"] = dataset[i].ID
		document["content"] = dataset[i].Content
		document["vector"] = dataset[i].Vector
		documents[i] = document
	}
	return documents
}

func cleanUp(t *testing.T, nameToIndex map[string]Index) {
	for path, childIndex := range nameToIndex {
		err := childIndex.Close()
		if err != nil {
			t.Fatal(err)
		}
		cleanupTmpIndexPath(t, path)
	}
}

func createChildIndex(docs []map[string]interface{}, mapping mapping.IndexMapping, t *testing.T, nameToIndex map[string]Index) Index {
	tmpIndexPath := createTmpIndexPath(t)
	index, err := New(tmpIndexPath, mapping)
	if err != nil {
		t.Fatal(err)
	}
	nameToIndex[index.Name()] = index
	batch := index.NewBatch()
	for _, doc := range docs {
		err := batch.Index(doc["id"].(string), doc)
		if err != nil {
			cleanUp(t, nameToIndex)
			t.Fatal(err)
		}
	}
	err = index.Batch(batch)
	if err != nil {
		cleanUp(t, nameToIndex)
		t.Fatal(err)
	}
	return index
}

func createPartitionedIndex(documents []map[string]interface{}, index *indexAliasImpl, numPartitions int,
	mapping mapping.IndexMapping, t *testing.T, multiLevel bool) map[string]Index {

	partitionSize := len(documents) / numPartitions
	extraDocs := len(documents) % numPartitions
	numDocsPerPartition := make([]int, numPartitions)
	for i := 0; i < numPartitions; i++ {
		numDocsPerPartition[i] = partitionSize
		if extraDocs > 0 {
			numDocsPerPartition[i]++
			extraDocs--
		}
	}
	docsPerPartition := make([][]map[string]interface{}, numPartitions)
	prevCutoff := 0
	for i := 0; i < numPartitions; i++ {
		docsPerPartition[i] = make([]map[string]interface{}, numDocsPerPartition[i])
		for j := 0; j < numDocsPerPartition[i]; j++ {
			docsPerPartition[i][j] = documents[prevCutoff+j]
		}
		prevCutoff += numDocsPerPartition[i]
	}

	rv := make(map[string]Index)
	if !multiLevel {
		// all indexes are at the same level
		for i := 0; i < numPartitions; i++ {
			index.Add(createChildIndex(docsPerPartition[i], mapping, t, rv))
		}
	} else {
		// alias tree
		indexes := make([]Index, numPartitions)
		for i := 0; i < numPartitions; i++ {
			indexes[i] = createChildIndex(docsPerPartition[i], mapping, t, rv)
		}
		numAlias := int(math.Ceil(float64(numPartitions) / 2.0))
		aliases := make([]IndexAlias, numAlias)
		for i := 0; i < numAlias; i++ {
			aliases[i] = NewIndexAlias()
			aliases[i].SetName(fmt.Sprintf("alias%d", i))
			for j := 0; j < 2; j++ {
				if i*2+j < numPartitions {
					aliases[i].Add(indexes[i*2+j])
				}
			}
		}
		for i := 0; i < numAlias; i++ {
			index.Add(aliases[i])
		}
	}
	return rv
}

func createMultipleSegmentsIndex(documents []map[string]interface{}, index Index, numSegments int) error {
	// create multiple batches to simulate more than one segment
	numBatches := numSegments

	batches := make([]*Batch, numBatches)
	numDocsPerBatch := len(documents) / numBatches
	extraDocs := len(documents) % numBatches

	docsPerBatch := make([]int, numBatches)
	for i := 0; i < numBatches; i++ {
		docsPerBatch[i] = numDocsPerBatch
		if extraDocs > 0 {
			docsPerBatch[i]++
			extraDocs--
		}
	}
	prevCutoff := 0
	for i := 0; i < numBatches; i++ {
		batches[i] = index.NewBatch()
		for j := prevCutoff; j < prevCutoff+docsPerBatch[i]; j++ {
			doc := documents[j]
			err := batches[i].Index(doc["id"].(string), doc)
			if err != nil {
				return err
			}
		}
		prevCutoff += docsPerBatch[i]
	}
	errMutex := sync.Mutex{}
	var errors []error
	wg := sync.WaitGroup{}
	wg.Add(len(batches))
	for i, batch := range batches {
		go func(ix int, batchx *Batch) {
			defer wg.Done()
			err := index.Batch(batchx)
			if err != nil {
				errMutex.Lock()
				errors = append(errors, err)
				errMutex.Unlock()
			}
		}(i, batch)
	}
	wg.Wait()
	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}

func truncateScore(score float64) float64 {
	epsilon := 1e-4
	truncated := float64(int(score*1e6)) / 1e6
	if math.Abs(truncated-1.0) <= epsilon {
		return 1.0
	}
	return truncated
}

// Function to compare two Explanation structs recursively
func compareExplanation(a, b *search.Explanation) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	if truncateScore(a.Value) != truncateScore(b.Value) || len(a.Children) != len(b.Children) {
		return false
	}

	// Sort the children slices before comparison
	sortChildren(a.Children)
	sortChildren(b.Children)

	for i := range a.Children {
		if !compareExplanation(a.Children[i], b.Children[i]) {
			return false
		}
	}
	return true
}

// Function to sort the children slices
func sortChildren(children []*search.Explanation) {
	sort.Slice(children, func(i, j int) bool {
		return children[i].Value < children[j].Value
	})
}

// All hits from a hybrid search/knn search should not have
// index names or score breakdown.
func finalHitsOmitKNNMetadata(hits []*search.DocumentMatch) bool {
	for _, hit := range hits {
		if hit.IndexNames != nil || hit.ScoreBreakdown != nil {
			return false
		}
	}
	return true
}

func finalHitsHaveValidIndex(hits []*search.DocumentMatch, indexes map[string]Index) bool {
	for _, hit := range hits {
		if hit.Index == "" {
			return false
		}
		var idx Index
		var ok bool
		if idx, ok = indexes[hit.Index]; !ok {
			return false
		}
		if idx == nil {
			return false
		}
		var doc index.Document
		doc, err = idx.Document(hit.ID)
		if err != nil {
			return false
		}
		if doc == nil {
			return false
		}
	}
	return true
}

func verifyResult(t *testing.T, controlResult *SearchResult, experimentalResult *SearchResult, testCaseNum int, verifyOnlyDocIDs bool) {
	if controlResult.Hits.Len() == 0 || experimentalResult.Hits.Len() == 0 {
		t.Fatalf("test case #%d failed: 0 hits returned", testCaseNum)
	}
	if len(controlResult.Hits) != len(experimentalResult.Hits) {
		t.Fatalf("test case #%d failed: expected %d results, got %d", testCaseNum, len(controlResult.Hits), len(experimentalResult.Hits))
	}
	if controlResult.Total != experimentalResult.Total {
		t.Fatalf("test case #%d failed: expected total hits to be %d, got %d", testCaseNum, controlResult.Total, experimentalResult.Total)
	}
	// KNN Metadata -> Score Breakdown and IndexNames MUST be omitted from the final hits
	if !finalHitsOmitKNNMetadata(controlResult.Hits) || !finalHitsOmitKNNMetadata(experimentalResult.Hits) {
		t.Fatalf("test case #%d failed: expected no KNN metadata in hits", testCaseNum)
	}
	if controlResult.Took == 0 || experimentalResult.Took == 0 {
		t.Fatalf("test case #%d failed: expected non-zero took time", testCaseNum)
	}
	if controlResult.Request == nil || experimentalResult.Request == nil {
		t.Fatalf("test case #%d failed: expected non-nil request", testCaseNum)
	}
	if verifyOnlyDocIDs {
		// in multi partitioned index, we cannot be sure of the score or the ordering of the hits as the tf-idf scores are localized to each partition
		// so we only check the ids
		controlMap := make(map[string]struct{})
		experimentalMap := make(map[string]struct{})
		for _, hit := range controlResult.Hits {
			controlMap[hit.ID] = struct{}{}
		}
		for _, hit := range experimentalResult.Hits {
			experimentalMap[hit.ID] = struct{}{}
		}
		if len(controlMap) != len(experimentalMap) {
			t.Fatalf("test case #%d failed: expected %d results, got %d", testCaseNum, len(controlMap), len(experimentalMap))
		}
		for id := range controlMap {
			if _, ok := experimentalMap[id]; !ok {
				t.Fatalf("test case #%d failed: expected id %s to be in experimental result", testCaseNum, id)
			}
		}
		return
	}
	for i := 0; i < len(controlResult.Hits); i++ {
		if controlResult.Hits[i].ID != experimentalResult.Hits[i].ID {
			t.Fatalf("test case #%d failed: expected hit %d to have id %s, got %s", testCaseNum, i, controlResult.Hits[i].ID, experimentalResult.Hits[i].ID)
		}
		// Truncate to 6 decimal places
		actualScore := truncateScore(experimentalResult.Hits[i].Score)
		expectScore := truncateScore(controlResult.Hits[i].Score)
		if expectScore != actualScore {
			t.Fatalf("test case #%d failed: expected hit %d to have score %f, got %f", testCaseNum, i, expectScore, actualScore)
		}
		if !compareExplanation(controlResult.Hits[i].Expl, experimentalResult.Hits[i].Expl) {
			t.Fatalf("test case #%d failed: expected hit %d to have explanation %v, got %v", testCaseNum, i, controlResult.Hits[i].Expl, experimentalResult.Hits[i].Expl)
		}
	}
	if truncateScore(controlResult.MaxScore) != truncateScore(experimentalResult.MaxScore) {
		t.Fatalf("test case #%d: expected maxScore to be %f, got %f", testCaseNum, controlResult.MaxScore, experimentalResult.MaxScore)
	}
}

func TestSimilaritySearchMultipleSegments(t *testing.T) {
	// using scorch options to prevent merges during the course of this test
	// so that the knnCollector can be accurately tested
	scorch.DefaultMemoryPressurePauseThreshold = 0
	scorch.DefaultMinSegmentsForInMemoryMerge = math.MaxInt
	dataset, searchRequests, err := readDatasetAndQueries(testInputCompressedFile)
	if err != nil {
		t.Fatal(err)
	}
	documents := makeDatasetIntoDocuments(dataset)

	contentFieldMapping := NewTextFieldMapping()
	contentFieldMapping.Analyzer = en.AnalyzerName

	vecFieldMappingL2 := mapping.NewVectorFieldMapping()
	vecFieldMappingL2.Dims = testDatasetDims
	vecFieldMappingL2.Similarity = index.EuclideanDistance

	vecFieldMappingDot := mapping.NewVectorFieldMapping()
	vecFieldMappingDot.Dims = testDatasetDims
	vecFieldMappingDot.Similarity = index.InnerProduct

	vecFieldMappingCosine := mapping.NewVectorFieldMapping()
	vecFieldMappingCosine.Dims = testDatasetDims
	vecFieldMappingCosine.Similarity = index.CosineSimilarity

	indexMappingL2Norm := NewIndexMapping()
	indexMappingL2Norm.DefaultMapping.AddFieldMappingsAt("content", contentFieldMapping)
	indexMappingL2Norm.DefaultMapping.AddFieldMappingsAt("vector", vecFieldMappingL2)

	indexMappingDotProduct := NewIndexMapping()
	indexMappingDotProduct.DefaultMapping.AddFieldMappingsAt("content", contentFieldMapping)
	indexMappingDotProduct.DefaultMapping.AddFieldMappingsAt("vector", vecFieldMappingDot)

	indexMappingCosine := NewIndexMapping()
	indexMappingCosine.DefaultMapping.AddFieldMappingsAt("content", contentFieldMapping)
	indexMappingCosine.DefaultMapping.AddFieldMappingsAt("vector", vecFieldMappingCosine)

	var reqSort = search.SortOrder{&search.SortScore{Desc: true}, &search.SortDocID{Desc: true}, &search.SortField{Desc: false, Field: "content"}}

	testCases := []struct {
		numSegments int
		queryIndex  int
		mapping     mapping.IndexMapping
		scoreValue  string
	}{
		// L2 norm similarity
		{
			numSegments: 6,
			queryIndex:  0,
			mapping:     indexMappingL2Norm,
		},
		{
			numSegments: 7,
			queryIndex:  1,
			mapping:     indexMappingL2Norm,
		},
		{
			numSegments: 8,
			queryIndex:  2,
			mapping:     indexMappingL2Norm,
		},
		{
			numSegments: 9,
			queryIndex:  3,
			mapping:     indexMappingL2Norm,
		},
		{
			numSegments: 10,
			queryIndex:  4,
			mapping:     indexMappingL2Norm,
		},
		{
			numSegments: 11,
			queryIndex:  5,
			mapping:     indexMappingL2Norm,
		},
		// dot_product similarity
		{
			numSegments: 6,
			queryIndex:  0,
			mapping:     indexMappingDotProduct,
		},
		{
			numSegments: 7,
			queryIndex:  1,
			mapping:     indexMappingDotProduct,
		},
		{
			numSegments: 8,
			queryIndex:  2,
			mapping:     indexMappingDotProduct,
		},
		{
			numSegments: 9,
			queryIndex:  3,
			mapping:     indexMappingDotProduct,
		},
		{
			numSegments: 10,
			queryIndex:  4,
			mapping:     indexMappingDotProduct,
		},
		{
			numSegments: 11,
			queryIndex:  5,
			mapping:     indexMappingDotProduct,
		},
		// cosine similarity
		{
			numSegments: 9,
			queryIndex:  0,
			mapping:     indexMappingCosine,
		},
		{
			numSegments: 5,
			queryIndex:  1,
			mapping:     indexMappingCosine,
		},
		{
			numSegments: 4,
			queryIndex:  2,
			mapping:     indexMappingCosine,
		},
		{
			numSegments: 12,
			queryIndex:  3,
			mapping:     indexMappingCosine,
		},
		{
			numSegments: 7,
			queryIndex:  4,
			mapping:     indexMappingCosine,
		},
		{
			numSegments: 11,
			queryIndex:  5,
			mapping:     indexMappingCosine,
		},
		// score none test
		{
			numSegments: 3,
			queryIndex:  0,
			mapping:     indexMappingL2Norm,
			scoreValue:  "none",
		},
		{
			numSegments: 7,
			queryIndex:  1,
			mapping:     indexMappingL2Norm,
			scoreValue:  "none",
		},
		{
			numSegments: 8,
			queryIndex:  2,
			mapping:     indexMappingL2Norm,
			scoreValue:  "none",
		},
		{
			numSegments: 3,
			queryIndex:  0,
			mapping:     indexMappingDotProduct,
			scoreValue:  "none",
		},
		{
			numSegments: 7,
			queryIndex:  1,
			mapping:     indexMappingDotProduct,
			scoreValue:  "none",
		},
		{
			numSegments: 8,
			queryIndex:  2,
			mapping:     indexMappingDotProduct,
			scoreValue:  "none",
		},
		{
			numSegments: 3,
			queryIndex:  0,
			mapping:     indexMappingCosine,
			scoreValue:  "none",
		},
		{
			numSegments: 7,
			queryIndex:  1,
			mapping:     indexMappingCosine,
			scoreValue:  "none",
		},
		{
			numSegments: 8,
			queryIndex:  2,
			mapping:     indexMappingCosine,
			scoreValue:  "none",
		},
	}
	for testCaseNum, testCase := range testCases {
		originalRequest := searchRequests[testCase.queryIndex]
		for _, operator := range knnOperators {
			// run single segment test first
			tmpIndexPath := createTmpIndexPath(t)
			index, err := New(tmpIndexPath, testCase.mapping)
			if err != nil {
				t.Fatal(err)
			}
			query := copySearchRequest(originalRequest, nil)
			query.Sort = reqSort.Copy()
			query.AddKNNOperator(operator)
			query.Explain = true

			nameToIndex := make(map[string]Index)
			nameToIndex[index.Name()] = index

			err = createMultipleSegmentsIndex(documents, index, 1)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}
			controlResult, err := index.Search(query)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}
			if !finalHitsHaveValidIndex(controlResult.Hits, nameToIndex) {
				cleanUp(t, nameToIndex)
				t.Fatalf("test case #%d failed: expected control result hits to have valid `Index`", testCaseNum)
			}
			if testCase.scoreValue == "none" {

				query := copySearchRequest(originalRequest, nil)
				query.Sort = reqSort.Copy()
				query.AddKNNOperator(operator)
				query.Explain = true
				query.Score = testCase.scoreValue

				expectedResultScoreNone, err := index.Search(query)
				if err != nil {
					cleanUp(t, nameToIndex)
					t.Fatal(err)
				}
				if !finalHitsHaveValidIndex(expectedResultScoreNone.Hits, nameToIndex) {
					cleanUp(t, nameToIndex)
					t.Fatalf("test case #%d failed: expected score none hits to have valid `Index`", testCaseNum)
				}
				verifyResult(t, controlResult, expectedResultScoreNone, testCaseNum, true)
			}
			cleanUp(t, nameToIndex)

			// run multiple segments test
			tmpIndexPath = createTmpIndexPath(t)
			index, err = New(tmpIndexPath, testCase.mapping)
			if err != nil {
				t.Fatal(err)
			}
			nameToIndex = make(map[string]Index)
			nameToIndex[index.Name()] = index
			err = createMultipleSegmentsIndex(documents, index, testCase.numSegments)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}

			query = copySearchRequest(originalRequest, nil)
			query.Sort = reqSort.Copy()
			query.AddKNNOperator(operator)
			query.Explain = true

			experimentalResult, err := index.Search(query)
			if err != nil {
				cleanUp(t, nameToIndex)
				t.Fatal(err)
			}
			if !finalHitsHaveValidIndex(experimentalResult.Hits, nameToIndex) {
				cleanUp(t, nameToIndex)
				t.Fatalf("test case #%d failed: expected experimental result hits to have valid `Index`", testCaseNum)
			}
			verifyResult(t, controlResult, experimentalResult, testCaseNum, false)
			cleanUp(t, nameToIndex)
		}
	}
}

// Test to determine the impact of boost on kNN queries.
func TestKNNScoreBoosting(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	const dims = 5
	getRandomVector := func() []float32 {
		vec := make([]float32, dims)
		for i := 0; i < dims; i++ {
			vec[i] = rand.Float32()
		}
		return vec
	}

	dataset := make([]map[string]interface{}, 10)

	// Indexing just a few docs to populate index.
	for i := 0; i < 100; i++ {
		dataset = append(dataset, map[string]interface{}{
			"type":    "vectorStuff",
			"content": strconv.Itoa(i),
			"vector":  getRandomVector(),
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
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	batch := index.NewBatch()
	for i := 0; i < len(dataset); i++ {
		batch.Index(strconv.Itoa(i), dataset[i])
	}

	err = index.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	queryVec := getRandomVector()
	searchRequest := NewSearchRequest(NewMatchNoneQuery())
	searchRequest.AddKNN("vector", queryVec, 3, 1.0)
	searchRequest.Fields = []string{"content", "vector"}

	hits, _ := index.Search(searchRequest)
	hitsMap := make(map[string]float64, 0)
	for _, hit := range hits.Hits {
		hitsMap[hit.ID] = (hit.Score)
	}

	searchRequest2 := NewSearchRequest(NewMatchNoneQuery())
	searchRequest.AddKNN("vector", queryVec, 3, 10.0)
	searchRequest.Fields = []string{"content", "vector"}

	hits2, _ := index.Search(searchRequest2)
	hitsMap2 := make(map[string]float64, 0)
	for _, hit := range hits2.Hits {
		hitsMap2[hit.ID] = (hit.Score)
	}

	for _, hit := range hits2.Hits {
		if hitsMap[hit.ID] != hitsMap2[hit.ID]/10 {
			t.Errorf("boosting not working: %v %v \n", hitsMap[hit.ID], hitsMap2[hit.ID])
		}
	}
}

// Test to see if KNN Operators get added right to the query.
func TestKNNOperator(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	const dims = 5
	getRandomVector := func() []float32 {
		vec := make([]float32, dims)
		for i := 0; i < dims; i++ {
			vec[i] = rand.Float32()
		}
		return vec
	}

	dataset := make([]map[string]interface{}, 10)

	// Indexing just a few docs to populate index.
	for i := 0; i < 10; i++ {
		dataset = append(dataset, map[string]interface{}{
			"type":    "vectorStuff",
			"content": strconv.Itoa(i),
			"vector":  getRandomVector(),
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
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	batch := index.NewBatch()
	for i := 0; i < len(dataset); i++ {
		batch.Index(strconv.Itoa(i), dataset[i])
	}

	err = index.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	termQuery := query.NewTermQuery("2")

	searchRequest := NewSearchRequest(termQuery)
	searchRequest.AddKNN("vector", getRandomVector(), 3, 2.0)
	searchRequest.AddKNN("vector", getRandomVector(), 2, 1.5)
	searchRequest.Fields = []string{"content", "vector"}

	// Conjunction
	searchRequest.AddKNNOperator(knnOperatorAnd)
	requiresFiltering := make(map[int]bool)
	conjunction, _, _, err := createKNNQuery(searchRequest, nil, requiresFiltering)
	if err != nil {
		t.Fatalf("unexpected error for AND knn operator")
	}

	conj, ok := conjunction.(*query.DisjunctionQuery)
	if !ok {
		t.Fatalf("expected disjunction query")
	}

	if len(conj.Disjuncts) != 2 {
		t.Fatalf("expected 2 disjuncts")
	}

	// Disjunction
	searchRequest.AddKNNOperator(knnOperatorOr)
	disjunction, _, _, err := createKNNQuery(searchRequest, nil, requiresFiltering)
	if err != nil {
		t.Fatalf("unexpected error for OR knn operator")
	}

	disj, ok := disjunction.(*query.DisjunctionQuery)
	if !ok {
		t.Fatalf("expected disjunction query")
	}

	if len(disj.Disjuncts) != 2 {
		t.Fatalf("expected 2 disjuncts")
	}

	// Incorrect operator.
	searchRequest.AddKNNOperator("bs_op")
	searchRequest.Query, _, _, err = createKNNQuery(searchRequest, nil, requiresFiltering)
	if err == nil {
		t.Fatalf("expected error for incorrect knn operator")
	}
}

func TestKNNFiltering(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	const dims = 5
	getRandomVector := func() []float32 {
		vec := make([]float32, dims)
		for i := 0; i < dims; i++ {
			vec[i] = rand.Float32()
		}
		return vec
	}

	dataset := make([]map[string]interface{}, 0)

	// Indexing just a few docs to populate index.
	for i := 0; i < 10; i++ {
		dataset = append(dataset, map[string]interface{}{
			"type":    "vectorStuff",
			"content": strconv.Itoa(i + 1000),
			"vector":  getRandomVector(),
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
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	batch := index.NewBatch()
	for i := 0; i < len(dataset); i++ {
		// the id of term "i" is (i-1000)
		batch.Index(strconv.Itoa(i), dataset[i])
	}

	err = index.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	termQuery := query.NewTermQuery("1004")
	filterRequest := NewSearchRequest(termQuery)
	filteredHits, err := index.Search(filterRequest)
	if err != nil {
		t.Fatal(err)
	}
	filteredDocIDs := make(map[string]struct{})
	for _, match := range filteredHits.Hits {
		filteredDocIDs[match.ID] = struct{}{}
	}

	searchRequest := NewSearchRequest(NewMatchNoneQuery())
	searchRequest.AddKNNWithFilter("vector", getRandomVector(), 3, 2.0, termQuery)
	searchRequest.Fields = []string{"content", "vector"}

	res, err := index.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}
	// check if any of the returned results are not part of the filtered hits.
	for _, match := range res.Hits {
		if _, exists := filteredDocIDs[match.ID]; !exists {
			t.Errorf("returned result not present in filtered hits")
		}
	}

	// No results should be returned with a match_none filter.
	searchRequest = NewSearchRequest(NewMatchNoneQuery())
	searchRequest.AddKNNWithFilter("vector", getRandomVector(), 3, 2.0,
		NewMatchNoneQuery())
	res, err = index.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hits) != 0 {
		t.Errorf("match none filter should return no hits")
	}

	// Testing with a disjunction query.

	termQuery = query.NewTermQuery("1003")
	termQuery2 := query.NewTermQuery("1005")
	disjQuery := query.NewDisjunctionQuery([]query.Query{termQuery, termQuery2})
	filterRequest = NewSearchRequest(disjQuery)
	filteredHits, err = index.Search(filterRequest)
	if err != nil {
		t.Fatal(err)
	}
	filteredDocIDs = make(map[string]struct{})
	for _, match := range filteredHits.Hits {
		filteredDocIDs[match.ID] = struct{}{}
	}

	searchRequest = NewSearchRequest(NewMatchNoneQuery())
	searchRequest.AddKNNWithFilter("vector", getRandomVector(), 3, 2.0, disjQuery)
	searchRequest.Fields = []string{"content", "vector"}

	res, err = index.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	for _, match := range res.Hits {
		if _, exists := filteredDocIDs[match.ID]; !exists {
			t.Errorf("returned result not present in filtered hits")
		}
	}
}

// -----------------------------------------------------------------------------
// Test nested vectors

func TestNestedVectors(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	const dims = 3
	const k = 1 // one nearest neighbor
	const vecFieldName = "vecData"

	dataset := map[string]map[string]interface{}{ // docID -> Doc
		"doc1": {
			vecFieldName: []float32{100, 100, 100},
		},
		"doc2": {
			vecFieldName: [][]float32{{0, 0, 0}, {1000, 1000, 1000}},
		},
	}

	// Index mapping
	indexMapping := NewIndexMapping()
	vm := mapping.NewVectorFieldMapping()
	vm.Dims = dims
	vm.Similarity = "l2_norm"
	indexMapping.DefaultMapping.AddFieldMappingsAt(vecFieldName, vm)

	// Create index and upload documents
	index, err := New(tmpIndexPath, indexMapping)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	batch := index.NewBatch()
	for docID, doc := range dataset {
		batch.Index(docID, doc)
	}

	err = index.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	// Run searches

	tests := []struct {
		queryVec      []float32
		expectedDocID string
	}{
		{
			queryVec:      []float32{100, 100, 100},
			expectedDocID: "doc1",
		},
		{
			queryVec:      []float32{0, 0, 0},
			expectedDocID: "doc2",
		},
		{
			queryVec:      []float32{1000, 1000, 1000},
			expectedDocID: "doc2",
		},
	}

	for _, test := range tests {
		searchReq := NewSearchRequest(query.NewMatchNoneQuery())
		searchReq.AddKNNWithFilter(vecFieldName, test.queryVec, k, 1000,
			NewMatchAllQuery())

		res, err := index.Search(searchReq)
		if err != nil {
			t.Fatal(err)
		}

		if len(res.Hits) != 1 {
			t.Fatalf("expected 1 hit, got %d", len(res.Hits))
		}

		if res.Hits[0].ID != test.expectedDocID {
			t.Fatalf("expected docID %s, got %s", test.expectedDocID,
				res.Hits[0].ID)
		}
	}
}

func TestNumVecsStat(t *testing.T) {

	dataset, _, err := readDatasetAndQueries(testInputCompressedFile)
	if err != nil {
		t.Fatal(err)
	}
	documents := makeDatasetIntoDocuments(dataset)

	indexMapping := NewIndexMapping()

	contentFieldMapping := NewTextFieldMapping()
	contentFieldMapping.Analyzer = en.AnalyzerName
	indexMapping.DefaultMapping.AddFieldMappingsAt("content", contentFieldMapping)

	vecFieldMapping1 := mapping.NewVectorFieldMapping()
	vecFieldMapping1.Dims = testDatasetDims
	vecFieldMapping1.Similarity = index.EuclideanDistance
	indexMapping.DefaultMapping.AddFieldMappingsAt("vector", vecFieldMapping1)

	tmpIndexPath := createTmpIndexPath(t)
	index, err := New(tmpIndexPath, indexMapping)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	for i := 0; i < 10; i++ {
		batch := index.NewBatch()
		for j := 0; j < 3; j++ {
			for k := 0; k < 10; k++ {
				err := batch.Index(fmt.Sprintf("%d", i*30+j*10+k), documents[j*10+k])
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		err = index.Batch(batch)
		if err != nil {
			t.Fatal(err)
		}
	}

	statsMap := index.StatsMap()

	if indexStats, exists := statsMap["index"]; exists {
		if indexStatsMap, ok := indexStats.(map[string]interface{}); ok {
			v1, ok := indexStatsMap["field:vector:num_vectors"].(uint64)
			if !ok || v1 != uint64(300) {
				t.Fatalf("mismatch in the number of vectors, expected 300, got %d", indexStatsMap["field:vector:num_vectors"])
			}
		}
	}
}
