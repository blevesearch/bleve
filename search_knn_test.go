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
	"encoding/json"
	"math/rand"
	"strconv"
	"testing"

	"github.com/blevesearch/bleve/v2/analysis/lang/en"
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
	vecFieldMappingDot.Similarity = index.CosineSimilarity

	indexMappingDotProduct := NewIndexMapping()
	indexMappingDotProduct.DefaultMapping.AddFieldMappingsAt("content", contentFieldMapping)
	indexMappingDotProduct.DefaultMapping.AddFieldMappingsAt("vector", vecFieldMappingDot)

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
	}

	index := NewIndexAlias()
	for testCaseNum, testCase := range testCases {
		for _, operator := range knnOperators {
			index.indexes = make([]Index, 0)
			query := searchRequests[testCase.queryIndex]
			query.AddKNNOperator(operator)

			indexPaths := createPartitionedIndex(documents, index, 1, testCase.mapping, t)
			controlResult, err := index.Search(query)
			if err != nil {
				t.Fatal(err)
			}
			cleanUp(t, indexPaths, index.indexes...)

			index.indexes = make([]Index, 0)
			indexPaths = createPartitionedIndex(documents, index, testCase.numIndexPartitions, testCase.mapping, t)
			experimentalResult, err := index.Search(query)
			if err != nil {
				t.Fatal(err)
			}
			verifyResult(t, controlResult, experimentalResult, testCaseNum, true)
			cleanUp(t, indexPaths, index.indexes...)
		}
	}
}

type testDocument struct {
	ID      string    `json:"id"`
	Content string    `json:"content"`
	Vector  []float64 `json:"vector"`
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

func cleanUp(t *testing.T, indexPaths []string, indexes ...Index) {
	for _, childIndex := range indexes {
		err := childIndex.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, indexPath := range indexPaths {
		cleanupTmpIndexPath(t, indexPath)
	}
}

func createPartitionedIndex(documents []map[string]interface{}, index *indexAliasImpl, numPartitions int,
	mapping mapping.IndexMapping, t *testing.T) []string {

	partitionSize := len(documents) / numPartitions
	extraDocs := len(documents) % numPartitions
	docsPerPartition := make([]int, numPartitions)
	for i := 0; i < numPartitions; i++ {
		docsPerPartition[i] = partitionSize
		if extraDocs > 0 {
			docsPerPartition[i]++
			extraDocs--
		}
	}
	var rv []string
	prevCutoff := 0
	for i := 0; i < numPartitions; i++ {
		tmpIndexPath := createTmpIndexPath(t)
		rv = append(rv, tmpIndexPath)
		childIndex, err := New(tmpIndexPath, mapping)
		if err != nil {
			cleanUp(t, rv)
			t.Fatal(err)
		}
		batch := childIndex.NewBatch()
		for j := prevCutoff; j < prevCutoff+docsPerPartition[i]; j++ {
			doc := documents[j]
			err := batch.Index(doc["id"].(string), doc)
			if err != nil {
				cleanUp(t, rv)
				t.Fatal(err)
			}
		}
		prevCutoff += docsPerPartition[i]
		err = childIndex.Batch(batch)
		if err != nil {
			cleanUp(t, rv)
			t.Fatal(err)
		}
		index.Add(childIndex)
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
	for _, batch := range batches {
		err = index.Batch(batch)
		if err != nil {
			return err
		}
	}
	return nil
}

func truncateScore(score float64) float64 {
	return float64(int(score*1e6)) / 1e6
}

func verifyResult(t *testing.T, controlResult *SearchResult, experimentalResult *SearchResult, testCaseNum int, verifyOnlyDocIDs bool) {
	if len(controlResult.Hits) != len(experimentalResult.Hits) {
		t.Fatalf("testcase %d failed: expected %d results, got %d", testCaseNum, len(controlResult.Hits), len(experimentalResult.Hits))
	}
	if controlResult.Total != experimentalResult.Total {
		t.Errorf("test case #%d: expected total hits to be %d, got %d", testCaseNum, controlResult.Total, experimentalResult.Total)
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
			t.Fatalf("testcase %d failed: expected %d results, got %d", testCaseNum, len(controlMap), len(experimentalMap))
		}
		for id := range controlMap {
			if _, ok := experimentalMap[id]; !ok {
				t.Fatalf("testcase %d failed: expected id %s to be in experimental result", testCaseNum, id)
			}
		}
		return
	}

	for i := 0; i < len(controlResult.Hits); i++ {
		if controlResult.Hits[i].ID != experimentalResult.Hits[i].ID {
			t.Fatalf("testcase %d failed: expected hit %d to have id %s, got %s", testCaseNum, i, controlResult.Hits[i].ID, experimentalResult.Hits[i].ID)
		}
		// Truncate to 6 decimal places
		actualScore := truncateScore(experimentalResult.Hits[i].Score)
		expectScore := truncateScore(controlResult.Hits[i].Score)
		if expectScore != actualScore {
			t.Fatalf("testcase %d failed: expected hit %d to have score %f, got %f", testCaseNum, i, expectScore, actualScore)
		}
	}
	if truncateScore(controlResult.MaxScore) != truncateScore(experimentalResult.MaxScore) {
		t.Errorf("test case #%d: expected maxScore to be %f, got %f", testCaseNum, controlResult.MaxScore, experimentalResult.MaxScore)
	}

}
func TestSimilaritySearchMultipleSegments(t *testing.T) {
	// to run this test you must first add the line
	// 				return nil
	// in the scorch.go file just before these two lines
	// 				s.asyncTasks.Add(1)
	//				go s.mergerLoop()
	// this is to prevent the merger from running and merging the segments
	// before we can test the search on multiple segments
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
	vecFieldMappingDot.Similarity = index.CosineSimilarity

	indexMappingL2Norm := NewIndexMapping()
	indexMappingL2Norm.DefaultMapping.AddFieldMappingsAt("content", contentFieldMapping)
	indexMappingL2Norm.DefaultMapping.AddFieldMappingsAt("vector", vecFieldMappingL2)

	indexMappingDotProduct := NewIndexMapping()
	indexMappingDotProduct.DefaultMapping.AddFieldMappingsAt("content", contentFieldMapping)
	indexMappingDotProduct.DefaultMapping.AddFieldMappingsAt("vector", vecFieldMappingL2)

	testCases := []struct {
		numSegments int
		queryIndex  int
		mapping     mapping.IndexMapping
		scoreValue  string
	}{
		// // L2 norm similarity
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
	}
	for testCaseNum, testCase := range testCases {
		for _, operator := range knnOperators {
			// run single segment test first
			tmpIndexPath := createTmpIndexPath(t)
			index, err := New(tmpIndexPath, testCase.mapping)
			if err != nil {
				t.Fatal(err)
			}
			query := searchRequests[testCase.queryIndex]
			query.Sort = search.SortOrder{&search.SortScore{Desc: true}, &search.SortDocID{Desc: true}}
			query.AddKNNOperator(operator)
			err = createMultipleSegmentsIndex(documents, index, 1)
			if err != nil {
				t.Fatal(err)
			}
			controlResult, err := index.Search(query)
			if err != nil {
				t.Fatal(err)
			}
			if testCase.scoreValue == "none" {
				query.Score = testCase.scoreValue
				expectedResultScoreNone, err := index.Search(query)
				if err != nil {
					t.Fatal(err)
				}
				verifyResult(t, controlResult, expectedResultScoreNone, testCaseNum, true)
				query.Score = ""
			}
			err = index.Close()
			if err != nil {
				t.Fatal(err)
			}
			cleanupTmpIndexPath(t, tmpIndexPath)

			// run multiple segments test
			tmpIndexPath = createTmpIndexPath(t)
			index, err = New(tmpIndexPath, testCase.mapping)
			if err != nil {
				t.Fatal(err)
			}
			query = searchRequests[testCase.queryIndex]
			query.AddKNNOperator(operator)
			err = createMultipleSegmentsIndex(documents, index, testCase.numSegments)
			if err != nil {
				t.Fatal(err)
			}
			experimentalResult, err := index.Search(query)
			if err != nil {
				t.Fatal(err)
			}
			verifyResult(t, controlResult, experimentalResult, testCaseNum, false)
			err = index.Close()
			if err != nil {
				t.Fatal(err)
			}
			cleanupTmpIndexPath(t, tmpIndexPath)
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
	conjunction, _, _, err := createKNNQuery(searchRequest)
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
	disjunction, _, _, err := createKNNQuery(searchRequest)
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
	searchRequest.Query, _, _, err = createKNNQuery(searchRequest)
	if err == nil {
		t.Fatalf("expected error for incorrect knn operator")
	}
}
