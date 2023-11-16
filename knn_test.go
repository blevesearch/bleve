//	Copyright (c) 2023 Couchbase, Inc.
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
	"encoding/json"
	"math/rand"
	"os"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
)

const testDatasetFileName = "test/tests/knn/dataset-30-docs.json"
const testQueryFileName = "test/tests/knn/small-query.json"

const testDatasetDims = 384

const randomizeDocuments = false

type testDocument struct {
	ID      string    `json:"id"`
	Content string    `json:"content"`
	Vector  []float64 `json:"vector"`
}

func createVectorDataset(datasetFileName string) ([]testDocument, error) {
	var dataset []testDocument
	datasetFileData, err := os.ReadFile(datasetFileName)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(datasetFileData, &dataset)
	if err != nil {
		return nil, err
	}
	return dataset, nil
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

func getSearchRequests(queryFileName string) ([]*SearchRequest, error) {
	var reqArr []*SearchRequest
	queryFileData, err := os.ReadFile(queryFileName)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(queryFileData, &reqArr)
	if err != nil {
		return nil, err
	}
	return reqArr, nil
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
	var rv []string
	for i := 0; i < numPartitions; i++ {
		tmpIndexPath := createTmpIndexPath(t)
		rv = append(rv, tmpIndexPath)
		childIndex, err := New(tmpIndexPath, mapping)
		if err != nil {
			cleanUp(t, rv)
			t.Fatal(err)
		}
		batch := childIndex.NewBatch()
		for j := i * partitionSize; (j < (i+1)*partitionSize) && j < len(documents); j++ {
			doc := documents[j]
			err := batch.Index(doc["id"].(string), doc)
			if err != nil {
				cleanUp(t, rv)
				t.Fatal(err)
			}
		}
		err = childIndex.Batch(batch)
		if err != nil {
			cleanUp(t, rv)
			t.Fatal(err)
		}
		index.Add(childIndex)
	}
	return rv
}

// Fisher-Yates shuffle
func shuffleDocuments(documents []map[string]interface{}) []map[string]interface{} {
	for i := range documents {
		j := i + rand.Intn(len(documents)-i)
		documents[i], documents[j] = documents[j], documents[i]
	}
	return documents
}

func truncateScore(score float64) float64 {
	return float64(int(score*1e6)) / 1e6
}

func TestSimilaritySearchQuery(t *testing.T) {

	dataset, err := createVectorDataset(testDatasetFileName)
	if err != nil {
		t.Fatal(err)
	}
	documents := makeDatasetIntoDocuments(dataset)
	if randomizeDocuments {
		documents = shuffleDocuments(documents)
	}
	searchRequests, err := getSearchRequests(testQueryFileName)
	if err != nil {
		t.Fatal(err)
	}

	indexMapping := NewIndexMapping()
	contentFieldMapping := NewTextFieldMapping()
	contentFieldMapping.Analyzer = "en"

	vecFieldMapping := mapping.NewVectorFieldMapping()
	vecFieldMapping.Dims = testDatasetDims
	vecFieldMapping.Similarity = "l2_norm"

	indexMapping.DefaultMapping.AddFieldMappingsAt("content", contentFieldMapping)
	indexMapping.DefaultMapping.AddFieldMappingsAt("vector", vecFieldMapping)

	index := NewIndexAlias()
	index.SetPartitionedMode(true)

	type testResult struct {
		score          float64
		scoreBreakdown []float64
	}

	type testCase struct {
		queryIndex         int
		numIndexPartitions int
		expectedResults    map[string]testResult
	}

	testCases := []testCase{
		{
			queryIndex:         0,
			numIndexPartitions: 1,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          0.5547758085810349,
					scoreBreakdown: []float64{0, 1.1095516171620698},
				},
				"doc23": {
					score:          0.3817633037007331,
					scoreBreakdown: []float64{0, 0.7635266074014662},
				},
				"doc28": {
					score:          0.33983667469689355,
					scoreBreakdown: []float64{0, 0.6796733493937871},
				},
			},
		},
		{
			queryIndex:         0,
			numIndexPartitions: 4,
			expectedResults: map[string]testResult{
				"doc23": {
					score:          0.3817633037007331,
					scoreBreakdown: []float64{0, 0.7635266074014662},
				},
				"doc28": {
					score:          0.33983667469689355,
					scoreBreakdown: []float64{0, 0.6796733493937871},
				},
				"doc13": {
					score:          0.3206958457835452,
					scoreBreakdown: []float64{0, 0.6413916915670904},
				},
			},
		},
		{
			queryIndex:         0,
			numIndexPartitions: 10,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          0.554775,
					scoreBreakdown: []float64{0, 1.109551},
				},
				"doc23": {
					score:          0.381763,
					scoreBreakdown: []float64{0, 0.763526},
				},
				"doc28": {
					score:          0.339836,
					scoreBreakdown: []float64{0, 0.679673},
				},
			},
		},
		{
			queryIndex:         1,
			numIndexPartitions: 1,
			expectedResults: map[string]testResult{
				"doc23": {
					score:          1.304929,
					scoreBreakdown: []float64{1.016928, 0.288001},
				},
				"doc29": {
					score:          1.137598,
					scoreBreakdown: []float64{0.719076, 0.418521},
				},
				"doc27": {
					score:          0.429730,
					scoreBreakdown: []float64{0.859461, 0},
				},
				"doc28": {
					score:          0.401976,
					scoreBreakdown: []float64{0.803952, 0},
				},
				"doc30": {
					score:          0.359538,
					scoreBreakdown: []float64{0.719076, 0},
				},
				"doc24": {
					score:          0.359538,
					scoreBreakdown: []float64{0.719076, 0},
				},
			},
		},
		{
			queryIndex:         1,
			numIndexPartitions: 5,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          1.0019961733597083,
					scoreBreakdown: []float64{0.28546778431249004, 0.7165283890472183},
				},
				"doc28": {
					score:          0.758083452201489,
					scoreBreakdown: []float64{0.3191626822375969, 0.4389207699638921},
				},
				"doc23": {
					score:          0.32598793043274804,
					scoreBreakdown: []float64{0.6519758608654961, 0},
				},
				"doc24": {
					score:          0.2305082774045959,
					scoreBreakdown: []float64{0.4610165548091918, 0},
				},
				"doc27": {
					score:          0.17059962977552257,
					scoreBreakdown: []float64{0.34119925955104513, 0},
				},
				"doc30": {
					score:          0.14273389215624502,
					scoreBreakdown: []float64{0.28546778431249004, 0},
				},
			},
		},
		{
			queryIndex:         2,
			numIndexPartitions: 1,
			expectedResults: map[string]testResult{
				"doc7": {
					score:          3333.333333333333,
					scoreBreakdown: []float64{0, 0, 10000},
				},
				"doc23": {
					score:          0.3234943403006508,
					scoreBreakdown: []float64{0.30977843187823456, 0.1754630785727416, 0},
				},
				"doc29": {
					score:          0.31601878039486375,
					scoreBreakdown: []float64{0.21904643099686824, 0.2549817395954274, 0},
				},
				"doc3": {
					score:          0.24118912169729392,
					scoreBreakdown: []float64{0.7235673650918818, 0, 0},
				},
				"doc13": {
					score:          0.20887591025526625,
					scoreBreakdown: []float64{0.6266277307657988, 0, 0},
				},
				"doc27": {
					score:          0.08727018618864227,
					scoreBreakdown: []float64{0.26181055856592683, 0, 0},
				},
				"doc28": {
					score:          0.0816337841412418,
					scoreBreakdown: []float64{0.2449013524237254, 0, 0},
				},
				"doc24": {
					score:          0.07301547699895608,
					scoreBreakdown: []float64{0.21904643099686824, 0, 0},
				},
				"doc30": {
					score:          0.07301547699895608,
					scoreBreakdown: []float64{0.21904643099686824, 0, 0},
				},
				"doc5": {
					score:          0.06883694922797147,
					scoreBreakdown: []float64{0, 0, 0.20651084768391442},
				},
			},
		},
		{
			queryIndex:         2,
			numIndexPartitions: 4,
			expectedResults: map[string]testResult{
				"doc7": {
					score:          3333.333333333333,
					scoreBreakdown: []float64{0, 0, 10000},
				},
				"doc23": {
					score:          0.2195946458144798,
					scoreBreakdown: []float64{0.11312710434186327, 0.21626486437985648, 0},
				},
				"doc28": {
					score:          0.18796580117926376,
					scoreBreakdown: []float64{0.08943482824521586, 0.1925138735236798, 0},
				},
				"doc3": {
					score:          0.12303621410516037,
					scoreBreakdown: []float64{0.36910864231548113, 0, 0},
				},
				"doc13": {
					score:          0.10655248891295889,
					scoreBreakdown: []float64{0.3196574667388767, 0, 0},
				},
				"doc5": {
					score:          0.07546992065969621,
					scoreBreakdown: []float64{0, 0, 0.22640976197908863},
				},
				"doc27": {
					score:          0.03186995104545246,
					scoreBreakdown: []float64{0.09560985313635739, 0, 0},
				},
				"doc24": {
					score:          0.02666431434541773,
					scoreBreakdown: []float64{0.07999294303625319, 0, 0},
				},
			},
		},
	}

	for testCaseNum, testCase := range testCases {
		index.indexes = make([]Index, 0)
		indexPaths := createPartitionedIndex(documents, index, testCase.numIndexPartitions, indexMapping, t)
		query := searchRequests[testCase.queryIndex]
		res, err := index.Search(query)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Hits) != len(testCase.expectedResults) {
			t.Fatalf("testcase %d failed: expected %d results, got %d", testCaseNum, len(testCase.expectedResults), len(res.Hits))
		}
		for i, hit := range res.Hits {
			var expectedHit testResult
			var ok bool
			if expectedHit, ok = testCase.expectedResults[hit.ID]; !ok {
				t.Fatalf("testcase %d failed: unexpected result %s", testCaseNum, hit.ID)
			}
			// Truncate to 6 decimal places
			actualScore := truncateScore(hit.Score)
			expectScore := truncateScore(expectedHit.score)
			if !randomizeDocuments && expectScore != actualScore {
				t.Fatalf("testcase %d failed: expected hit %d to have score %f, got %f", testCaseNum, i, expectedHit.score, hit.Score)
			}
			if len(hit.ScoreBreakdown) != len(expectedHit.scoreBreakdown) {
				t.Fatalf("testcase %d failed: expected hit %d to have %d score breakdowns, got %d", testCaseNum, i, len(expectedHit.scoreBreakdown), len(hit.ScoreBreakdown))
			}
			if !randomizeDocuments {
				actualScore := truncateScore(hit.ScoreBreakdown[0])
				expectScore := truncateScore(expectedHit.scoreBreakdown[0])
				if expectScore != actualScore {
					t.Fatalf("testcase %d failed: expected hit %d to have score breakdown %f, got %f", testCaseNum, i, expectedHit.scoreBreakdown[0], hit.ScoreBreakdown[0])
				}
			}
			for j := 1; j < len(hit.ScoreBreakdown); j++ {
				// Truncate to 6 decimal places
				actualScore := truncateScore(hit.ScoreBreakdown[j])
				expectScore := truncateScore(expectedHit.scoreBreakdown[j])
				if expectScore != actualScore {
					t.Fatalf("testcase %d failed: expected hit %d to have score breakdown %f, got %f", testCaseNum, i, expectedHit.scoreBreakdown[j], hit.ScoreBreakdown[j])
				}
			}
		}
		cleanUp(t, indexPaths, index.indexes...)
	}
}
