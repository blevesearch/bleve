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
	"archive/zip"
	"encoding/json"
	"math"
	"math/rand"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
)

const testInputCompressedFile = "test/knn/knn_dataset_queries.zip"
const testDatasetFileName = "knn_dataset.json"
const testQueryFileName = "knn_queries.json"

const testDatasetDims = 384

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

func TestSimilaritySearchRandomized(t *testing.T) {
	runKNNTest(t, true)
}

func TestSimilaritySearchNotRandomized(t *testing.T) {
	runKNNTest(t, false)
}

func runKNNTest(t *testing.T, randomizeDocuments bool) {
	dataset, searchRequests, err := readDatasetAndQueries(testInputCompressedFile)
	if err != nil {
		t.Fatal(err)
	}
	documents := makeDatasetIntoDocuments(dataset)
	if randomizeDocuments {
		documents = shuffleDocuments(documents)
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

	type testResult struct {
		score          float64
		scoreBreakdown []float64
	}

	type testCase struct {
		testType           string
		queryIndex         int
		numIndexPartitions int
		expectedResults    map[string]testResult
	}

	testCases := []testCase{
		{
			testType:           "single_partition:match_none:oneKNNreq:k=3",
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
			testType:           "multi_partition:match_none:oneKNNreq:k=3",
			queryIndex:         0,
			numIndexPartitions: 4,
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
			testType:           "multi_partition:match_none:oneKNNreq:k=2",
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
			testType:           "single_partition:match:oneKNNreq:k=2",
			queryIndex:         1,
			numIndexPartitions: 1,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          1.8859816084399936,
					scoreBreakdown: []float64{0.7764299912779237, 1.1095516171620698},
				},
				"doc23": {
					score:          1.8615644255330264,
					scoreBreakdown: []float64{1.0980378181315602, 0.7635266074014662},
				},
				"doc27": {
					score:          0.4640056648691007,
					scoreBreakdown: []float64{0.9280113297382014, 0},
				},
				"doc28": {
					score:          0.434037555556026,
					scoreBreakdown: []float64{0.868075111112052, 0},
				},
				"doc30": {
					score:          0.38821499563896184,
					scoreBreakdown: []float64{0.7764299912779237, 0},
				},
				"doc24": {
					score:          0.38821499563896184,
					scoreBreakdown: []float64{0.7764299912779237, 0},
				},
			},
		},
		{
			testType:           "multi_partition:match:oneKNNreq:k=2",
			queryIndex:         1,
			numIndexPartitions: 5,
			expectedResults: map[string]testResult{
				"doc23": {
					score:          1.5207250366637521,
					scoreBreakdown: []float64{0.7571984292622859, 0.7635266074014662},
				},
				"doc29": {
					score:          1.4834345192674083,
					scoreBreakdown: []float64{0.3738829021053385, 1.1095516171620698},
				},
				"doc24": {
					score:          0.2677100734235977,
					scoreBreakdown: []float64{0.5354201468471954, 0},
				},
				"doc27": {
					score:          0.22343776840593196,
					scoreBreakdown: []float64{0.4468755368118639, 0},
				},
				"doc28": {
					score:          0.20900689401100958,
					scoreBreakdown: []float64{0.41801378802201916, 0},
				},
				"doc30": {
					score:          0.18694145105266924,
					scoreBreakdown: []float64{0.3738829021053385, 0},
				},
			},
		},
		{
			testType:           "single_partition:disjunction:twoKNNreq:k=2,2",
			queryIndex:         2,
			numIndexPartitions: 1,
			expectedResults: map[string]testResult{
				"doc7": {
					score:          math.MaxFloat64,
					scoreBreakdown: []float64{0, 0, math.MaxFloat64 / 3.0},
				},
				"doc29": {
					score:          0.6774608026082964,
					scoreBreakdown: []float64{0.23161973134064517, 0.7845714725717996, 0},
				},
				"doc23": {
					score:          0.5783030702431613,
					scoreBreakdown: []float64{0.32755976365480655, 0.5398948417099355, 0},
				},
				"doc3": {
					score:          0.2550334160459894,
					scoreBreakdown: []float64{0.7651002481379682, 0, 0},
				},
				"doc13": {
					score:          0.2208654210738964,
					scoreBreakdown: []float64{0.6625962632216892, 0, 0},
				},
				"doc5": {
					score:          0.21180931116413285,
					scoreBreakdown: []float64{0, 0, 0.6354279334923986},
				},
				"doc27": {
					score:          0.09227950890170131,
					scoreBreakdown: []float64{0.27683852670510395, 0, 0},
				},
				"doc28": {
					score:          0.0863195764709126,
					scoreBreakdown: []float64{0.2589587294127378, 0, 0},
				},
				"doc30": {
					score:          0.07720657711354839,
					scoreBreakdown: []float64{0.23161973134064517, 0, 0},
				},
				"doc24": {
					score:          0.07720657711354839,
					scoreBreakdown: []float64{0.23161973134064517, 0, 0},
				},
			},
		},
		{
			testType:           "multi_partition:disjunction:twoKNNreq:k=2,2",
			queryIndex:         2,
			numIndexPartitions: 4,
			expectedResults: map[string]testResult{
				"doc7": {
					score:          math.MaxFloat64,
					scoreBreakdown: []float64{0, 0, math.MaxFloat64 / 3.0},
				},
				"doc29": {
					score:          0.567426591648309,
					scoreBreakdown: []float64{0.06656841490066398, 0.7845714725717996, 0},
				},
				"doc23": {
					score:          0.5639255136185979,
					scoreBreakdown: []float64{0.3059934287179615, 0.5398948417099355, 0},
				},
				"doc5": {
					score:          0.21180931116413285,
					scoreBreakdown: []float64{0, 0, 0.6354279334923986},
				},
				"doc3": {
					score:          0.14064944169372873,
					scoreBreakdown: []float64{0.42194832508118624, 0, 0},
				},
				"doc13": {
					score:          0.12180599172106943,
					scoreBreakdown: []float64{0.3654179751632083, 0, 0},
				},
				"doc27": {
					score:          0.026521491065731144,
					scoreBreakdown: []float64{0.07956447319719344, 0, 0},
				},
				"doc28": {
					score:          0.024808583220893122,
					scoreBreakdown: []float64{0.07442574966267937, 0, 0},
				},
				"doc30": {
					score:          0.02218947163355466,
					scoreBreakdown: []float64{0.06656841490066398, 0, 0},
				},
				"doc24": {
					score:          0.02218947163355466,
					scoreBreakdown: []float64{0.06656841490066398, 0, 0},
				},
			},
		},
		{
			// control:
			// from = 0
			// size = 8
			testType:           "pagination",
			queryIndex:         3,
			numIndexPartitions: 4,
			expectedResults: map[string]testResult{
				"doc24": {
					score:          1.22027994094805,
					scoreBreakdown: []float64{0.027736154383370196, 0.3471022633855392, 0.5085619451465123, 0.33687957803262836},
				},
				"doc17": {
					score:          0.7851856993753307,
					scoreBreakdown: []float64{0.3367753689069724, 0, 0.3892791754255179, 0.320859721501284},
				},
				"doc21": {
					score:          0.5927148028393034,
					scoreBreakdown: []float64{0.06974846263723515, 0, 0.3914133076090359, 0.3291246335394669},
				},
				"doc14": {
					score:          0.45680756875853035,
					scoreBreakdown: []float64{0.5968461853543279, 0, 0, 0.31676895216273276},
				},
				"doc25": {
					score:          0.292014972318407,
					scoreBreakdown: []float64{0.17861510907524708, 0, 0.405414835561567, 0},
				},
				"doc23": {
					score:          0.24706850662359503,
					scoreBreakdown: []float64{0.09761951136424651, 0, 0.39651750188294355, 0},
				},
				"doc15": {
					score:          0.24489276164017085,
					scoreBreakdown: []float64{0.17216818679645968, 0, 0, 0.317617336483882},
				},
				"doc5": {
					score:          0.10331722282971788,
					scoreBreakdown: []float64{0, 0.4132688913188715, 0, 0},
				},
			},
		},
		{
			// experimental:
			// from = 0
			// size = 3
			testType:           "pagination",
			queryIndex:         4,
			numIndexPartitions: 4,
			expectedResults: map[string]testResult{
				"doc24": {
					score:          1.22027994094805,
					scoreBreakdown: []float64{0.027736154383370196, 0.3471022633855392, 0.5085619451465123, 0.33687957803262836},
				},
				"doc17": {
					score:          0.7851856993753307,
					scoreBreakdown: []float64{0.3367753689069724, 0, 0.3892791754255179, 0.320859721501284},
				},
				"doc21": {
					score:          0.5927148028393034,
					scoreBreakdown: []float64{0.06974846263723515, 0, 0.3914133076090359, 0.3291246335394669},
				},
			},
		},
		{
			// from = 3
			// size = 3
			testType:           "pagination",
			queryIndex:         5,
			numIndexPartitions: 4,
			expectedResults: map[string]testResult{
				"doc14": {
					score:          0.45680756875853035,
					scoreBreakdown: []float64{0.5968461853543279, 0, 0, 0.31676895216273276},
				},
				"doc25": {
					score:          0.292014972318407,
					scoreBreakdown: []float64{0.17861510907524708, 0, 0.405414835561567, 0},
				},
				"doc23": {
					score:          0.24706850662359503,
					scoreBreakdown: []float64{0.09761951136424651, 0, 0.39651750188294355, 0},
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
		if randomizeDocuments && testCase.testType == "pagination" {
			// pagination is not deterministic when documents are randomized
			continue
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

func TestSimilaritySearchMultipleSegments(t *testing.T) {
	dataset, searchRequests, err := readDatasetAndQueries(testInputCompressedFile)
	if err != nil {
		t.Fatal(err)
	}
	documents := makeDatasetIntoDocuments(dataset)

	indexMapping := NewIndexMapping()
	contentFieldMapping := NewTextFieldMapping()
	contentFieldMapping.Analyzer = "en"

	vecFieldMapping := mapping.NewVectorFieldMapping()
	vecFieldMapping.Dims = testDatasetDims
	vecFieldMapping.Similarity = "l2_norm"

	indexMapping.DefaultMapping.AddFieldMappingsAt("content", contentFieldMapping)
	indexMapping.DefaultMapping.AddFieldMappingsAt("vector", vecFieldMapping)

	type testResult struct {
		score          float64
		scoreBreakdown []float64
	}

	testCases := []struct {
		numSegments     int
		queryIndex      int
		expectedResults map[string]testResult
	}{
		{
			numSegments: 1,
			queryIndex:  0,
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
			numSegments: 6,
			queryIndex:  0,
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
			numSegments: 1,
			queryIndex:  1,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          1.8859816084399936,
					scoreBreakdown: []float64{0.7764299912779237, 1.1095516171620698},
				},
				"doc23": {
					score:          1.8615644255330264,
					scoreBreakdown: []float64{1.0980378181315602, 0.7635266074014662},
				},
				"doc27": {
					score:          0.4640056648691007,
					scoreBreakdown: []float64{0.9280113297382014, 0},
				},
				"doc28": {
					score:          0.434037555556026,
					scoreBreakdown: []float64{0.868075111112052, 0},
				},
				"doc30": {
					score:          0.38821499563896184,
					scoreBreakdown: []float64{0.7764299912779237, 0},
				},
				"doc24": {
					score:          0.38821499563896184,
					scoreBreakdown: []float64{0.7764299912779237, 0},
				},
			},
		},
		{
			numSegments: 7,
			queryIndex:  1,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          1.8859816084399936,
					scoreBreakdown: []float64{0.7764299912779237, 1.1095516171620698},
				},
				"doc23": {
					score:          1.8615644255330264,
					scoreBreakdown: []float64{1.0980378181315602, 0.7635266074014662},
				},
				"doc27": {
					score:          0.4640056648691007,
					scoreBreakdown: []float64{0.9280113297382014, 0},
				},
				"doc28": {
					score:          0.434037555556026,
					scoreBreakdown: []float64{0.868075111112052, 0},
				},
				"doc30": {
					score:          0.38821499563896184,
					scoreBreakdown: []float64{0.7764299912779237, 0},
				},
				"doc24": {
					score:          0.38821499563896184,
					scoreBreakdown: []float64{0.7764299912779237, 0},
				},
			},
		},
		{
			numSegments: 1,
			queryIndex:  2,
			expectedResults: map[string]testResult{
				"doc7": {
					score:          2357.022603955158,
					scoreBreakdown: []float64{0, 0, 7071.067811865475},
				},
				"doc29": {
					score:          0.6774608026082964,
					scoreBreakdown: []float64{0.23161973134064517, 0.7845714725717996, 0},
				},
				"doc23": {
					score:          0.5783030702431613,
					scoreBreakdown: []float64{0.32755976365480655, 0.5398948417099355, 0},
				},
				"doc3": {
					score:          0.2550334160459894,
					scoreBreakdown: []float64{0.7651002481379682, 0, 0},
				},
				"doc13": {
					score:          0.2208654210738964,
					scoreBreakdown: []float64{0.6625962632216892, 0, 0},
				},
				"doc5": {
					score:          0.21180931116413285,
					scoreBreakdown: []float64{0, 0, 0.6354279334923986},
				},
				"doc27": {
					score:          0.09227950890170131,
					scoreBreakdown: []float64{0.27683852670510395, 0, 0},
				},
				"doc28": {
					score:          0.0863195764709126,
					scoreBreakdown: []float64{0.2589587294127378, 0, 0},
				},
				"doc30": {
					score:          0.07720657711354839,
					scoreBreakdown: []float64{0.23161973134064517, 0, 0},
				},
				"doc24": {
					score:          0.07720657711354839,
					scoreBreakdown: []float64{0.23161973134064517, 0, 0},
				},
			},
		},
		{
			numSegments: 6,
			queryIndex:  2,
			expectedResults: map[string]testResult{
				"doc7": {
					score:          2357.022603955158,
					scoreBreakdown: []float64{0, 0, 7071.067811865475},
				},
				"doc29": {
					score:          0.6774608026082964,
					scoreBreakdown: []float64{0.23161973134064517, 0.7845714725717996, 0},
				},
				"doc23": {
					score:          0.5783030702431613,
					scoreBreakdown: []float64{0.32755976365480655, 0.5398948417099355, 0},
				},
				"doc3": {
					score:          0.2550334160459894,
					scoreBreakdown: []float64{0.7651002481379682, 0, 0},
				},
				"doc13": {
					score:          0.2208654210738964,
					scoreBreakdown: []float64{0.6625962632216892, 0, 0},
				},
				"doc5": {
					score:          0.21180931116413285,
					scoreBreakdown: []float64{0, 0, 0.6354279334923986},
				},
				"doc27": {
					score:          0.09227950890170131,
					scoreBreakdown: []float64{0.27683852670510395, 0, 0},
				},
				"doc28": {
					score:          0.0863195764709126,
					scoreBreakdown: []float64{0.2589587294127378, 0, 0},
				},
				"doc30": {
					score:          0.07720657711354839,
					scoreBreakdown: []float64{0.23161973134064517, 0, 0},
				},
				"doc24": {
					score:          0.07720657711354839,
					scoreBreakdown: []float64{0.23161973134064517, 0, 0},
				},
			},
		},
	}
	for testCaseNum, testCase := range testCases {
		tmpIndexPath := createTmpIndexPath(t)
		index, err := New(tmpIndexPath, indexMapping)
		if err != nil {
			t.Fatal(err)
		}
		query := searchRequests[testCase.queryIndex]
		err = createMultipleSegmentsIndex(documents, index, testCase.numSegments)
		if err != nil {
			t.Fatal(err)
		}
		res, err := index.Search(query)
		if err != nil {
			t.Fatal(err)
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
			if expectScore != actualScore {
				t.Fatalf("testcase %d failed: expected hit %d to have score %f, got %f", testCaseNum, i, expectedHit.score, hit.Score)
			}
			if len(hit.ScoreBreakdown) != len(expectedHit.scoreBreakdown) {
				t.Fatalf("testcase %d failed: expected hit %d to have %d score breakdowns, got %d", testCaseNum, i, len(expectedHit.scoreBreakdown), len(hit.ScoreBreakdown))
			}
			for j := 0; j < len(hit.ScoreBreakdown); j++ {
				// Truncate to 6 decimal places
				actualScore := truncateScore(hit.ScoreBreakdown[j])
				expectScore := truncateScore(expectedHit.scoreBreakdown[j])
				if expectScore != actualScore {
					t.Fatalf("testcase %d failed: expected hit %d to have score breakdown %f, got %f", testCaseNum, i, expectedHit.scoreBreakdown[j], hit.ScoreBreakdown[j])
				}
			}
		}
		err = index.Close()
		if err != nil {
			t.Fatal(err)
		}
		cleanupTmpIndexPath(t, tmpIndexPath)
	}
}
