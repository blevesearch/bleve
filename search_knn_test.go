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
	"math"
	"math/rand"
	"strconv"
	"testing"

	"github.com/blevesearch/bleve/v2/analysis/lang/en"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

const testInputCompressedFile = "test/knn/knn_dataset_queries.zip"
const testDatasetFileName = "knn_dataset.json"
const testQueryFileName = "knn_queries.json"

const testDatasetDims = 384

func TestSimilaritySearchPartitionedIndexRandomized(t *testing.T) {
	runKNNTest(t, true)
}

func TestSimilaritySearchPartitionedIndexNotRandomized(t *testing.T) {
	runKNNTest(t, false)
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

type testResult struct {
	score          float64
	scoreBreakdown map[int]float64
}

func verifyResult(t *testing.T, actualResult *SearchResult, expectedResult map[string]testResult, randomizeDocuments bool, testCaseNum int, skipScoreCheck bool) {
	if len(actualResult.Hits) != len(expectedResult) {
		t.Fatalf("testcase %d failed: expected %d results, got %d", testCaseNum, len(expectedResult), len(actualResult.Hits))
	}
	if skipScoreCheck {
		return
	}
	for i, hit := range actualResult.Hits {
		var expectedHit testResult
		var ok bool
		if expectedHit, ok = expectedResult[hit.ID]; !ok {
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
		expectedResults    map[string]testResult
	}

	testCases := []testCase{
		// l2 norm similarity
		{
			testType:           "single_partition:match_none:oneKNNreq:k=3",
			queryIndex:         0,
			numIndexPartitions: 1,
			mapping:            indexMappingL2Norm,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          0.5547758085810349,
					scoreBreakdown: map[int]float64{1: 1.1095516171620698},
				},
				"doc23": {
					score:          0.3817633037007331,
					scoreBreakdown: map[int]float64{1: 0.7635266074014662},
				},
				"doc28": {
					score:          0.33983667469689355,
					scoreBreakdown: map[int]float64{1: 0.6796733493937871},
				},
			},
		},
		{
			testType:           "multi_partition:match_none:oneKNNreq:k=3",
			queryIndex:         0,
			numIndexPartitions: 4,
			mapping:            indexMappingL2Norm,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          0.5547758085810349,
					scoreBreakdown: map[int]float64{1: 1.1095516171620698},
				},
				"doc23": {
					score:          0.3817633037007331,
					scoreBreakdown: map[int]float64{1: 0.7635266074014662},
				},
				"doc28": {
					score:          0.33983667469689355,
					scoreBreakdown: map[int]float64{1: 0.6796733493937871},
				},
			},
		},
		{
			testType:           "multi_partition:match_none:oneKNNreq:k=2",
			queryIndex:         0,
			numIndexPartitions: 10,
			mapping:            indexMappingL2Norm,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          0.5547758085810349,
					scoreBreakdown: map[int]float64{1: 1.1095516171620698},
				},
				"doc23": {
					score:          0.3817633037007331,
					scoreBreakdown: map[int]float64{1: 0.7635266074014662},
				},
				"doc28": {
					score:          0.33983667469689355,
					scoreBreakdown: map[int]float64{1: 0.6796733493937871},
				},
			},
		},
		{
			testType:           "single_partition:match:oneKNNreq:k=2",
			queryIndex:         1,
			numIndexPartitions: 1,
			mapping:            indexMappingL2Norm,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          1.8859816084399936,
					scoreBreakdown: map[int]float64{0: 0.7764299912779237, 1: 1.1095516171620698},
				},
				"doc23": {
					score:          1.8615644255330264,
					scoreBreakdown: map[int]float64{0: 1.0980378181315602, 1: 0.7635266074014662},
				},
				"doc27": {
					score:          0.4640056648691007,
					scoreBreakdown: map[int]float64{0: 0.9280113297382014},
				},
				"doc28": {
					score:          0.434037555556026,
					scoreBreakdown: map[int]float64{0: 0.868075111112052},
				},
				"doc30": {
					score:          0.38821499563896184,
					scoreBreakdown: map[int]float64{0: 0.7764299912779237},
				},
				"doc24": {
					score:          0.38821499563896184,
					scoreBreakdown: map[int]float64{0: 0.7764299912779237},
				},
			},
		},
		{
			testType:           "multi_partition:match:oneKNNreq:k=2",
			queryIndex:         1,
			numIndexPartitions: 5,
			mapping:            indexMappingL2Norm,
			expectedResults: map[string]testResult{
				"doc23": {
					score:          1.5207250366637521,
					scoreBreakdown: map[int]float64{0: 0.7571984292622859, 1: 0.7635266074014662},
				},
				"doc29": {
					score:          1.4834345192674083,
					scoreBreakdown: map[int]float64{0: 0.3738829021053385, 1: 1.1095516171620698},
				},
				"doc24": {
					score:          0.2677100734235977,
					scoreBreakdown: map[int]float64{0: 0.5354201468471954},
				},
				"doc27": {
					score:          0.22343776840593196,
					scoreBreakdown: map[int]float64{0: 0.4468755368118639},
				},
				"doc28": {
					score:          0.20900689401100958,
					scoreBreakdown: map[int]float64{0: 0.41801378802201916},
				},
				"doc30": {
					score:          0.18694145105266924,
					scoreBreakdown: map[int]float64{0: 0.3738829021053385},
				},
			},
		},
		{
			testType:           "single_partition:disjunction:twoKNNreq:k=2,2",
			queryIndex:         2,
			numIndexPartitions: 1,
			mapping:            indexMappingL2Norm,
			expectedResults: map[string]testResult{
				"doc7": {
					score:          math.MaxFloat64 / 3.0,
					scoreBreakdown: map[int]float64{2: math.MaxFloat64},
				},
				"doc29": {
					score:          0.6774608026082964,
					scoreBreakdown: map[int]float64{0: 0.23161973134064517, 1: 0.7845714725717996},
				},
				"doc23": {
					score:          0.5783030702431613,
					scoreBreakdown: map[int]float64{0: 0.32755976365480655, 1: 0.5398948417099355},
				},
				"doc3": {
					score:          0.2550334160459894,
					scoreBreakdown: map[int]float64{0: 0.7651002481379682},
				},
				"doc13": {
					score:          0.2208654210738964,
					scoreBreakdown: map[int]float64{0: 0.6625962632216892},
				},
				"doc5": {
					score:          0.21180931116413285,
					scoreBreakdown: map[int]float64{2: 0.6354279334923986},
				},
				"doc27": {
					score:          0.09227950890170131,
					scoreBreakdown: map[int]float64{0: 0.2768385267051039},
				},
				"doc28": {
					score:          0.0863195764709126,
					scoreBreakdown: map[int]float64{0: 0.2589587294127378},
				},
				"doc30": {
					score:          0.07720657711354839,
					scoreBreakdown: map[int]float64{0: 0.23161973134064517},
				},
				"doc24": {
					score:          0.07720657711354839,
					scoreBreakdown: map[int]float64{0: 0.23161973134064517},
				},
			},
		},
		{
			testType:           "multi_partition:disjunction:twoKNNreq:k=2,2",
			queryIndex:         2,
			numIndexPartitions: 4,
			mapping:            indexMappingL2Norm,
			expectedResults: map[string]testResult{
				"doc7": {
					score:          math.MaxFloat64 / 3.0,
					scoreBreakdown: map[int]float64{2: math.MaxFloat64},
				},
				"doc29": {
					score:          0.567426591648309,
					scoreBreakdown: map[int]float64{0: 0.06656841490066398, 1: 0.7845714725717996},
				},
				"doc23": {
					score:          0.5639255136185979,
					scoreBreakdown: map[int]float64{0: 0.3059934287179615, 1: 0.5398948417099355},
				},
				"doc5": {
					score:          0.21180931116413285,
					scoreBreakdown: map[int]float64{2: 0.6354279334923986},
				},
				"doc3": {
					score:          0.14064944169372873,
					scoreBreakdown: map[int]float64{0: 0.4219483250811862},
				},
				"doc13": {
					score:          0.12180599172106943,
					scoreBreakdown: map[int]float64{0: 0.3654179751632083},
				},
				"doc27": {
					score:          0.026521491065731144,
					scoreBreakdown: map[int]float64{0: 0.07956447319719343},
				},
				"doc28": {
					score:          0.024808583220893122,
					scoreBreakdown: map[int]float64{0: 0.07442574966267937},
				},
				"doc30": {
					score:          0.02218947163355466,
					scoreBreakdown: map[int]float64{0: 0.06656841490066398},
				},
				"doc24": {
					score:          0.02218947163355466,
					scoreBreakdown: map[int]float64{0: 0.06656841490066398},
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
			mapping:            indexMappingL2Norm,
			expectedResults: map[string]testResult{
				"doc24": {
					score:          1.22027994094805,
					scoreBreakdown: map[int]float64{0: 0.027736154383370196, 1: 0.3471022633855392, 2: 0.5085619451465123, 3: 0.33687957803262836},
				},
				"doc17": {
					score:          0.7851856993753307,
					scoreBreakdown: map[int]float64{0: 0.3367753689069724, 2: 0.3892791754255179, 3: 0.320859721501284},
				},
				"doc21": {
					score:          0.5927148028393034,
					scoreBreakdown: map[int]float64{0: 0.06974846263723515, 2: 0.3914133076090359, 3: 0.3291246335394669},
				},
				"doc14": {
					score:          0.45680756875853035,
					scoreBreakdown: map[int]float64{0: 0.5968461853543279, 3: 0.31676895216273276},
				},
				"doc25": {
					score:          0.292014972318407,
					scoreBreakdown: map[int]float64{0: 0.17861510907524708, 2: 0.405414835561567},
				},
				"doc23": {
					score:          0.24706850662359503,
					scoreBreakdown: map[int]float64{0: 0.09761951136424651, 2: 0.39651750188294355},
				},
				"doc15": {
					score:          0.24489276164017085,
					scoreBreakdown: map[int]float64{0: 0.17216818679645968, 3: 0.317617336483882},
				},
				"doc5": {
					score:          0.10331722282971788,
					scoreBreakdown: map[int]float64{1: 0.4132688913188715},
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
			mapping:            indexMappingL2Norm,
			expectedResults: map[string]testResult{
				"doc24": {
					score:          1.22027994094805,
					scoreBreakdown: map[int]float64{0: 0.027736154383370196, 1: 0.3471022633855392, 2: 0.5085619451465123, 3: 0.33687957803262836},
				},
				"doc17": {
					score:          0.7851856993753307,
					scoreBreakdown: map[int]float64{0: 0.3367753689069724, 2: 0.3892791754255179, 3: 0.320859721501284},
				},
				"doc21": {
					score:          0.5927148028393034,
					scoreBreakdown: map[int]float64{0: 0.06974846263723515, 2: 0.3914133076090359, 3: 0.3291246335394669},
				},
			},
		},
		{
			// from = 3
			// size = 3
			testType:           "pagination",
			queryIndex:         5,
			numIndexPartitions: 4,
			mapping:            indexMappingL2Norm,
			expectedResults: map[string]testResult{
				"doc14": {
					score:          0.45680756875853035,
					scoreBreakdown: map[int]float64{0: 0.5968461853543279, 3: 0.31676895216273276},
				},
				"doc25": {
					score:          0.292014972318407,
					scoreBreakdown: map[int]float64{0: 0.17861510907524708, 2: 0.405414835561567},
				},
				"doc23": {
					score:          0.24706850662359503,
					scoreBreakdown: map[int]float64{0: 0.09761951136424651, 2: 0.39651750188294355},
				},
			},
		},
		// dot product similarity
		{
			testType:           "single_partition:match_none:oneKNNreq:k=3",
			queryIndex:         0,
			numIndexPartitions: 1,
			mapping:            indexMappingDotProduct,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          0.2746838331222534,
					scoreBreakdown: map[int]float64{1: 0.5493676662445068},
				},
				"doc23": {
					score:          0.17257216572761536,
					scoreBreakdown: map[int]float64{1: 0.3451443314552307},
				},
				"doc28": {
					score:          0.13217630982398987,
					scoreBreakdown: map[int]float64{1: 0.26435261964797974},
				},
			},
		},
		{
			testType:           "multi_partition:match_none:oneKNNreq:k=3",
			queryIndex:         0,
			numIndexPartitions: 4,
			mapping:            indexMappingDotProduct,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          0.2746838331222534,
					scoreBreakdown: map[int]float64{1: 0.5493676662445068},
				},
				"doc23": {
					score:          0.17257216572761536,
					scoreBreakdown: map[int]float64{1: 0.3451443314552307},
				},
				"doc28": {
					score:          0.13217630982398987,
					scoreBreakdown: map[int]float64{1: 0.26435261964797974},
				},
			},
		},
		{
			testType:           "multi_partition:match_none:oneKNNreq:k=2",
			queryIndex:         0,
			numIndexPartitions: 10,
			mapping:            indexMappingDotProduct,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          0.2746838331222534,
					scoreBreakdown: map[int]float64{1: 0.5493676662445068},
				},
				"doc23": {
					score:          0.17257216572761536,
					scoreBreakdown: map[int]float64{1: 0.3451443314552307},
				},
				"doc28": {
					score:          0.13217630982398987,
					scoreBreakdown: map[int]float64{1: 0.26435261964797974},
				},
			},
		},
		{
			testType:           "single_partition:match:oneKNNreq:k=2",
			queryIndex:         1,
			numIndexPartitions: 1,
			mapping:            indexMappingDotProduct,
			expectedResults: map[string]testResult{
				"doc23": {
					score:          1.443182149586791,
					scoreBreakdown: map[int]float64{0: 1.0980378181315602, 1: 0.3451443314552307},
				},
				"doc29": {
					score:          1.3257976575224304,
					scoreBreakdown: map[int]float64{0: 0.7764299912779237, 1: 0.5493676662445068},
				},
				"doc27": {
					score:          0.4640056648691007,
					scoreBreakdown: map[int]float64{0: 0.9280113297382014},
				},
				"doc28": {
					score:          0.434037555556026,
					scoreBreakdown: map[int]float64{0: 0.868075111112052},
				},
				"doc30": {
					score:          0.38821499563896184,
					scoreBreakdown: map[int]float64{0: 0.7764299912779237},
				},
				"doc24": {
					score:          0.38821499563896184,
					scoreBreakdown: map[int]float64{0: 0.7764299912779237},
				},
			},
		},
		{
			testType:           "multi_partition:match:oneKNNreq:k=2",
			queryIndex:         1,
			numIndexPartitions: 5,
			mapping:            indexMappingDotProduct,
			expectedResults: map[string]testResult{
				"doc23": {
					score:          1.1023427607175167,
					scoreBreakdown: map[int]float64{0: 0.7571984292622859, 1: 0.3451443314552307},
				},
				"doc29": {
					score:          0.9232505683498453,
					scoreBreakdown: map[int]float64{0: 0.3738829021053385, 1: 0.5493676662445068},
				},
				"doc24": {
					score:          0.2677100734235977,
					scoreBreakdown: map[int]float64{0: 0.5354201468471954},
				},
				"doc27": {
					score:          0.22343776840593196,
					scoreBreakdown: map[int]float64{0: 0.4468755368118639},
				},
				"doc28": {
					score:          0.20900689401100958,
					scoreBreakdown: map[int]float64{0: 0.41801378802201916},
				},
				"doc30": {
					score:          0.18694145105266924,
					scoreBreakdown: map[int]float64{0: 0.3738829021053385},
				},
			},
		},
		{
			testType:           "single_partition:disjunction:twoKNNreq:k=2,2",
			queryIndex:         2,
			numIndexPartitions: 1,
			mapping:            indexMappingDotProduct,
			expectedResults: map[string]testResult{
				"doc29": {
					score:          0.4133875556711759,
					scoreBreakdown: map[int]float64{0: 0.23161973134064517, 1: 0.38846160216611875},
				},
				"doc23": {
					score:          0.3810757739432651,
					scoreBreakdown: map[int]float64{0: 0.32755976365480655, 1: 0.24405389726009102},
				},
				"doc3": {
					score:          0.2550334160459894,
					scoreBreakdown: map[int]float64{0: 0.7651002481379682},
				},
				"doc7": {
					score:          0.23570219015076832,
					scoreBreakdown: map[int]float64{2: 0.707106570452305},
				},
				"doc13": {
					score:          0.2208654210738964,
					scoreBreakdown: map[int]float64{0: 0.6625962632216892},
				},
				"doc5": {
					score:          0.10455702372648192,
					scoreBreakdown: map[int]float64{2: 0.31367107117944576},
				},
				"doc27": {
					score:          0.09227950890170131,
					scoreBreakdown: map[int]float64{0: 0.2768385267051039},
				},
				"doc28": {
					score:          0.0863195764709126,
					scoreBreakdown: map[int]float64{0: 0.2589587294127378},
				},
				"doc30": {
					score:          0.07720657711354839,
					scoreBreakdown: map[int]float64{0: 0.23161973134064517},
				},
				"doc24": {
					score:          0.07720657711354839,
					scoreBreakdown: map[int]float64{0: 0.23161973134064517},
				},
			},
		},
		{
			testType:           "multi_partition:disjunction:twoKNNreq:k=2,2",
			queryIndex:         2,
			numIndexPartitions: 4,
			mapping:            indexMappingDotProduct,
			expectedResults: map[string]testResult{
				"doc23": {
					score:          0.36669821731870167,
					scoreBreakdown: map[int]float64{0: 0.3059934287179615, 1: 0.24405389726009102},
				},
				"doc29": {
					score:          0.30335334471118847,
					scoreBreakdown: map[int]float64{0: 0.06656841490066398, 1: 0.38846160216611875},
				},
				"doc7": {
					score:          0.23570219015076832,
					scoreBreakdown: map[int]float64{2: 0.707106570452305},
				},
				"doc3": {
					score:          0.14064944169372873,
					scoreBreakdown: map[int]float64{0: 0.4219483250811862},
				},
				"doc13": {
					score:          0.12180599172106943,
					scoreBreakdown: map[int]float64{0: 0.3654179751632083},
				},
				"doc5": {
					score:          0.10455702372648192,
					scoreBreakdown: map[int]float64{2: 0.31367107117944576},
				},
				"doc27": {
					score:          0.026521491065731144,
					scoreBreakdown: map[int]float64{0: 0.07956447319719343},
				},
				"doc28": {
					score:          0.024808583220893122,
					scoreBreakdown: map[int]float64{0: 0.07442574966267937},
				},
				"doc30": {
					score:          0.02218947163355466,
					scoreBreakdown: map[int]float64{0: 0.06656841490066398},
				},
				"doc24": {
					score:          0.02218947163355466,
					scoreBreakdown: map[int]float64{0: 0.06656841490066398},
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
			mapping:            indexMappingDotProduct,
			expectedResults: map[string]testResult{
				"doc24": {
					score:          0.45716299530348536,
					scoreBreakdown: map[int]float64{0: 0.027736154383370196, 1: 0.09718442379837483, 2: 0.24962871627005187, 3: 0.08261370085168844},
				},
				"doc17": {
					score:          0.40792221729717437,
					scoreBreakdown: map[int]float64{0: 0.3367753689069724, 2: 0.14920840064225363, 3: 0.05791252018033977},
				},
				"doc14": {
					score:          0.3240253778614369,
					scoreBreakdown: map[int]float64{0: 0.5968461853543279, 3: 0.05120457036854595},
				},
				"doc21": {
					score:          0.2191859820036201,
					scoreBreakdown: map[int]float64{0: 0.06974846263723515, 2: 0.1515430653633034, 3: 0.0709564480042883},
				},
				"doc25": {
					score:          0.1724318546817751,
					scoreBreakdown: map[int]float64{0: 0.17861510907524708, 2: 0.16624860028830313},
				},
				"doc23": {
					score:          0.12732176553007302,
					scoreBreakdown: map[int]float64{0: 0.09761951136424651, 2: 0.15702401969589952},
				},
				"doc15": {
					score:          0.11238897955499198,
					scoreBreakdown: map[int]float64{0: 0.17216818679645968, 3: 0.052609772313524296},
				},
				"doc20": {
					score:          0.06759830898809112,
					scoreBreakdown: map[int]float64{0: 0.2703932359523645},
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
			mapping:            indexMappingDotProduct,
			expectedResults: map[string]testResult{
				"doc24": {
					score:          0.45716299530348536,
					scoreBreakdown: map[int]float64{0: 0.027736154383370196, 1: 0.09718442379837483, 2: 0.24962871627005187, 3: 0.08261370085168844},
				},
				"doc17": {
					score:          0.40792221729717437,
					scoreBreakdown: map[int]float64{0: 0.3367753689069724, 2: 0.14920840064225363, 3: 0.05791252018033977},
				},
				"doc14": {
					score:          0.3240253778614369,
					scoreBreakdown: map[int]float64{0: 0.5968461853543279, 3: 0.05120457036854595},
				},
			},
		},
		{
			// from = 3
			// size = 3
			testType:           "pagination",
			queryIndex:         5,
			numIndexPartitions: 4,
			mapping:            indexMappingDotProduct,
			expectedResults: map[string]testResult{
				"doc21": {
					score:          0.2191859820036201,
					scoreBreakdown: map[int]float64{0: 0.06974846263723515, 2: 0.1515430653633034, 3: 0.0709564480042883},
				},
				"doc25": {
					score:          0.1724318546817751,
					scoreBreakdown: map[int]float64{0: 0.17861510907524708, 2: 0.16624860028830313},
				},
				"doc23": {
					score:          0.12732176553007302,
					scoreBreakdown: map[int]float64{0: 0.09761951136424651, 2: 0.15702401969589952},
				},
			},
		},
	}

	index := NewIndexAlias()
	for testCaseNum, testCase := range testCases {
		index.indexes = make([]Index, 0)
		indexPaths := createPartitionedIndex(documents, index, testCase.numIndexPartitions, testCase.mapping, t)
		query := searchRequests[testCase.queryIndex]
		res, err := index.Search(query)
		if err != nil {
			t.Fatal(err)
		}
		// pagination test case -> scores are not deterministic
		skipScoreCheck := testCase.testType == "pagination"
		verifyResult(t, res, testCase.expectedResults, randomizeDocuments, testCaseNum, skipScoreCheck)
		cleanUp(t, indexPaths, index.indexes...)
	}
}

func getExpectedResultFromSearchResult(res *SearchResult) map[string]testResult {
	rv := make(map[string]testResult)
	for _, hit := range res.Hits {
		rv[hit.ID] = testResult{
			score:          hit.Score,
			scoreBreakdown: hit.ScoreBreakdown,
		}
	}
	return rv
}
func TestSimilaritySearchMultipleSegments(t *testing.T) {
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
	}
	for testCaseNum, testCase := range testCases {
		// run single segment test first
		tmpIndexPath := createTmpIndexPath(t)
		index, err := New(tmpIndexPath, testCase.mapping)
		if err != nil {
			t.Fatal(err)
		}
		query := searchRequests[testCase.queryIndex]
		err = createMultipleSegmentsIndex(documents, index, 1)
		if err != nil {
			t.Fatal(err)
		}
		res, err := index.Search(query)
		if err != nil {
			t.Fatal(err)
		}
		expectedResult := getExpectedResultFromSearchResult(res)
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
		err = createMultipleSegmentsIndex(documents, index, testCase.numSegments)
		if err != nil {
			t.Fatal(err)
		}
		actualResult, err := index.Search(query)
		if err != nil {
			t.Fatal(err)
		}
		verifyResult(t, actualResult, expectedResult, false, testCaseNum, false)
		err = index.Close()
		if err != nil {
			t.Fatal(err)
		}
		cleanupTmpIndexPath(t, tmpIndexPath)
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
	conjunction, err := queryWithKNN(searchRequest)
	if err != nil {
		t.Fatalf("unexpected error for AND knn operator")
	}

	conj, ok := conjunction.(*query.ConjunctionQuery)
	if !ok {
		t.Fatalf("expected conjunction query")
	}

	if len(conj.Conjuncts) == 3 {
		_, ok := conj.Conjuncts[0].(*query.TermQuery)
		if !ok {
			t.Fatalf("expected first query to be a term query,"+
				" but it's %T", conj.Conjuncts[0])
		}
	} else {
		t.Fatalf("expected 3 conjuncts")
	}

	// Disjunction
	searchRequest.AddKNNOperator(knnOperatorOr)
	disjunction, err := queryWithKNN(searchRequest)
	if err != nil {
		t.Fatalf("unexpected error for OR knn operator")
	}

	disj, ok := disjunction.(*query.DisjunctionQuery)
	if !ok {
		t.Fatalf("expected disjunction query")
	}

	if len(disj.Disjuncts) == 3 {
		_, ok := disj.Disjuncts[0].(*query.TermQuery)
		if !ok {
			t.Fatalf("expected first query to be a term query,"+
				" but it's %T", conj.Conjuncts[0])
		}
	} else {
		t.Fatalf("expected 3 disjuncts")
	}

	// Incorrect operator.
	searchRequest.AddKNNOperator("bs_op")
	searchRequest.Query, err = queryWithKNN(searchRequest)
	if err == nil {
		t.Fatalf("expected error for incorrect knn operator")
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
		searchReq.AddKNN(vecFieldName, test.queryVec, k, 1000)

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
