//  Copyright (c) 2025 Couchbase, Inc.
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
	"math"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

func createHybridSearchIndex(path string) (Index, error) {
	// Index mapping
	indexMapping := NewIndexMapping()

	// Disable default mapping to match expected configuration
	indexMapping.DefaultMapping.Enabled = false
	indexMapping.DefaultMapping.Dynamic = false

	// Create a specific document mapping type
	docMapping := NewDocumentMapping()
	docMapping.Enabled = true
	docMapping.Dynamic = false

	// Text field for color with specific properties
	colorFieldMapping := NewTextFieldMapping()
	colorFieldMapping.Analyzer = "en" // Use "en" analyzer as specified
	colorFieldMapping.DocValues = true
	colorFieldMapping.IncludeInAll = true
	colorFieldMapping.Store = true
	colorFieldMapping.Index = true
	docMapping.AddFieldMappingsAt("color", colorFieldMapping)

	// Vector field for color vector with L2 similarity
	vecFieldMapping := mapping.NewVectorFieldMapping()
	vecFieldMapping.Dims = 3
	vecFieldMapping.Similarity = index.EuclideanDistance // l2_norm equivalent
	vecFieldMapping.VectorIndexOptimizedFor = "recall"
	docMapping.AddFieldMappingsAt("colorvect_l2", vecFieldMapping)

	// Add the document mapping to the index
	indexMapping.AddDocumentMapping("_default", docMapping)

	// Create index
	return New(path, indexMapping)
}

func getHybridSearchDocuments() []map[string]interface{} {
	documents := []map[string]interface{}{
		{
			"color":        "dark slate blue",
			"colorvect_l2": []float32{72, 61, 139},
		},
		{
			"color":        "blue",
			"colorvect_l2": []float32{0, 0, 255},
		},
		{
			"color":        "navy",
			"colorvect_l2": []float32{0, 0, 128},
		},
		{
			"color":        "steel blue",
			"colorvect_l2": []float32{70, 130, 180},
		},
		{
			"color":        "light blue",
			"colorvect_l2": []float32{173, 216, 230},
		},
		{
			"color":        "deep sky blue",
			"colorvect_l2": []float32{0, 191, 255},
		},
		{
			"color":        "royal blue",
			"colorvect_l2": []float32{65, 105, 225},
		},
		{
			"color":        "powder blue",
			"colorvect_l2": []float32{176, 224, 230},
		},
		{
			"color":        "corn flower blue",
			"colorvect_l2": []float32{100, 149, 237},
		},
		{
			"color":        "alice blue",
			"colorvect_l2": []float32{240, 248, 255},
		},
		{
			"color":        "blue violet",
			"colorvect_l2": []float32{138, 43, 226},
		},
		{
			"color":        "sky blue",
			"colorvect_l2": []float32{135, 206, 235},
		},
		{
			"color":        "indigo",
			"colorvect_l2": []float32{75, 0, 130},
		},
		{
			"color":        "midnight blue",
			"colorvect_l2": []float32{25, 25, 112},
		},
		{
			"color":        "dark blue",
			"colorvect_l2": []float32{0, 0, 139},
		},
		{
			"color":        "medium slate blue",
			"colorvect_l2": []float32{123, 104, 238},
		},
		{
			"color":        "cadet blue",
			"colorvect_l2": []float32{95, 158, 160},
		},
		{
			"color":        "light steel blue",
			"colorvect_l2": []float32{176, 196, 222},
		},
		{
			"color":        "dodger blue",
			"colorvect_l2": []float32{30, 144, 255},
		},
		{
			"color":        "medium blue",
			"colorvect_l2": []float32{0, 0, 205},
		},
		{
			"color":        "slate blue",
			"colorvect_l2": []float32{106, 90, 205},
		},
		{
			"color":        "light sky blue",
			"colorvect_l2": []float32{135, 206, 250},
		},
	}

	return documents
}

func createHybridSearchRequest() *SearchRequest {
	// Create hybrid search request (FTS + KNN)
	textQuery := query.NewMatchPhraseQuery("dark")
	searchRequest := NewSearchRequest(textQuery)

	queryVector_1 := []float32{0, 0, 129} // Similar to blue colors
	searchRequest.AddKNN("colorvect_l2", queryVector_1, 5, 1.0)

	queryVector_2 := []float32{0, 0, 250} // lighter blue
	searchRequest.AddKNN("colorvect_l2", queryVector_2, 5, 1.0)

	src, sws := 1, 10
	searchRequest.Params = Params{ScoreRankConstant: &src, ScoreWindowSize: &sws}

	searchRequest.Size = 10
	searchRequest.Score = ReciprocalRankFusionStrategy

	searchRequest.Explain = false
	return searchRequest
}

// verifyRRFResults verifies that the search results match the expected RRF ranking and scores
func verifyRRFResults(t *testing.T, result *SearchResult) {
	// Manual RRF calculation for verification
	// With k=1 (ScoreRankConstant), RRF formula: 1/(1+rank)
	//
	// FTS "dark" ranks:
	// 1. dark blue, 2. dark slate blue
	//
	// kNN1 [0,0,129] ranks:
	// 1. navy, 2. dark blue, 3. midnight blue, 4. indigo, 5. medium blue
	//
	// kNN2 [0,0,250] ranks:
	// 1. blue, 2. medium blue, 3. dark blue, 4. navy, 5. royal blue

	expectedRRFScores := map[string]float64{
		"dark blue":       1.083333, // FTS(1): 1/2 + kNN1(2): 1/3 + kNN2(3): 1/4 = 1.083333
		"navy":            0.7,      // kNN1(1): 1/2 + kNN2(4): 1/5 = 0.7
		"blue":            0.5,      // kNN2(1): 1/2 = 0.5
		"medium blue":     0.5,      // kNN1(5): 1/6 + kNN2(2): 1/3 = 0.5
		"dark slate blue": 0.333333, // FTS(2): 1/3 = 0.333333
		"midnight blue":   0.25,     // kNN1(3): 1/4 = 0.25
		"indigo":          0.2,      // kNN1(4): 1/5 = 0.2
		"royal blue":      0.166667, // kNN2(5): 1/6 = 0.166667
	}

	// Verify top results match expected RRF ranking
	expectedOrder := []string{"dark blue", "navy", "blue", "medium blue", "dark slate blue", "midnight blue", "indigo", "royal blue"}

	if len(result.Hits) < len(expectedOrder) {
		t.Fatalf("Expected at least %d results, got %d", len(expectedOrder), len(result.Hits))
	}

	for i, expectedID := range expectedOrder {
		if result.Hits[i].ID != expectedID {
			id := result.Hits[i].ID
			if !(id == "blue" || id == "medium blue") { // Don't throw an error, since these scores are the same
				t.Errorf("Position %d: expected %s, got %s", i+1, expectedID, result.Hits[i].ID)
			}
		}

		expectedScore := expectedRRFScores[expectedID]
		actualScore := result.Hits[i].Score
		tolerance := 0.001

		if math.Abs(actualScore-expectedScore) > tolerance {
			t.Errorf("Score for %s: expected %.6f, got %.6f (diff: %.6f)",
				expectedID, expectedScore, actualScore, math.Abs(actualScore-expectedScore))
		}
	}
}

func TestRRFEndToEnd(t *testing.T) {
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	index, err := createHybridSearchIndex(tmpIndexPath)

	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	documents := getHybridSearchDocuments()

	// Index documents
	batch := index.NewBatch()
	for _, doc := range documents {
		colorName := doc["color"].(string)
		err = batch.Index(colorName, doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = index.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	searchRequest := createHybridSearchRequest()

	// Execute search
	result, err := index.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify RRF results
	verifyRRFResults(t, result)
}

// TestRRFAliasWithSingleIndex tests RRF with an alias containing one index
func TestRRFAliasWithSingleIndex(t *testing.T) {
	// Create single index with all documents
	tmpIndexPath := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath)

	index, err := createHybridSearchIndex(tmpIndexPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	documents := getHybridSearchDocuments()

	// Create alias and add the single index
	alias := NewIndexAlias()
	alias.Add(index)

	// Index all documents
	batch := alias.NewBatch()
	for _, doc := range documents {
		colorName := doc["color"].(string)
		err = batch.Index(colorName, doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = alias.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	searchRequest := createHybridSearchRequest()

	// Execute search through alias
	result, err := alias.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify RRF results - should be identical to direct index search
	verifyRRFResults(t, result)
}

// TestRRFAliasWithTwoIndexes tests RRF with an alias containing two indexes
func TestRRFAliasWithTwoIndexes(t *testing.T) {
	documents := getHybridSearchDocuments()

	// Split documents into two groups
	midpoint := len(documents) / 2
	docs1 := documents[:midpoint]
	docs2 := documents[midpoint:]

	// Create first index
	tmpIndexPath1 := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath1)

	index1, err := createHybridSearchIndex(tmpIndexPath1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index1.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Index first half of documents
	batch1 := index1.NewBatch()
	for _, doc := range docs1 {
		colorName := doc["color"].(string)
		err = batch1.Index(colorName, doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = index1.Batch(batch1)
	if err != nil {
		t.Fatal(err)
	}

	// Create second index
	tmpIndexPath2 := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath2)

	index2, err := createHybridSearchIndex(tmpIndexPath2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index2.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Index second half of documents
	batch2 := index2.NewBatch()
	for _, doc := range docs2 {
		colorName := doc["color"].(string)
		err = batch2.Index(colorName, doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = index2.Batch(batch2)
	if err != nil {
		t.Fatal(err)
	}

	// Create alias and add both indexes
	alias := NewIndexAlias()
	alias.Add(index1, index2)

	searchRequest := createHybridSearchRequest()

	// Execute search through alias
	result, err := alias.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify RRF results - should be identical to single index results
	verifyRRFResults(t, result)
}

// TestRRFNestedAliases tests RRF with an alias containing two index aliases
func TestRRFNestedAliases(t *testing.T) {
	documents := getHybridSearchDocuments()

	// Split documents into three groups
	thirdPoint1 := len(documents) / 3
	thirdPoint2 := 2 * len(documents) / 3
	docs1 := documents[:thirdPoint1]
	docs2 := documents[thirdPoint1:thirdPoint2]
	docs3 := documents[thirdPoint2:]

	// Create first index
	tmpIndexPath1 := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath1)

	index1, err := createHybridSearchIndex(tmpIndexPath1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index1.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Index first third of documents
	batch1 := index1.NewBatch()
	for _, doc := range docs1 {
		colorName := doc["color"].(string)
		err = batch1.Index(colorName, doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = index1.Batch(batch1)
	if err != nil {
		t.Fatal(err)
	}

	// Create second index
	tmpIndexPath2 := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath2)

	index2, err := createHybridSearchIndex(tmpIndexPath2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index2.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Index second third of documents
	batch2 := index2.NewBatch()
	for _, doc := range docs2 {
		colorName := doc["color"].(string)
		err = batch2.Index(colorName, doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = index2.Batch(batch2)
	if err != nil {
		t.Fatal(err)
	}

	// Create third index
	tmpIndexPath3 := createTmpIndexPath(t)
	defer cleanupTmpIndexPath(t, tmpIndexPath3)

	index3, err := createHybridSearchIndex(tmpIndexPath3)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index3.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Index third third of documents
	batch3 := index3.NewBatch()
	for _, doc := range docs3 {
		colorName := doc["color"].(string)
		err = batch3.Index(colorName, doc)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = index3.Batch(batch3)
	if err != nil {
		t.Fatal(err)
	}

	// Create first sub-alias (contains 1 index)
	subAlias1 := NewIndexAlias()
	subAlias1.SetName("subAlias1")
	subAlias1.Add(index1)

	// Create second sub-alias (contains 2 indexes)
	subAlias2 := NewIndexAlias()
	subAlias2.SetName("subAlias2")
	subAlias2.Add(index2, index3)

	// Create master alias containing the two sub-aliases
	masterAlias := NewIndexAlias()
	masterAlias.SetName("masterAlias")
	masterAlias.Add(subAlias1, subAlias2)

	searchRequest := createHybridSearchRequest()

	// Execute search through master alias
	result, err := masterAlias.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify RRF results - should be identical to single index results
	verifyRRFResults(t, result)
}
