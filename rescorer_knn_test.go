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
	"context"
	"math"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
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

func createScoreFusionRequest(scoreMethod string, knn bool) *SearchRequest {
	// Create hybrid search request (FTS + KNN)
	textQuery := query.NewMatchPhraseQuery("dark")
	searchRequest := NewSearchRequest(textQuery)

	if knn {
		queryVector_1 := []float32{0, 0, 129} // Similar to blue colors
		searchRequest.AddKNN("colorvect_l2", queryVector_1, 5, 1.0)

		queryVector_2 := []float32{0, 0, 250} // lighter blue
		searchRequest.AddKNN("colorvect_l2", queryVector_2, 5, 1.0)
	}

	params := RequestParams{1, 10}
	searchRequest.AddParams(params)

	searchRequest.Size = 10

	searchRequest.Score = scoreMethod
	searchRequest.Explain = false
	return searchRequest
}

// verifyRRFResults verifies that the search hits match the expected RRF ranking and scores
func verifyRRFResults(t *testing.T, hits search.DocumentMatchCollection) {
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

	if len(hits) < len(expectedOrder) {
		t.Fatalf("Expected at least %d results, got %d", len(expectedOrder), len(hits))
	}

	for i, expectedID := range expectedOrder {
		if hits[i].ID != expectedID {
			id := hits[i].ID
			if !(id == "blue" || id == "medium blue") { // Don't throw an error, since these scores are the same
				t.Errorf("Position %d: expected %s, got %s", i+1, expectedID, hits[i].ID)
			}
		}

		expectedScore := expectedRRFScores[expectedID]
		actualScore := hits[i].Score
		tolerance := 0.001

		if math.Abs(actualScore-expectedScore) > tolerance {
			t.Errorf("Score for %s: expected %.6f, got %.6f (diff: %.6f)",
				expectedID, expectedScore, actualScore, math.Abs(actualScore-expectedScore))
		}
	}
}

// setupSingleIndex creates a single index with all documents
func setupSingleIndex(t *testing.T) (Index, func()) {
	tmpIndexPath := createTmpIndexPath(t)

	index, err := createHybridSearchIndex(tmpIndexPath)
	if err != nil {
		t.Fatal(err)
	}

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

	cleanup := func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
		cleanupTmpIndexPath(t, tmpIndexPath)
	}

	return index, cleanup
}

// setupAliasWithSingleIndex creates an alias containing one index with all documents
func setupAliasWithSingleIndex(t *testing.T) (Index, func()) {
	tmpIndexPath := createTmpIndexPath(t)

	index, err := createHybridSearchIndex(tmpIndexPath)
	if err != nil {
		t.Fatal(err)
	}

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

	cleanup := func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
		cleanupTmpIndexPath(t, tmpIndexPath)
	}

	return alias, cleanup
}

// setupAliasWithTwoIndexes creates an alias containing two indexes with documents split between them
func setupAliasWithTwoIndexes(t *testing.T) (Index, func()) {
	documents := getHybridSearchDocuments()

	// Split documents into two groups
	midpoint := len(documents) / 2
	docs1 := documents[:midpoint]
	docs2 := documents[midpoint:]

	// Create first index
	tmpIndexPath1 := createTmpIndexPath(t)
	index1, err := createHybridSearchIndex(tmpIndexPath1)
	if err != nil {
		t.Fatal(err)
	}

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
	index2, err := createHybridSearchIndex(tmpIndexPath2)
	if err != nil {
		t.Fatal(err)
	}

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

	cleanup := func() {
		err := index1.Close()
		if err != nil {
			t.Fatal(err)
		}
		err = index2.Close()
		if err != nil {
			t.Fatal(err)
		}
		cleanupTmpIndexPath(t, tmpIndexPath1)
		cleanupTmpIndexPath(t, tmpIndexPath2)
	}

	return alias, cleanup
}

// setupNestedAliases creates nested aliases with three indexes spread across sub-aliases
func setupNestedAliases(t *testing.T) (Index, func()) {
	documents := getHybridSearchDocuments()

	// Split documents into three groups
	thirdPoint1 := len(documents) / 3
	thirdPoint2 := 2 * len(documents) / 3
	docs1 := documents[:thirdPoint1]
	docs2 := documents[thirdPoint1:thirdPoint2]
	docs3 := documents[thirdPoint2:]

	// Create first index
	tmpIndexPath1 := createTmpIndexPath(t)
	index1, err := createHybridSearchIndex(tmpIndexPath1)
	if err != nil {
		t.Fatal(err)
	}

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
	index2, err := createHybridSearchIndex(tmpIndexPath2)
	if err != nil {
		t.Fatal(err)
	}

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
	index3, err := createHybridSearchIndex(tmpIndexPath3)
	if err != nil {
		t.Fatal(err)
	}

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

	cleanup := func() {
		err := index1.Close()
		if err != nil {
			t.Fatal(err)
		}
		err = index2.Close()
		if err != nil {
			t.Fatal(err)
		}
		err = index3.Close()
		if err != nil {
			t.Fatal(err)
		}
		cleanupTmpIndexPath(t, tmpIndexPath1)
		cleanupTmpIndexPath(t, tmpIndexPath2)
		cleanupTmpIndexPath(t, tmpIndexPath3)
	}

	return masterAlias, cleanup
}

func TestRRFEndToEnd(t *testing.T) {
	// Setup the index configuration
	index, cleanup := setupSingleIndex(t)
	defer cleanup()

	// Create the search request
	searchRequest := createScoreFusionRequest(ScoreRRF, true)

	// Execute search
	result, err := index.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify RRF results
	verifyRRFResults(t, result.Hits)
}

// TestRRFAliasWithSingleIndex tests RRF with an alias containing one index
func TestRRFAliasWithSingleIndex(t *testing.T) {
	// Setup the alias configuration
	alias, cleanup := setupAliasWithSingleIndex(t)
	defer cleanup()

	// Create the search request
	searchRequest := createScoreFusionRequest(ScoreRRF, true)

	// Execute search through alias
	result, err := alias.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify RRF results - should be identical to direct index search
	verifyRRFResults(t, result.Hits)
}

// TestRRFAliasWithTwoIndexes tests RRF with an alias containing two indexes
func TestRRFAliasWithTwoIndexes(t *testing.T) {
	// Setup the alias configuration
	alias, cleanup := setupAliasWithTwoIndexes(t)
	defer cleanup()

	// Create the search request
	searchRequest := createScoreFusionRequest(ScoreRRF, true)

	// Execute search through alias
	result, err := alias.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify RRF results - should be identical to single index results
	verifyRRFResults(t, result.Hits)
}

// TestRRFNestedAliases tests RRF with an alias containing two index aliases
func TestRRFNestedAliases(t *testing.T) {
	// Setup the nested aliases configuration
	masterAlias, cleanup := setupNestedAliases(t)
	defer cleanup()

	// Create the search request
	searchRequest := createScoreFusionRequest(ScoreRRF, true)

	// Execute search through master alias
	result, err := masterAlias.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify RRF results - should be identical to single index results
	verifyRRFResults(t, result.Hits)
}

// TestRRFPagination tests RRF with pagination across different index/alias configurations
func TestRRFPagination(t *testing.T) {
	scenarios := []struct {
		name  string
		setup func(t *testing.T) (Index, func())
	}{
		{
			name:  "SingleIndex",
			setup: setupSingleIndex,
		},
		{
			name:  "AliasWithSingleIndex",
			setup: setupAliasWithSingleIndex,
		},
		{
			name:  "AliasWithTwoIndexes",
			setup: setupAliasWithTwoIndexes,
		},
		{
			name:  "NestedAliases",
			setup: setupNestedAliases,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Setup the index/alias configuration
			index, cleanup := scenario.setup(t)
			defer cleanup()

			// Create first page request (first 5 results)
			firstPageRequest := createScoreFusionRequest(ScoreRRF, true)
			firstPageRequest.From = 0
			firstPageRequest.Size = 5

			// Execute first page search
			firstPageResult, err := index.Search(firstPageRequest)
			if err != nil {
				t.Fatal(err)
			}

			// Create second page request (next 5 results, starting from index 5)
			secondPageRequest := createScoreFusionRequest(ScoreRRF, true)
			secondPageRequest.From = 5
			secondPageRequest.Size = 5

			// Execute second page search
			secondPageResult, err := index.Search(secondPageRequest)
			if err != nil {
				t.Fatal(err)
			}

			// Combine results from both pages
			combinedHits := make(search.DocumentMatchCollection, 0, len(firstPageResult.Hits)+len(secondPageResult.Hits))
			combinedHits = append(combinedHits, firstPageResult.Hits...)
			combinedHits = append(combinedHits, secondPageResult.Hits...)

			// Verify we have the expected number of results
			if len(firstPageResult.Hits) != 5 {
				t.Errorf("Expected 5 results in first page, got %d", len(firstPageResult.Hits))
			}
			if len(secondPageResult.Hits) != 3 {
				t.Errorf("Expected 3 results in second page, got %d", len(secondPageResult.Hits))
			}

			// Verify combined RRF results match expected ranking
			verifyRRFResults(t, combinedHits)
		})
	}
}

// TestHybridRRFFaceting tests that facet results are identical whether using RRF or default scoring in hybrid search
func TestRRFFaceting(t *testing.T) {
	scenarios := []struct {
		name  string
		setup func(t *testing.T) (Index, func())
	}{
		{
			name:  "SingleIndex",
			setup: setupSingleIndex,
		},
		{
			name:  "AliasWithSingleIndex",
			setup: setupAliasWithSingleIndex,
		},
		{
			name:  "AliasWithTwoIndexes",
			setup: setupAliasWithTwoIndexes,
		},
		{
			name:  "NestedAliases",
			setup: setupNestedAliases,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Setup the index/alias configuration
			index, cleanup := scenario.setup(t)
			defer cleanup()

			// Create search request with default scoring and facets
			defaultRequest := createScoreFusionRequest(ScoreDefault, false)
			defaultRequest.Score = ScoreDefault // Use default scoring
			defaultRequest.Size = 10
			// Add facet for color field with size 10
			colorFacet := NewFacetRequest("color", 10)
			defaultRequest.AddFacet("color", colorFacet)

			// Create search request with RRF scoring and identical facets
			rrfRequest := createScoreFusionRequest(ScoreRRF, true)
			rrfRequest.Score = ScoreRRF // Use RRF scoring
			rrfRequest.Size = 10
			// Add identical facet for color field with size 10
			colorFacetRRF := NewFacetRequest("color", 10)
			rrfRequest.AddFacet("color", colorFacetRRF)

			// Execute both searches
			defaultResult, err := index.Search(defaultRequest)
			if err != nil {
				t.Fatalf("Default scoring search failed: %v", err)
			}

			rrfResult, err := index.Search(rrfRequest)
			if err != nil {
				t.Fatalf("RRF scoring search failed: %v", err)
			}

			// Verify both searches returned results
			if len(defaultResult.Hits) == 0 {
				t.Fatal("Expected search results with default scoring, got none")
			}
			if len(rrfResult.Hits) == 0 {
				t.Fatal("Expected search results with RRF scoring, got none")
			}

			// Verify both searches returned facets
			if defaultResult.Facets == nil {
				t.Fatal("Expected facets with default scoring, got nil")
			}
			if rrfResult.Facets == nil {
				t.Fatal("Expected facets with RRF scoring, got nil")
			}

			// Check that color facet exists in both results
			defaultColorFacet, defaultExists := defaultResult.Facets["color"]
			rrfColorFacet, rrfExists := rrfResult.Facets["color"]

			if !defaultExists {
				t.Fatal("Expected color facet in default scoring results")
			}
			if !rrfExists {
				t.Fatal("Expected color facet in RRF scoring results")
			}

			// Compare the facet results - they should be identical
			// Since facets are based on the document corpus and not scoring,
			// they should not be affected by the scoring method (even with KNN)
			if defaultColorFacet.Total != rrfColorFacet.Total {
				t.Errorf("Facet totals differ: default=%d, RRF=%d",
					defaultColorFacet.Total, rrfColorFacet.Total)
			}

			if defaultColorFacet.Missing != rrfColorFacet.Missing {
				t.Errorf("Facet missing counts differ: default=%d, RRF=%d",
					defaultColorFacet.Missing, rrfColorFacet.Missing)
			}

			if defaultColorFacet.Other != rrfColorFacet.Other {
				t.Errorf("Facet other counts differ: default=%d, RRF=%d",
					defaultColorFacet.Other, rrfColorFacet.Other)
			}

			// Compare the facet terms
			defaultTerms := defaultColorFacet.Terms.Terms()
			rrfTerms := rrfColorFacet.Terms.Terms()

			if len(defaultTerms) != len(rrfTerms) {
				t.Errorf("Facet terms count differs: default=%d, RRF=%d",
					len(defaultTerms), len(rrfTerms))
			} else {
				// Compare each term
				for i, defaultTerm := range defaultTerms {
					rrfTerm := rrfTerms[i]
					if defaultTerm.Term != rrfTerm.Term {
						t.Errorf("Facet term differs at position %d: default=%s, RRF=%s",
							i, defaultTerm.Term, rrfTerm.Term)
					}
					if defaultTerm.Count != rrfTerm.Count {
						t.Errorf("Facet term count differs for %s: default=%d, RRF=%d",
							defaultTerm.Term, defaultTerm.Count, rrfTerm.Count)
					}
				}
			}
		})
	}
}

// verifyRSFResults verifies that the search hits match expected RSF ranking and scores
func verifyRSFResults(t *testing.T, hits search.DocumentMatchCollection) {
	// For RSF, we expect similar high-level results to RRF but with different scoring methodology
	// RSF uses min-max normalization of scores within the window
	// Expected top documents should include those matching "dark" query and similar vectors

	// Verify we have reasonable number of results
	if len(hits) == 0 {
		t.Fatal("Expected non-empty search results for RSF")
	}

	// Verify we have at least 8 results
	if len(hits) < 8 {
		t.Errorf("Expected at least 6 results for RSF, got %d", len(hits))
	}

	// Documents that should definitely appear in top results (high relevance)
	// These all get both text relevance (for "dark blue") or strong vector similarity
	topExpectedDocs := []string{"dark blue", "navy", "blue", "medium blue"}

	// Create map of all hits for easier lookup
	docMap := make(map[string]int) // doc -> position (0-based)
	for i, hit := range hits {
		docMap[hit.ID] = i
	}

	// Verify that "dark blue" appears in top 5 positions (high text + vector relevance)
	if pos, found := docMap["dark blue"]; !found {
		t.Error("Expected 'dark blue' to appear in results but not found")
	} else if pos >= 5 {
		t.Errorf("Expected 'dark blue' in top 3 positions, found at position %d", pos+1)
	}

	// Verify that at least 3 of the top expected documents appear in top 5 results
	topFoundCount := 0
	for _, expectedDoc := range topExpectedDocs {
		if pos, found := docMap[expectedDoc]; found && pos < 5 {
			topFoundCount++
		}
	}
	if topFoundCount < 3 {
		t.Errorf("Expected at least 3 of top expected documents in top 5 results, found %d", topFoundCount)
	}

	// Verify scores are reasonable and within expected range
	// RSF scores should be between 0 and sum of weights (3.0 with default weights)
	// but typically should be more constrained than the full range
	for i, hit := range hits {
		if hit.Score < 0 || hit.Score > 3.0 {
			t.Errorf("Hit %d (%s) has unreasonable score: %.6f", i, hit.ID, hit.Score)
		}
		// First hit should have a substantial score (at least 0.1)
		if i == 0 && hit.Score < 0.1 {
			t.Errorf("Top hit (%s) has unexpectedly low score: %.6f", hit.ID, hit.Score)
		}
	}

	// Verify hits are sorted by score descending with strict ordering
	for i := 1; i < len(hits); i++ {
		if hits[i-1].Score < hits[i].Score {
			t.Errorf("Hits not sorted properly: hit %d (%s, score %.6f) < hit %d (%s, score %.6f)",
				i, hits[i-1].ID, hits[i-1].Score, i+1, hits[i].ID, hits[i].Score)
		}
	}

	// Verify score range is reasonable - top score should be significantly higher than bottom
	topScore := hits[0].Score
	fifthScore := hits[4].Score
	if topScore-fifthScore < 0.001 {
		t.Errorf("Insufficient score differentiation: top score %.6f, 5rd score %.6f (diff: %.6f)",
			topScore, fifthScore, topScore-fifthScore)
	}
}

// TestRSFEndToEnd tests RSF scoring with a single index
func TestRSFEndToEnd(t *testing.T) {
	// Setup the index configuration
	index, cleanup := setupSingleIndex(t)
	defer cleanup()

	// Create the search request
	searchRequest := createScoreFusionRequest(ScoreRSF, true)

	ctx := context.Background()
	ctx = context.WithValue(ctx, search.SearchTypeKey, search.GlobalScoring)

	// Execute search
	result, err := index.SearchInContext(ctx, searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify RSF results
	verifyRSFResults(t, result.Hits)
}

// TestRSFAliasWithSingleIndex tests RSF with an alias containing one index
func TestRSFAliasWithSingleIndex(t *testing.T) {
	// Setup the alias configuration
	alias, cleanup := setupAliasWithSingleIndex(t)
	defer cleanup()

	// Create the search request
	searchRequest := createScoreFusionRequest(ScoreRSF, true)

	ctx := context.Background()
	ctx = context.WithValue(ctx, search.SearchTypeKey, search.GlobalScoring)

	// Execute search
	result, err := alias.SearchInContext(ctx, searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify RSF results - should be identical to direct index search
	verifyRSFResults(t, result.Hits)
}

// TestRSFAliasWithTwoIndexes tests RSF with an alias containing two indexes
func TestRSFAliasWithTwoIndexes(t *testing.T) {
	// Setup the alias configuration
	alias, cleanup := setupAliasWithTwoIndexes(t)
	defer cleanup()

	// Create the search request
	searchRequest := createScoreFusionRequest(ScoreRSF, true)

	ctx := context.Background()
	ctx = context.WithValue(ctx, search.SearchTypeKey, search.GlobalScoring)

	// Execute search
	result, err := alias.SearchInContext(ctx, searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify RSF results - should be identical to single index results
	verifyRSFResults(t, result.Hits)
}

// TestRSFNestedAliases tests RSF with an alias containing two index aliases
func TestRSFNestedAliases(t *testing.T) {
	// Setup the nested aliases configuration
	masterAlias, cleanup := setupNestedAliases(t)
	defer cleanup()

	// Create the search request
	searchRequest := createScoreFusionRequest(ScoreRSF, true)

	ctx := context.Background()
	ctx = context.WithValue(ctx, search.SearchTypeKey, search.GlobalScoring)

	// Execute search
	result, err := masterAlias.SearchInContext(ctx, searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify RSF results - should be identical to single index results
	verifyRSFResults(t, result.Hits)
}

// TestRSFPagination tests RSF with pagination across different index/alias configurations
func TestRSFPagination(t *testing.T) {
	scenarios := []struct {
		name  string
		setup func(t *testing.T) (Index, func())
	}{
		{
			name:  "SingleIndex",
			setup: setupSingleIndex,
		},
		{
			name:  "AliasWithSingleIndex",
			setup: setupAliasWithSingleIndex,
		},
		{
			name:  "AliasWithTwoIndexes",
			setup: setupAliasWithTwoIndexes,
		},
		{
			name:  "NestedAliases",
			setup: setupNestedAliases,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Setup the index/alias configuration
			index, cleanup := scenario.setup(t)
			defer cleanup()

			// Create first page request (first 5 results)
			firstPageRequest := createScoreFusionRequest(ScoreDefault, true)
			firstPageRequest.From = 0
			firstPageRequest.Size = 5

			// Execute first page search
			firstPageResult, err := index.Search(firstPageRequest)
			if err != nil {
				t.Fatal(err)
			}

			// Create second page request (next 5 results, starting from index 5)
			secondPageRequest := createScoreFusionRequest(ScoreDefault, true)
			secondPageRequest.From = 5
			secondPageRequest.Size = 5

			// Execute second page search
			secondPageResult, err := index.Search(secondPageRequest)
			if err != nil {
				t.Fatal(err)
			}

			// Combine results from both pages
			combinedHits := make(search.DocumentMatchCollection, 0, len(firstPageResult.Hits)+len(secondPageResult.Hits))
			combinedHits = append(combinedHits, firstPageResult.Hits...)
			combinedHits = append(combinedHits, secondPageResult.Hits...)

			// Verify we have reasonable number of results
			if len(firstPageResult.Hits) == 0 {
				t.Error("Expected results in first page, got none")
			}
			if len(combinedHits) == 0 {
				t.Error("Expected combined results, got none")
			}

			// Verify combined RSF results
			verifyRSFResults(t, combinedHits)
		})
	}
}

// TestRSFFaceting tests that facet results are identical whether using RSF or default scoring in hybrid search
func TestRSFFaceting(t *testing.T) {
	scenarios := []struct {
		name  string
		setup func(t *testing.T) (Index, func())
	}{
		{
			name:  "SingleIndex",
			setup: setupSingleIndex,
		},
		{
			name:  "AliasWithSingleIndex",
			setup: setupAliasWithSingleIndex,
		},
		{
			name:  "AliasWithTwoIndexes",
			setup: setupAliasWithTwoIndexes,
		},
		{
			name:  "NestedAliases",
			setup: setupNestedAliases,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Setup the index/alias configuration
			index, cleanup := scenario.setup(t)
			defer cleanup()

			// Create search request with default scoring and facets
			defaultRequest := createScoreFusionRequest(ScoreDefault, false)
			defaultRequest.Score = ScoreDefault // Use default scoring
			defaultRequest.Size = 10
			// Add facet for color field with size 10
			colorFacet := NewFacetRequest("color", 10)
			defaultRequest.AddFacet("color", colorFacet)

			// Create search request with RSF scoring and identical facets
			rsfRequest := createScoreFusionRequest(ScoreRSF, true)
			rsfRequest.Size = 10
			// Add identical facet for color field with size 10
			colorFacetRSF := NewFacetRequest("color", 10)
			rsfRequest.AddFacet("color", colorFacetRSF)

			// Execute both searches
			defaultResult, err := index.Search(defaultRequest)
			if err != nil {
				t.Fatalf("Default scoring search failed: %v", err)
			}

			rsfResult, err := index.Search(rsfRequest)
			if err != nil {
				t.Fatalf("RSF scoring search failed: %v", err)
			}

			// Verify both searches returned results
			if len(defaultResult.Hits) == 0 {
				t.Fatal("Expected search results with default scoring, got none")
			}
			if len(rsfResult.Hits) == 0 {
				t.Fatal("Expected search results with RSF scoring, got none")
			}

			// Verify both searches returned facets
			if defaultResult.Facets == nil {
				t.Fatal("Expected facets with default scoring, got nil")
			}
			if rsfResult.Facets == nil {
				t.Fatal("Expected facets with RSF scoring, got nil")
			}

			// Check that color facet exists in both results
			defaultColorFacet, defaultExists := defaultResult.Facets["color"]
			rsfColorFacet, rsfExists := rsfResult.Facets["color"]

			if !defaultExists {
				t.Fatal("Expected color facet in default scoring results")
			}
			if !rsfExists {
				t.Fatal("Expected color facet in RSF scoring results")
			}

			// Compare the facet results - they should be identical
			// Since facets are based on the document corpus and not scoring,
			// they should not be affected by the scoring method (even with KNN)
			if defaultColorFacet.Total != rsfColorFacet.Total {
				t.Errorf("Facet totals differ: default=%d, RSF=%d",
					defaultColorFacet.Total, rsfColorFacet.Total)
			}

			if defaultColorFacet.Missing != rsfColorFacet.Missing {
				t.Errorf("Facet missing counts differ: default=%d, RSF=%d",
					defaultColorFacet.Missing, rsfColorFacet.Missing)
			}

			if defaultColorFacet.Other != rsfColorFacet.Other {
				t.Errorf("Facet other counts differ: default=%d, RSF=%d",
					defaultColorFacet.Other, rsfColorFacet.Other)
			}

			// Compare the facet terms
			defaultTerms := defaultColorFacet.Terms.Terms()
			rsfTerms := rsfColorFacet.Terms.Terms()

			if len(defaultTerms) != len(rsfTerms) {
				t.Errorf("Facet terms count differs: default=%d, RSF=%d",
					len(defaultTerms), len(rsfTerms))
			} else {
				// Compare each term
				for i, defaultTerm := range defaultTerms {
					rsfTerm := rsfTerms[i]
					if defaultTerm.Term != rsfTerm.Term {
						t.Errorf("Facet term differs at position %d: default=%s, RSF=%s",
							i, defaultTerm.Term, rsfTerm.Term)
					}
					if defaultTerm.Count != rsfTerm.Count {
						t.Errorf("Facet term count differs for %s: default=%d, RSF=%d",
							defaultTerm.Term, defaultTerm.Count, rsfTerm.Count)
					}
				}
			}
		})
	}
}
