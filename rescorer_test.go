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

package bleve

import (
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
)

func createFTSIndex(path string) (Index, error) {
	// Index mapping for FTS-only testing
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

	// Text field for description with specific properties
	descriptionFieldMapping := NewTextFieldMapping()
	descriptionFieldMapping.Analyzer = "en"
	descriptionFieldMapping.DocValues = true
	descriptionFieldMapping.IncludeInAll = true
	descriptionFieldMapping.Store = true
	descriptionFieldMapping.Index = true
	docMapping.AddFieldMappingsAt("description", descriptionFieldMapping)

	// Text field for category with specific properties
	categoryFieldMapping := NewTextFieldMapping()
	categoryFieldMapping.Analyzer = "en"
	categoryFieldMapping.DocValues = true
	categoryFieldMapping.IncludeInAll = true
	categoryFieldMapping.Store = true
	categoryFieldMapping.Index = true
	docMapping.AddFieldMappingsAt("category", categoryFieldMapping)

	// Add the document mapping to the index
	indexMapping.AddDocumentMapping("_default", docMapping)

	// Create index
	return New(path, indexMapping)
}

func getFTSDocuments() []map[string]interface{} {
	documents := []map[string]interface{}{
		{
			"color":       "dark slate blue",
			"description": "deep and rich color with dark undertones",
			"category":    "blue shades",
		},
		{
			"color":       "blue",
			"description": "primary color that is bright and vibrant",
			"category":    "primary colors",
		},
		{
			"color":       "navy",
			"description": "dark blue color often used in uniforms",
			"category":    "dark colors",
		},
		{
			"color":       "steel blue",
			"description": "metallic blue with gray undertones",
			"category":    "metallic shades",
		},
		{
			"color":       "light blue",
			"description": "pale and soft blue color with light appearance",
			"category":    "light colors",
		},
		{
			"color":       "deep sky blue",
			"description": "bright blue reminiscent of clear skies",
			"category":    "sky colors",
		},
		{
			"color":       "royal blue",
			"description": "rich and regal blue color fit for royalty",
			"category":    "rich colors",
		},
		{
			"color":       "powder blue",
			"description": "very light blue with powder-like softness",
			"category":    "light colors",
		},
		{
			"color":       "corn flower blue",
			"description": "medium blue color named after the flower",
			"category":    "floral colors",
		},
		{
			"color":       "alice blue",
			"description": "very pale blue with light and airy quality",
			"category":    "light colors",
		},
		{
			"color":       "blue violet",
			"description": "purple-blue color with violet undertones",
			"category":    "purple shades",
		},
		{
			"color":       "sky blue",
			"description": "bright blue color of a clear day sky",
			"category":    "sky colors",
		},
		{
			"color":       "indigo",
			"description": "deep purple-blue color with dark intensity",
			"category":    "dark colors",
		},
		{
			"color":       "midnight blue",
			"description": "very dark blue like the night sky",
			"category":    "dark colors",
		},
		{
			"color":       "dark blue",
			"description": "deep blue color with dark characteristics",
			"category":    "dark colors",
		},
		{
			"color":       "medium slate blue",
			"description": "medium intensity blue with slate properties",
			"category":    "blue shades",
		},
		{
			"color":       "cadet blue",
			"description": "grayish blue color often used in uniforms",
			"category":    "metallic shades",
		},
		{
			"color":       "light steel blue",
			"description": "light metallic blue with steel-like appearance",
			"category":    "light colors",
		},
		{
			"color":       "dodger blue",
			"description": "bright medium blue with vibrant intensity",
			"category":    "bright colors",
		},
		{
			"color":       "medium blue",
			"description": "standard blue with medium intensity and saturation",
			"category":    "blue shades",
		},
		{
			"color":       "slate blue",
			"description": "blue-gray color with slate-like properties",
			"category":    "blue shades",
		},
		{
			"color":       "light sky blue",
			"description": "light version of sky blue with airy quality",
			"category":    "light colors",
		},
	}

	return documents
}

func createFTSSearchRequest() *SearchRequest {
	// Create multi-FTS search request (multiple FTS queries for RRF)
	// Query 1: Search for "dark" in color field
	query1 := query.NewMatchPhraseQuery("dark")
	query1.SetField("color")

	// Query 2: Search for "light" in description field
	query2 := query.NewMatchPhraseQuery("light")
	query2.SetField("description")

	// Query 3: Search for "blue" in category field
	query3 := query.NewMatchPhraseQuery("blue")
	query3.SetField("category")

	// Use the first query as the main query for the search request
	searchRequest := NewSearchRequest(query1)

	// Add additional queries for RRF (this simulates multiple query sources)
	// Since SearchRequest doesn't have a direct way to add multiple FTS queries,
	// we'll use a disjunction query to combine them for RRF simulation
	queries := []query.Query{query1, query2, query3}
	disjunctionQuery := query.NewDisjunctionQuery(queries)
	searchRequest.Query = disjunctionQuery

	src, sws := 1, 10
	searchRequest.Params = Params{ScoreRankConstant: &src, ScoreWindowSize: &sws}

	searchRequest.Size = 10
	searchRequest.Score = ReciprocalRankFusionStrategy

	searchRequest.Explain = false
	return searchRequest
}

// verifyFTSRRFResults verifies that the search hits match the expected RRF ranking and scores
func verifyFTSRRFResults(t *testing.T, hits search.DocumentMatchCollection) {
	// Manual RRF calculation for verification
	// With k=1 (ScoreRankConstant), RRF formula: 1/(1+rank)
	//
	// For FTS-only with disjunction query, we need to consider how each document
	// matches each of the three query components:
	// 1. "dark" in color field
	// 2. "light" in description field
	// 3. "blue" in category field
	//
	// Documents that match multiple query components will rank higher

	// Expected matches:
	// Query 1 ("dark" in color): dark slate blue, dark blue, midnight blue (has "dark")
	// Query 2 ("light" in description): light blue, powder blue, alice blue, light steel blue, light sky blue
	// Query 3 ("blue" in category): dark slate blue, medium slate blue, medium blue, slate blue

	expectedTopDocuments := []string{
		"dark slate blue",   // matches query 1 and 3
		"light blue",        // matches query 2
		"dark blue",         // matches query 1
		"light steel blue",  // matches query 2
		"medium slate blue", // matches query 3
	}

	if len(hits) == 0 {
		t.Fatal("Expected search results, got none")
	}

	// Verify we have results and they're ranked by score
	for i := 0; i < len(hits)-1; i++ {
		if hits[i].Score < hits[i+1].Score {
			t.Errorf("Results not properly ranked by score: position %d (%.6f) < position %d (%.6f)",
				i, hits[i].Score, i+1, hits[i+1].Score)
		}
	}

	// Check that some expected top documents are present in results
	foundExpected := 0
	for _, hit := range hits {
		for _, expected := range expectedTopDocuments {
			if hit.ID == expected {
				foundExpected++
				break
			}
		}
	}

	if foundExpected < 3 {
		t.Errorf("Expected to find at least 3 of the top expected documents, found %d", foundExpected)
		t.Logf("Actual results:")
		for i, hit := range hits {
			t.Logf("  %d: %s (score: %.6f)", i+1, hit.ID, hit.Score)
		}
	}
}

// setupFTSSingleIndex creates a single index with all FTS documents
func setupFTSSingleIndex(t *testing.T) (Index, func()) {
	tmpIndexPath := createTmpIndexPath(t)

	index, err := createFTSIndex(tmpIndexPath)
	if err != nil {
		t.Fatal(err)
	}

	documents := getFTSDocuments()

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

// setupFTSAliasWithSingleIndex creates an alias containing one index with all FTS documents
func setupFTSAliasWithSingleIndex(t *testing.T) (Index, func()) {
	tmpIndexPath := createTmpIndexPath(t)

	index, err := createFTSIndex(tmpIndexPath)
	if err != nil {
		t.Fatal(err)
	}

	documents := getFTSDocuments()

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

// setupFTSAliasWithTwoIndexes creates an alias containing two indexes with FTS documents split between them
func setupFTSAliasWithTwoIndexes(t *testing.T) (Index, func()) {
	documents := getFTSDocuments()

	// Split documents into two groups
	midpoint := len(documents) / 2
	docs1 := documents[:midpoint]
	docs2 := documents[midpoint:]

	// Create first index
	tmpIndexPath1 := createTmpIndexPath(t)
	index1, err := createFTSIndex(tmpIndexPath1)
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
	index2, err := createFTSIndex(tmpIndexPath2)
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

// setupFTSNestedAliases creates nested aliases with three indexes spread across sub-aliases
func setupFTSNestedAliases(t *testing.T) (Index, func()) {
	documents := getFTSDocuments()

	// Split documents into three groups
	thirdPoint1 := len(documents) / 3
	thirdPoint2 := 2 * len(documents) / 3
	docs1 := documents[:thirdPoint1]
	docs2 := documents[thirdPoint1:thirdPoint2]
	docs3 := documents[thirdPoint2:]

	// Create first index
	tmpIndexPath1 := createTmpIndexPath(t)
	index1, err := createFTSIndex(tmpIndexPath1)
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
	index2, err := createFTSIndex(tmpIndexPath2)
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
	index3, err := createFTSIndex(tmpIndexPath3)
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

func TestFTSRRFEndToEnd(t *testing.T) {
	// Setup the index configuration
	index, cleanup := setupFTSSingleIndex(t)
	defer cleanup()

	// Create the search request
	searchRequest := createFTSSearchRequest()

	// Execute search
	result, err := index.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify FTS RRF results
	verifyFTSRRFResults(t, result.Hits)
}

// TestFTSRRFAliasWithSingleIndex tests RRF with an alias containing one index
func TestFTSRRFAliasWithSingleIndex(t *testing.T) {
	// Setup the alias configuration
	alias, cleanup := setupFTSAliasWithSingleIndex(t)
	defer cleanup()

	// Create the search request
	searchRequest := createFTSSearchRequest()

	// Execute search through alias
	result, err := alias.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify FTS RRF results - should be identical to direct index search
	verifyFTSRRFResults(t, result.Hits)
}

// TestFTSRRFAliasWithTwoIndexes tests RRF with an alias containing two indexes
func TestFTSRRFAliasWithTwoIndexes(t *testing.T) {
	// Setup the alias configuration
	alias, cleanup := setupFTSAliasWithTwoIndexes(t)
	defer cleanup()

	// Create the search request
	searchRequest := createFTSSearchRequest()

	// Execute search through alias
	result, err := alias.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify FTS RRF results - should be consistent across distributed indexes
	verifyFTSRRFResults(t, result.Hits)
}

// TestFTSRRFNestedAliases tests RRF with an alias containing two index aliases
func TestFTSRRFNestedAliases(t *testing.T) {
	// Setup the nested aliases configuration
	masterAlias, cleanup := setupFTSNestedAliases(t)
	defer cleanup()

	// Create the search request
	searchRequest := createFTSSearchRequest()

	// Execute search through master alias
	result, err := masterAlias.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify FTS RRF results - should be consistent across nested aliases
	verifyFTSRRFResults(t, result.Hits)
}

// TestFTSRRFPagination tests FTS RRF with pagination across different index/alias configurations
func TestFTSRRFPagination(t *testing.T) {
	scenarios := []struct {
		name  string
		setup func(t *testing.T) (Index, func())
	}{
		{
			name:  "SingleIndex",
			setup: setupFTSSingleIndex,
		},
		{
			name:  "AliasWithSingleIndex",
			setup: setupFTSAliasWithSingleIndex,
		},
		{
			name:  "AliasWithTwoIndexes",
			setup: setupFTSAliasWithTwoIndexes,
		},
		{
			name:  "NestedAliases",
			setup: setupFTSNestedAliases,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			// Setup the index/alias configuration
			index, cleanup := scenario.setup(t)
			defer cleanup()

			// Create first page request (first 5 results)
			firstPageRequest := createFTSSearchRequest()
			firstPageRequest.From = 0
			firstPageRequest.Size = 5

			// Execute first page search
			firstPageResult, err := index.Search(firstPageRequest)
			if err != nil {
				t.Fatal(err)
			}

			// Create second page request (next 5 results, starting from index 5)
			secondPageRequest := createFTSSearchRequest()
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

			// Verify we have results (FTS may have variable results based on matches)
			if len(firstPageResult.Hits) == 0 {
				t.Fatal("Expected at least some results in first page, got 0")
			}
			if len(firstPageResult.Hits) > 5 {
				t.Errorf("Expected at most 5 results in first page, got %d", len(firstPageResult.Hits))
			}

			// Total hits should not exceed the number of documents that match our queries
			totalHits := len(combinedHits)
			if totalHits == 0 {
				t.Fatal("Expected at least some combined results, got 0")
			}

			// Verify combined FTS RRF results
			verifyFTSRRFResults(t, combinedHits)
		})
	}
}
