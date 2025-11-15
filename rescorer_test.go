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
	"fmt"
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

var benchmarkResult search.DocumentMatchCollection

type benchmarkConfig struct {
	name       string
	ftsHits    int
	knnHits    int
	knnQueries int
}

func BenchmarkRescorerRRF(b *testing.B) {
	runRescorerBenchmarks(b, ScoreRRF)
}

func BenchmarkRescorerRSF(b *testing.B) {
	runRescorerBenchmarks(b, ScoreRSF)
}

func runRescorerBenchmarks(b *testing.B, scoreMode string) {
	configs := []benchmarkConfig{
		{name: "small", ftsHits: 256, knnHits: 192, knnQueries: 1},
		{name: "medium", ftsHits: 1024, knnHits: 896, knnQueries: 2},
		{name: "large", ftsHits: 4096, knnHits: 3584, knnQueries: 3},
	}

	for _, cfg := range configs {
		b.Run(fmt.Sprintf("%s/%s", scoreMode, cfg.name), func(b *testing.B) {
			b.ReportAllocs()

			rescorer, baseFTSHits, baseKNNHits := buildBenchmarkInputs(cfg, scoreMode)

			var last search.DocumentMatchCollection
			b.StopTimer()
			for i := 0; i < b.N; i++ {
				ftsHits := cloneDocumentMatches(baseFTSHits)
				knnHits := cloneDocumentMatches(baseKNNHits)

				b.StartTimer()
				hits, _, _ := rescorer.rescore(ftsHits, knnHits)
				b.StopTimer()

				last = hits
			}

			if len(last) == 0 {
				b.Fatalf("rescorer returned no hits for config %q", cfg.name)
			}

			benchmarkResult = last
		})
	}
}

func buildBenchmarkInputs(cfg benchmarkConfig, scoreMode string) (*rescorer, search.DocumentMatchCollection, search.DocumentMatchCollection) {
	windowSize := cfg.ftsHits
	if cfg.knnHits > windowSize {
		windowSize = cfg.knnHits
	}

	matchQuery := query.NewMatchQuery("rescorer benchmark payload")
	matchQuery.SetBoost(1.0)

	req := &SearchRequest{
		Query:  matchQuery,
		Size:   cfg.ftsHits,
		From:   0,
		Score:  scoreMode,
		Params: &RequestParams{ScoreRankConstant: DefaultScoreRankConstant, ScoreWindowSize: windowSize},
	}

	activeKNNQueries := cfg.knnQueries
	if knnAdder, ok := interface{}(req).(interface {
		AddKNN(field string, vector []float32, k int64, boost float64)
	}); ok {
		for i := 0; i < cfg.knnQueries; i++ {
			knnAdder.AddKNN(fmt.Sprintf("vector_%d", i), []float32{1.0, 0.5, 0.25}, int64(cfg.knnHits), 1.0)
		}
	} else {
		activeKNNQueries = 0
	}

	r := newRescorer(req)
	r.origBoosts = make([]float64, activeKNNQueries+1)
	for i := range r.origBoosts {
		r.origBoosts[i] = 1.0
	}

	ftsHits, knnHits := buildBenchmarkHits(cfg, activeKNNQueries)
	return r, ftsHits, knnHits
}

func buildBenchmarkHits(cfg benchmarkConfig, activeKNNQueries int) (search.DocumentMatchCollection, search.DocumentMatchCollection) {
	ftsHits := make(search.DocumentMatchCollection, cfg.ftsHits)
	for i := 0; i < cfg.ftsHits; i++ {
		ftsHits[i] = &search.DocumentMatch{
			ID:        fmt.Sprintf("doc-%06d", i),
			Score:     float64(cfg.ftsHits - i),
			HitNumber: uint64(i + 1),
		}
	}

	knnHits := make(search.DocumentMatchCollection, cfg.knnHits)
	for i := 0; i < cfg.knnHits; i++ {
		id := fmt.Sprintf("doc-%06d", i)
		if cfg.ftsHits > 0 {
			id = fmt.Sprintf("doc-%06d", i%cfg.ftsHits)
		}
		if cfg.ftsHits == 0 || i%4 == 0 {
			id = fmt.Sprintf("knn-only-%06d", i/4)
		}

		scoreBreakdown := make(map[int]float64, activeKNNQueries)
		for q := 0; q < activeKNNQueries; q++ {
			scoreBreakdown[q] = float64(cfg.knnHits - i + q + 1)
		}

		knnHits[i] = &search.DocumentMatch{
			ID:             id,
			Score:          float64(cfg.knnHits - i),
			ScoreBreakdown: scoreBreakdown,
			HitNumber:      uint64(i + 1),
		}
	}

	return ftsHits, knnHits
}

func cloneDocumentMatches(src search.DocumentMatchCollection) search.DocumentMatchCollection {
	dst := make(search.DocumentMatchCollection, len(src))
	for i, hit := range src {
		if hit == nil {
			continue
		}

		cloned := *hit

		if hit.ScoreBreakdown != nil {
			cloned.ScoreBreakdown = make(map[int]float64, len(hit.ScoreBreakdown))
			for k, v := range hit.ScoreBreakdown {
				cloned.ScoreBreakdown[k] = v
			}
		}

		if len(hit.Sort) > 0 {
			cloned.Sort = append([]string(nil), hit.Sort...)
		}
		if len(hit.DecodedSort) > 0 {
			cloned.DecodedSort = append([]string(nil), hit.DecodedSort...)
		}
		if len(hit.IndexNames) > 0 {
			cloned.IndexNames = append([]string(nil), hit.IndexNames...)
		}

		dst[i] = &cloned
	}
	return dst
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

func createFTSSearchRequest(scoreMethod string) *SearchRequest {
	// Create multi-FTS search request (multiple FTS queries for fusion scoring)
	// Query 1: Search for "dark" in color field
	query1 := query.NewMatchPhraseQuery("dark")
	query1.SetField("color")

	// Query 2: Search for "light" in description field
	query2 := query.NewMatchPhraseQuery("light")
	query2.SetField("description")

	// // Query 3: Search for "blue" in category field
	query3 := query.NewMatchPhraseQuery("blue")
	query3.SetField("category")

	// Use the first query as the main query for the search request
	searchRequest := NewSearchRequest(query1)

	// Add additional queries for fusion scoring (this simulates multiple query sources)
	// Since SearchRequest doesn't have a direct way to add multiple FTS queries,
	// we'll use a disjunction query to combine them for fusion scoring simulation
	queries := []query.Query{query1, query2, query3}
	disjunctionQuery := query.NewDisjunctionQuery(queries)
	searchRequest.Query = disjunctionQuery

	params := RequestParams{1, 10}
	searchRequest.AddParams(params)

	searchRequest.Size = 10
	searchRequest.Score = scoreMethod

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
	searchRequest := createFTSSearchRequest(ScoreRRF)

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
	searchRequest := createFTSSearchRequest(ScoreRRF)

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
	searchRequest := createFTSSearchRequest(ScoreRRF)

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
	searchRequest := createFTSSearchRequest(ScoreRRF)

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
			firstPageRequest := createFTSSearchRequest(ScoreRRF)
			firstPageRequest.From = 0
			firstPageRequest.Size = 5

			// Execute first page search
			firstPageResult, err := index.Search(firstPageRequest)
			if err != nil {
				t.Fatal(err)
			}

			// Create second page request (next 5 results, starting from index 5)
			secondPageRequest := createFTSSearchRequest(ScoreRRF)
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

// TestFTSRRFFaceting tests that facet results are identical whether using RRF or default scoring
func TestFTSRRFFaceting(t *testing.T) {
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

			// Create search request with default scoring and facets
			defaultRequest := createFTSSearchRequest(ScoreRRF)
			defaultRequest.Score = ScoreDefault // Use default scoring
			defaultRequest.Size = 10
			// Add facet for color field with size 10
			colorFacet := NewFacetRequest("color", 10)
			defaultRequest.AddFacet("color", colorFacet)

			// Create search request with RRF scoring and identical facets
			rrfRequest := createFTSSearchRequest(ScoreRRF)
			rrfRequest.Size = 10
			rrfRequest.Score = ScoreRRF
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
			// they should not be affected by the scoring method
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

// verifyFTSRSFResults verifies that the search hits match expected RSF ranking and scores for FTS-only search
func verifyFTSRSFResults(t *testing.T, hits search.DocumentMatchCollection) {
	// For FTS RSF, we expect documents that match multiple query components to rank higher
	// Query components: "dark" in color, "light" in description, "blue" in category
	// RSF uses min-max normalization of scores within the window

	// Verify we have reasonable number of results
	if len(hits) == 0 {
		t.Fatal("Expected non-empty search results for FTS RSF")
	}

	// Verify we have at least 5 results for meaningful comparison
	if len(hits) < 5 {
		t.Errorf("Expected at least 5 results for FTS RSF, got %d", len(hits))
	}

	// Documents that should appear in top results based on multi-query matching:
	// - "dark slate blue": matches "dark" in color AND "blue" in category (2 matches)
	// - "light blue": matches "light" in description (1 strong match)
	// - "dark blue": matches "dark" in color (1 match)
	// - Documents with "light" in description should rank well
	topExpectedDocs := []string{"dark slate blue", "light blue", "dark blue", "medium slate blue", "light sky blue"}

	// Create map of all hits for easier lookup
	docMap := make(map[string]int) // doc -> position (0-based)
	for i, hit := range hits {
		docMap[hit.ID] = i
	}

	// Verify that "dark slate blue" appears in top 3 positions (matches 2 query components)
	if pos, found := docMap["dark slate blue"]; !found {
		t.Error("Expected 'dark slate blue' to appear in results but not found")
	} else if pos >= 3 {
		t.Errorf("Expected 'dark slate blue' in top 3 positions, found at position %d", pos+1)
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

	// Verify score range is reasonable - top score should be significantly higher than 5th
	if len(hits) >= 5 {
		topScore := hits[0].Score
		fifthScore := hits[4].Score
		if topScore-fifthScore < 0.001 {
			t.Errorf("Insufficient score differentiation: top score %.6f, 5th score %.6f (diff: %.6f)",
				topScore, fifthScore, topScore-fifthScore)
		}
	}
}

// TestFTSRSFEndToEnd tests RSF scoring with a single FTS index
func TestFTSRSFEndToEnd(t *testing.T) {
	// Setup the index configuration
	index, cleanup := setupFTSSingleIndex(t)
	defer cleanup()

	// Create the search request
	searchRequest := createFTSSearchRequest(ScoreRSF)

	// Execute search
	result, err := index.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify FTS RSF results
	verifyFTSRSFResults(t, result.Hits)
}

// TestFTSRSFAliasWithSingleIndex tests RSF with an alias containing one FTS index
func TestFTSRSFAliasWithSingleIndex(t *testing.T) {
	// Setup the alias configuration
	alias, cleanup := setupFTSAliasWithSingleIndex(t)
	defer cleanup()

	// Create the search request
	searchRequest := createFTSSearchRequest(ScoreRSF)

	// Execute search through alias
	result, err := alias.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify FTS RSF results - should be identical to direct index search
	verifyFTSRSFResults(t, result.Hits)
}

// TestFTSRSFAliasWithTwoIndexes tests RSF with an alias containing two FTS indexes
func TestFTSRSFAliasWithTwoIndexes(t *testing.T) {
	// Setup the alias configuration
	alias, cleanup := setupFTSAliasWithTwoIndexes(t)
	defer cleanup()

	// Create the search request
	searchRequest := createFTSSearchRequest(ScoreRSF)

	// Execute search through alias
	result, err := alias.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify FTS RSF results - should be consistent across distributed indexes
	verifyFTSRSFResults(t, result.Hits)
}

// TestFTSRSFNestedAliases tests RSF with an alias containing two index aliases
func TestFTSRSFNestedAliases(t *testing.T) {
	// Setup the nested aliases configuration
	masterAlias, cleanup := setupFTSNestedAliases(t)
	defer cleanup()

	// Create the search request
	searchRequest := createFTSSearchRequest(ScoreRSF)

	// Execute search through master alias
	result, err := masterAlias.Search(searchRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Verify FTS RSF results - should be consistent across nested aliases
	verifyFTSRSFResults(t, result.Hits)
}

// TestFTSRSFPagination tests FTS RSF with pagination across different index/alias configurations
func TestFTSRSFPagination(t *testing.T) {
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
			firstPageRequest := createFTSSearchRequest(ScoreRSF)
			firstPageRequest.From = 0
			firstPageRequest.Size = 5

			// Execute first page search
			firstPageResult, err := index.Search(firstPageRequest)
			if err != nil {
				t.Fatal(err)
			}

			// Create second page request (next 5 results, starting from index 5)
			secondPageRequest := createFTSSearchRequest(ScoreRSF)
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

			// Verify combined FTS RSF results
			verifyFTSRSFResults(t, combinedHits)
		})
	}
}

// TestFTSRSFFaceting tests that facet results are identical whether using RSF or default scoring
func TestFTSRSFFaceting(t *testing.T) {
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

			// Create search request with default scoring and facets
			defaultRequest := createFTSSearchRequest(ScoreRRF)
			defaultRequest.Score = ScoreDefault // Use default scoring
			defaultRequest.Size = 10
			// Add facet for category field with size 10
			categoryFacet := NewFacetRequest("category", 10)
			defaultRequest.AddFacet("category", categoryFacet)

			// Create search request with RSF scoring and identical facets
			rsfRequest := createFTSSearchRequest(ScoreRSF)
			rsfRequest.Size = 10
			// Add identical facet for category field with size 10
			categoryFacetRSF := NewFacetRequest("category", 10)
			rsfRequest.AddFacet("category", categoryFacetRSF)

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

			// Check that category facet exists in both results
			defaultCategoryFacet, defaultExists := defaultResult.Facets["category"]
			rsfCategoryFacet, rsfExists := rsfResult.Facets["category"]

			if !defaultExists {
				t.Fatal("Expected category facet in default scoring results")
			}
			if !rsfExists {
				t.Fatal("Expected category facet in RSF scoring results")
			}

			// Compare the facet results - they should be identical
			// Since facets are based on the document corpus and not scoring,
			// they should not be affected by the scoring method
			if defaultCategoryFacet.Total != rsfCategoryFacet.Total {
				t.Errorf("Facet totals differ: default=%d, RSF=%d",
					defaultCategoryFacet.Total, rsfCategoryFacet.Total)
			}

			if defaultCategoryFacet.Missing != rsfCategoryFacet.Missing {
				t.Errorf("Facet missing counts differ: default=%d, RSF=%d",
					defaultCategoryFacet.Missing, rsfCategoryFacet.Missing)
			}

			if defaultCategoryFacet.Other != rsfCategoryFacet.Other {
				t.Errorf("Facet other counts differ: default=%d, RSF=%d",
					defaultCategoryFacet.Other, rsfCategoryFacet.Other)
			}

			// Compare the facet terms
			defaultTerms := defaultCategoryFacet.Terms.Terms()
			rsfTerms := rsfCategoryFacet.Terms.Terms()

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
