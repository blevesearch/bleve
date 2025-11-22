//  Copyright (c) 2014 Couchbase, Inc.
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

package collector

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/facet"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

func TestTop10Scores(t *testing.T) {
	// a stub search with more than 10 matches
	// the top-10 scores are > 10
	// everything else is less than 10
	searcher := &stubSearcher{
		matches: []*search.DocumentMatch{
			{
				IndexInternalID: index.IndexInternalID("a"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("b"),
				Score:           9,
			},
			{
				IndexInternalID: index.IndexInternalID("c"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("d"),
				Score:           9,
			},
			{
				IndexInternalID: index.IndexInternalID("e"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("f"),
				Score:           9,
			},
			{
				IndexInternalID: index.IndexInternalID("g"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("h"),
				Score:           9,
			},
			{
				IndexInternalID: index.IndexInternalID("i"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("j"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("k"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("l"),
				Score:           99,
			},
			{
				IndexInternalID: index.IndexInternalID("m"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("n"),
				Score:           11,
			},
		},
	}

	collector := NewTopNCollector(10, 0, search.SortOrder{&search.SortScore{Desc: true}})
	err := collector.Collect(context.Background(), searcher, &stubReader{})
	if err != nil {
		t.Fatal(err)
	}

	maxScore := collector.MaxScore()
	if maxScore != 99.0 {
		t.Errorf("expected max score 99.0, got %f", maxScore)
	}

	total := collector.Total()
	if total != 14 {
		t.Errorf("expected 14 total results, got %d", total)
	}

	results := collector.Results()

	if len(results) != 10 {
		t.Logf("results: %v", results)
		t.Fatalf("expected 10 results, got %d", len(results))
	}

	if results[0].ID != "l" {
		t.Errorf("expected first result to have ID 'l', got %s", results[0].ID)
	}

	if results[0].Score != 99.0 {
		t.Errorf("expected highest score to be 99.0, got %f", results[0].Score)
	}

	minScore := 1000.0
	for _, result := range results {
		if result.Score < minScore {
			minScore = result.Score
		}
	}

	if minScore < 10 {
		t.Errorf("expected minimum score to be higher than 10, got %f", minScore)
	}
}

func TestTop10ScoresSkip10(t *testing.T) {
	// a stub search with more than 10 matches
	// the top-10 scores are > 10
	// everything else is less than 10
	searcher := &stubSearcher{
		matches: []*search.DocumentMatch{
			{
				IndexInternalID: index.IndexInternalID("a"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("b"),
				Score:           9.5,
			},
			{
				IndexInternalID: index.IndexInternalID("c"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("d"),
				Score:           9,
			},
			{
				IndexInternalID: index.IndexInternalID("e"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("f"),
				Score:           9,
			},
			{
				IndexInternalID: index.IndexInternalID("g"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("h"),
				Score:           9,
			},
			{
				IndexInternalID: index.IndexInternalID("i"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("j"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("k"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("l"),
				Score:           99,
			},
			{
				IndexInternalID: index.IndexInternalID("m"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("n"),
				Score:           11,
			},
		},
	}

	collector := NewTopNCollector(10, 10, search.SortOrder{&search.SortScore{Desc: true}})
	err := collector.Collect(context.Background(), searcher, &stubReader{})
	if err != nil {
		t.Fatal(err)
	}

	maxScore := collector.MaxScore()
	if maxScore != 99.0 {
		t.Errorf("expected max score 99.0, got %f", maxScore)
	}

	total := collector.Total()
	if total != 14 {
		t.Errorf("expected 14 total results, got %d", total)
	}

	results := collector.Results()

	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}

	if results[0].ID != "b" {
		t.Errorf("expected first result to have ID 'b', got %s", results[0].ID)
	}

	if results[0].Score != 9.5 {
		t.Errorf("expected highest score to be 9.5, got %f", results[0].Score)
	}
}

func TestTop10ScoresSkip10Only9Hits(t *testing.T) {
	// a stub search with only 10 matches
	searcher := &stubSearcher{
		matches: []*search.DocumentMatch{
			{
				IndexInternalID: index.IndexInternalID("a"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("c"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("e"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("g"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("i"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("j"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("k"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("m"),
				Score:           11,
			},
			{
				IndexInternalID: index.IndexInternalID("n"),
				Score:           11,
			},
		},
	}

	collector := NewTopNCollector(10, 10, search.SortOrder{&search.SortScore{Desc: true}})
	err := collector.Collect(context.Background(), searcher, &stubReader{})
	if err != nil {
		t.Fatal(err)
	}

	total := collector.Total()
	if total != 9 {
		t.Errorf("expected 9 total results, got %d", total)
	}

	results := collector.Results()

	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

func TestPaginationSameScores(t *testing.T) {
	// a stub search with more than 10 matches
	// all documents have the same score
	searcher := &stubSearcher{
		matches: []*search.DocumentMatch{
			{
				IndexInternalID: index.IndexInternalID("a"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("b"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("c"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("d"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("e"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("f"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("g"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("h"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("i"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("j"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("k"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("l"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("m"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("n"),
				Score:           5,
			},
		},
	}

	// first get first 5 hits
	collector := NewTopNCollector(5, 0, search.SortOrder{&search.SortScore{Desc: true}})
	err := collector.Collect(context.Background(), searcher, &stubReader{})
	if err != nil {
		t.Fatal(err)
	}

	total := collector.Total()
	if total != 14 {
		t.Errorf("expected 14 total results, got %d", total)
	}

	results := collector.Results()

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}

	firstResults := make(map[string]struct{})
	for _, hit := range results {
		firstResults[hit.ID] = struct{}{}
	}

	// a stub search with more than 10 matches
	// all documents have the same score
	searcher = &stubSearcher{
		matches: []*search.DocumentMatch{
			{
				IndexInternalID: index.IndexInternalID("a"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("b"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("c"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("d"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("e"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("f"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("g"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("h"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("i"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("j"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("k"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("l"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("m"),
				Score:           5,
			},
			{
				IndexInternalID: index.IndexInternalID("n"),
				Score:           5,
			},
		},
	}

	// now get next 5 hits
	collector = NewTopNCollector(5, 5, search.SortOrder{&search.SortScore{Desc: true}})
	err = collector.Collect(context.Background(), searcher, &stubReader{})
	if err != nil {
		t.Fatal(err)
	}

	total = collector.Total()
	if total != 14 {
		t.Errorf("expected 14 total results, got %d", total)
	}

	results = collector.Results()

	if len(results) != 5 {
		t.Fatalf("expected 5 results, got %d", len(results))
	}

	// make sure that none of these hits repeat ones we saw in the top 5
	for _, hit := range results {
		if _, ok := firstResults[hit.ID]; ok {
			t.Errorf("doc ID %s is in top 5 and next 5 result sets", hit.ID)
		}
	}
}

// TestStreamResults verifies the search.DocumentMatchHandler
func TestStreamResults(t *testing.T) {
	matches := []*search.DocumentMatch{
		{
			IndexInternalID: index.IndexInternalID("a"),
			Score:           11,
		},
		{
			IndexInternalID: index.IndexInternalID("b"),
			Score:           1,
		},
		{
			IndexInternalID: index.IndexInternalID("c"),
			Score:           11,
		},
		{
			IndexInternalID: index.IndexInternalID("d"),
			Score:           999,
		},
		{
			IndexInternalID: index.IndexInternalID("e"),
			Score:           11,
		},
		{
			IndexInternalID: index.IndexInternalID("f"),
			Score:           9,
		},
		{
			IndexInternalID: index.IndexInternalID("g"),
			Score:           11,
		},
		{
			IndexInternalID: index.IndexInternalID("h"),
			Score:           89,
		},
		{
			IndexInternalID: index.IndexInternalID("i"),
			Score:           101,
		},
		{
			IndexInternalID: index.IndexInternalID("j"),
			Score:           112,
		},
		{
			IndexInternalID: index.IndexInternalID("k"),
			Score:           10,
		},
		{
			IndexInternalID: index.IndexInternalID("l"),
			Score:           99,
		},
		{
			IndexInternalID: index.IndexInternalID("m"),
			Score:           11,
		},
		{
			IndexInternalID: index.IndexInternalID("n"),
			Score:           111,
		},
	}

	searcher := &stubSearcher{
		matches: matches,
	}
	ind := 0
	docMatchHandler := func(hit *search.DocumentMatch) error {
		if hit == nil {
			return nil // search completed
		}
		if !bytes.Equal(hit.IndexInternalID, matches[ind].IndexInternalID) {
			t.Errorf("%d hit IndexInternalID actual: %s, expected: %s",
				ind, hit.IndexInternalID, matches[ind].IndexInternalID)
		}
		if hit.Score != matches[ind].Score {
			t.Errorf("%d hit Score actual: %s, expected: %s",
				ind, hit.IndexInternalID, matches[ind].IndexInternalID)
		}
		ind++
		return nil
	}

	var handlerMaker search.MakeDocumentMatchHandler = func(ctx *search.SearchContext) (search.DocumentMatchHandler, bool, error) {
		return docMatchHandler, false, nil
	}

	ctx := context.WithValue(context.Background(), search.MakeDocumentMatchHandlerKey, handlerMaker)

	collector := NewTopNCollector(10, 0, search.SortOrder{&search.SortScore{Desc: true}})
	err := collector.Collect(ctx, searcher, &stubReader{})
	if err != nil {
		t.Fatal(err)
	}

	maxScore := collector.MaxScore()
	if maxScore != 999.0 {
		t.Errorf("expected max score 99.0, got %f", maxScore)
	}

	total := collector.Total()
	if int(total) != ind {
		t.Errorf("expected 14 total results, got %d", total)
	}

	results := collector.Results()

	if len(results) != 0 {
		t.Fatalf("expected 0 results, got %d", len(results))
	}
}

// TestCollectorChaining verifies the chaining of collectors.
// The custom DocumentMatchHandler can process every hit for
// the search query and then pass the hit to the topn collector
// to eventually have the sorted top `N` results.
func TestCollectorChaining(t *testing.T) {
	matches := []*search.DocumentMatch{
		{
			IndexInternalID: index.IndexInternalID("a"),
			Score:           11,
		},
		{
			IndexInternalID: index.IndexInternalID("b"),
			Score:           1,
		},
		{
			IndexInternalID: index.IndexInternalID("c"),
			Score:           11,
		},
		{
			IndexInternalID: index.IndexInternalID("d"),
			Score:           999,
		},
		{
			IndexInternalID: index.IndexInternalID("e"),
			Score:           11,
		},
		{
			IndexInternalID: index.IndexInternalID("f"),
			Score:           9,
		},
		{
			IndexInternalID: index.IndexInternalID("g"),
			Score:           11,
		},
		{
			IndexInternalID: index.IndexInternalID("h"),
			Score:           89,
		},
		{
			IndexInternalID: index.IndexInternalID("i"),
			Score:           101,
		},
		{
			IndexInternalID: index.IndexInternalID("j"),
			Score:           112,
		},
		{
			IndexInternalID: index.IndexInternalID("k"),
			Score:           10,
		},
		{
			IndexInternalID: index.IndexInternalID("l"),
			Score:           99,
		},
		{
			IndexInternalID: index.IndexInternalID("m"),
			Score:           11,
		},
		{
			IndexInternalID: index.IndexInternalID("n"),
			Score:           111,
		},
	}

	searcher := &stubSearcher{
		matches: matches,
	}

	var topNHandler search.DocumentMatchHandler
	ind := 0
	docMatchHandler := func(hit *search.DocumentMatch) error {
		if hit == nil {
			return nil // search completed
		}
		if !bytes.Equal(hit.IndexInternalID, matches[ind].IndexInternalID) {
			t.Errorf("%d hit IndexInternalID actual: %s, expected: %s",
				ind, hit.IndexInternalID, matches[ind].IndexInternalID)
		}
		if hit.Score != matches[ind].Score {
			t.Errorf("%d hit Score actual: %s, expected: %s",
				ind, hit.IndexInternalID, matches[ind].IndexInternalID)
		}
		ind++
		// give the hit back to the topN collector
		err := topNHandler(hit)
		if err != nil {
			t.Errorf("unexpected err: %v", err)
		}
		return nil
	}

	var handlerMaker search.MakeDocumentMatchHandler = func(ctx *search.SearchContext) (search.DocumentMatchHandler, bool, error) {
		topNHandler, _, _ = MakeTopNDocumentMatchHandler(ctx)
		return docMatchHandler, false, nil
	}

	ctx := context.WithValue(context.Background(), search.MakeDocumentMatchHandlerKey,
		handlerMaker)

	collector := NewTopNCollector(10, 0, search.SortOrder{&search.SortScore{Desc: true}})
	err := collector.Collect(ctx, searcher, &stubReader{})
	if err != nil {
		t.Fatal(err)
	}

	maxScore := collector.MaxScore()
	if maxScore != 999.0 {
		t.Errorf("expected max score 99.0, got %f", maxScore)
	}

	total := collector.Total()
	if int(total) != ind {
		t.Errorf("expected 14 total results, got %d", total)
	}

	results := collector.Results()

	if len(results) != 10 { // as it is paged
		t.Fatalf("expected 0 results, got %d", len(results))
	}

	if results[0].ID != "d" {
		t.Errorf("expected first result to have ID 'l', got %s", results[0].ID)
	}

	if results[0].Score != 999.0 {
		t.Errorf("expected highest score to be 999.0, got %f", results[0].Score)
	}

	minScore := 1000.0
	for _, result := range results {
		if result.Score < minScore {
			minScore = result.Score
		}
	}

	if minScore < 10 {
		t.Errorf("expected minimum score to be higher than 10, got %f", minScore)
	}
}

func setupIndex(t *testing.T) index.Index {
	analysisQueue := index.NewAnalysisQueue(1)
	i, err := scorch.NewScorch(
		scorch.Name,
		map[string]interface{}{
			"path": "",
		},
		analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = i.Open()
	if err != nil {
		t.Fatal(err)
	}

	return i
}

func TestSetFacetsBuilder(t *testing.T) {
	// Field common to both sorting and faceting.
	sortFacetsField := "locations"

	coll := NewTopNCollector(10, 0, search.SortOrder{&search.SortField{Field: sortFacetsField}})

	i := setupIndex(t)
	indexReader, err := i.Reader()
	if err != nil {
		t.Fatal(err)
	}

	fb := search.NewFacetsBuilder(indexReader)
	facetBuilder := facet.NewTermsFacetBuilder(sortFacetsField, 100)
	fb.Add("locations_facet", facetBuilder)
	coll.SetFacetsBuilder(fb)

	// Should not duplicate the "locations" field in the collector.
	if len(coll.neededFields) != 1 || coll.neededFields[0] != sortFacetsField {
		t.Errorf("expected fields in collector: %v, observed: %v", []string{sortFacetsField}, coll.neededFields)
	}
}

func TestSearchAfterNumeric(t *testing.T) {
	idx := setupIndex(t)
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docs := []struct {
		id   string
		data int64
	}{
		{"a", 10},
		{"b", 9},
		{"c", 8},
		{"d", 7},
		{"e", 6},
		{"f", 5},
		{"g", 4},
		{"h", 3},
		{"i", 2},
		{"j", 1},
	}

	batch := index.NewBatch()
	for _, d := range docs {
		doc := document.NewDocument(d.id)
		field := document.NewNumericFieldWithIndexingOptions("data", []uint64{}, float64(d.data), index.IndexField|index.StoreField|index.IncludeTermVectors)
		doc.AddField(field)
		batch.Update(doc)
	}

	err := idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	searcher, err := searcher.NewMatchAllSearcher(context.Background(), reader, 1.0, search.SearcherOptions{})
	if err != nil {
		t.Fatal(err)
	}

	sortOrder := search.SortOrder{&search.SortField{Field: "data", Type: search.SortFieldAsNumber, Desc: true}}

	after := []string{"6"}

	collectorAfter := NewTopNCollectorAfter(5, sortOrder, after)
	err = collectorAfter.Collect(context.Background(), searcher, reader)
	if err != nil {
		t.Fatal(err)
	}

	resultsAfter := collectorAfter.Results()
	if len(resultsAfter) != 5 {
		t.Fatalf("expected 5 results, got %d", len(resultsAfter))
	}
	for i := range resultsAfter {
		raID := resultsAfter[i].ID
		docID := docs[i+len(resultsAfter)].id
		if raID != docID {
			t.Errorf("expected result '%s', got '%s'", docID, raID)
		}
	}
}

func TestSearchAfterDateTime(t *testing.T) {
	idx := setupIndex(t)
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docs := []struct {
		id   string
		data time.Time
	}{
		{"a", time.Unix(10, 0).UTC()},
		{"b", time.Unix(9, 0).UTC()},
		{"c", time.Unix(8, 0).UTC()},
		{"d", time.Unix(7, 0).UTC()},
		{"e", time.Unix(6, 0).UTC()},
		{"f", time.Unix(5, 0).UTC()},
		{"g", time.Unix(4, 0).UTC()},
		{"h", time.Unix(3, 0).UTC()},
		{"i", time.Unix(2, 0).UTC()},
		{"j", time.Unix(1, 0).UTC()},
	}

	batch := index.NewBatch()
	for _, d := range docs {
		doc := document.NewDocument(d.id)
		field, err := document.NewDateTimeFieldWithIndexingOptions("data", []uint64{}, d.data, time.RFC3339Nano, index.IndexField|index.StoreField|index.IncludeTermVectors)
		if err != nil {
			t.Fatal(err)
		}
		doc.AddField(field)
		batch.Update(doc)
	}

	err := idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	searcher, err := searcher.NewMatchAllSearcher(context.Background(), reader, 1.0, search.SearcherOptions{})
	if err != nil {
		t.Fatal(err)
	}

	sortOrder := search.SortOrder{&search.SortField{Field: "data", Type: search.SortFieldAsDate, Desc: true}}

	afterTime := time.Unix(6, 0).UTC()
	after := []string{afterTime.Format(time.RFC3339Nano)}

	collectorAfter := NewTopNCollectorAfter(5, sortOrder, after)
	err = collectorAfter.Collect(context.Background(), searcher, reader)
	if err != nil {
		t.Fatal(err)
	}

	resultsAfter := collectorAfter.Results()
	if len(resultsAfter) != 5 {
		t.Fatalf("expected 5 results, got %d", len(resultsAfter))
	}
	for i := range resultsAfter {
		raID := resultsAfter[i].ID
		docID := docs[i+len(resultsAfter)].id
		if raID != docID {
			t.Errorf("expected result '%s', got '%s'", docID, raID)
		}
	}
}

func TestSearchAfterGeo(t *testing.T) {
	idx := setupIndex(t)
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docs := []struct {
		id  string
		lon float64
		lat float64
	}{
		{"a", 1, 0},
		{"b", 2, 0},
		{"c", 3, 0},
		{"d", 4, 0},
		{"e", 5, 0},
		{"f", 6, 0},
		{"g", 7, 0},
		{"h", 8, 0},
		{"i", 9, 0},
		{"j", 10, 0},
	}

	batch := index.NewBatch()
	for _, d := range docs {
		doc := document.NewDocument(d.id)
		field := document.NewGeoPointFieldWithIndexingOptions("location", []uint64{}, d.lon, d.lat, index.IndexField|index.StoreField|index.IncludeTermVectors)
		doc.AddField(field)
		batch.Update(doc)
	}

	err := idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	searcher, err := searcher.NewMatchAllSearcher(context.Background(), reader, 1.0, search.SearcherOptions{})
	if err != nil {
		t.Fatal(err)
	}

	centerLon, centerLat := 0.0, 0.0
	sortOrder := search.SortOrder{&search.SortGeoDistance{Field: "location", Lon: centerLon, Lat: centerLat, Desc: false}}

	// search after doc "e" which has lon 5, lat 0
	afterLon, afterLat := 5.0, 0.0
	afterDistance := geo.Haversin(centerLon, centerLat, afterLon, afterLat)
	// to compensate scaling
	afterDistance *= 1000
	after := []string{strconv.FormatFloat(afterDistance, 'f', -1, 64)}

	collectorAfter := NewTopNCollectorAfter(5, sortOrder, after)
	err = collectorAfter.Collect(context.Background(), searcher, reader)
	if err != nil {
		t.Fatal(err)
	}

	resultsAfter := collectorAfter.Results()

	if len(resultsAfter) != 5 {
		t.Fatalf("expected 5 results, got %d", len(resultsAfter))
	}
	for i := range resultsAfter {
		raID := resultsAfter[i].ID
		docID := docs[i+len(resultsAfter)].id
		if raID != docID {
			t.Errorf("expected result '%s', got '%s'", docID, raID)
		}
	}
}

func BenchmarkTop10of0Scores(b *testing.B) {
	benchHelper(0, func() search.Collector {
		return NewTopNCollector(10, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop10of3Scores(b *testing.B) {
	benchHelper(3, func() search.Collector {
		return NewTopNCollector(10, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop10of10Scores(b *testing.B) {
	benchHelper(10, func() search.Collector {
		return NewTopNCollector(10, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop10of25Scores(b *testing.B) {
	benchHelper(25, func() search.Collector {
		return NewTopNCollector(10, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop10of50Scores(b *testing.B) {
	benchHelper(50, func() search.Collector {
		return NewTopNCollector(10, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop10of10000Scores(b *testing.B) {
	benchHelper(10000, func() search.Collector {
		return NewTopNCollector(10, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop100of0Scores(b *testing.B) {
	benchHelper(0, func() search.Collector {
		return NewTopNCollector(100, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop100of3Scores(b *testing.B) {
	benchHelper(3, func() search.Collector {
		return NewTopNCollector(100, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop100of10Scores(b *testing.B) {
	benchHelper(10, func() search.Collector {
		return NewTopNCollector(100, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop100of25Scores(b *testing.B) {
	benchHelper(25, func() search.Collector {
		return NewTopNCollector(100, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop100of50Scores(b *testing.B) {
	benchHelper(50, func() search.Collector {
		return NewTopNCollector(100, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop100of10000Scores(b *testing.B) {
	benchHelper(10000, func() search.Collector {
		return NewTopNCollector(100, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop1000of10000Scores(b *testing.B) {
	benchHelper(10000, func() search.Collector {
		return NewTopNCollector(1000, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop10000of100000Scores(b *testing.B) {
	benchHelper(100000, func() search.Collector {
		return NewTopNCollector(10000, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop10of100000Scores(b *testing.B) {
	benchHelper(100000, func() search.Collector {
		return NewTopNCollector(10, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop100of100000Scores(b *testing.B) {
	benchHelper(100000, func() search.Collector {
		return NewTopNCollector(100, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop1000of100000Scores(b *testing.B) {
	benchHelper(100000, func() search.Collector {
		return NewTopNCollector(1000, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

func BenchmarkTop10000of1000000Scores(b *testing.B) {
	benchHelper(1000000, func() search.Collector {
		return NewTopNCollector(10000, 0, search.SortOrder{&search.SortScore{Desc: true}})
	}, b)
}

// Test field collapsing - deduplicate results by field value
func TestCollapse(t *testing.T) {
	// Create test data: multiple chunks per document
	// doc1: chunks a (score 10), b (score 20), c (score 5)
	// doc2: chunks d (score 15), e (score 8)
	// doc3: chunk f (score 25)
	// With collapse on document_id, we should get:
	// - doc1 represented by chunk b (score 20)
	// - doc2 represented by chunk d (score 15)
	// - doc3 represented by chunk f (score 25)
	// Sorted by score desc: f(25), b(20), d(15)

	fieldValues := map[string]string{
		"a": "doc1",
		"b": "doc1",
		"c": "doc1",
		"d": "doc2",
		"e": "doc2",
		"f": "doc3",
	}

	searcher := &stubSearcher{
		matches: []*search.DocumentMatch{
			{IndexInternalID: index.IndexInternalID("a"), Score: 10},
			{IndexInternalID: index.IndexInternalID("b"), Score: 20},
			{IndexInternalID: index.IndexInternalID("c"), Score: 5},
			{IndexInternalID: index.IndexInternalID("d"), Score: 15},
			{IndexInternalID: index.IndexInternalID("e"), Score: 8},
			{IndexInternalID: index.IndexInternalID("f"), Score: 25},
		},
	}

	reader := &stubReaderWithCollapse{
		fieldValues: fieldValues,
		fieldName:   "document_id",
	}

	collector := NewTopNCollector(10, 0, search.SortOrder{&search.SortScore{Desc: true}})
	collector.SetCollapse("document_id")

	err := collector.Collect(context.Background(), searcher, reader)
	if err != nil {
		t.Fatal(err)
	}

	total := collector.Total()
	if total != 6 {
		t.Errorf("expected 6 total hits, got %d", total)
	}

	results := collector.Results()

	// Should get 3 results (one per unique document_id)
	if len(results) != 3 {
		t.Fatalf("expected 3 results after collapse, got %d", len(results))
	}

	// Check that we got the highest scoring chunk per document
	// Result 0: f (doc3, score 25)
	if results[0].ID != "f" {
		t.Errorf("expected first result ID 'f', got %s", results[0].ID)
	}
	if results[0].Score != 25 {
		t.Errorf("expected first result score 25, got %f", results[0].Score)
	}

	// Result 1: b (doc1, score 20)
	if results[1].ID != "b" {
		t.Errorf("expected second result ID 'b', got %s", results[1].ID)
	}
	if results[1].Score != 20 {
		t.Errorf("expected second result score 20, got %f", results[1].Score)
	}

	// Result 2: d (doc2, score 15)
	if results[2].ID != "d" {
		t.Errorf("expected third result ID 'd', got %s", results[2].ID)
	}
	if results[2].Score != 15 {
		t.Errorf("expected third result score 15, got %f", results[2].Score)
	}
}

// Test collapse with missing field values
func TestCollapseWithMissingValues(t *testing.T) {
	// doc1: chunk a (score 10)
	// doc2: chunk b (score 20)
	// no doc_id: chunks c (score 15), d (score 8)
	// With collapse, missing values should be grouped together

	fieldValues := map[string]string{
		"a": "doc1",
		"b": "doc2",
		// c and d have no document_id field
	}

	searcher := &stubSearcher{
		matches: []*search.DocumentMatch{
			{IndexInternalID: index.IndexInternalID("a"), Score: 10},
			{IndexInternalID: index.IndexInternalID("b"), Score: 20},
			{IndexInternalID: index.IndexInternalID("c"), Score: 15},
			{IndexInternalID: index.IndexInternalID("d"), Score: 8},
		},
	}

	reader := &stubReaderWithCollapse{
		fieldValues: fieldValues,
		fieldName:   "document_id",
	}

	collector := NewTopNCollector(10, 0, search.SortOrder{&search.SortScore{Desc: true}})
	collector.SetCollapse("document_id")

	err := collector.Collect(context.Background(), searcher, reader)
	if err != nil {
		t.Fatal(err)
	}

	results := collector.Results()

	// Should get 3 results:
	// - doc2 (b, score 20)
	// - missing group (c, score 15)
	// - doc1 (a, score 10)
	if len(results) != 3 {
		t.Fatalf("expected 3 results after collapse, got %d", len(results))
	}

	// Verify scores are in descending order
	if results[0].Score != 20 || results[1].Score != 15 || results[2].Score != 10 {
		t.Errorf("results not properly sorted: scores %f, %f, %f",
			results[0].Score, results[1].Score, results[2].Score)
	}
}

// stubReaderWithCollapse is a stub reader that can return collapse field values
type stubReaderWithCollapse struct {
	stubReader
	fieldValues map[string]string
	fieldName   string
}

func (sr *stubReaderWithCollapse) DocumentVisitFieldTerms(id index.IndexInternalID, fields []string, visitor index.DocValueVisitor) error {
	idStr := string(id)
	for _, field := range fields {
		if field == sr.fieldName {
			if value, ok := sr.fieldValues[idStr]; ok {
				visitor(field, []byte(value))
			}
			// If not in map, field is missing (empty value will be used)
		}
	}
	return nil
}

func (sr *stubReaderWithCollapse) DocValueReader(fields []string) (index.DocValueReader, error) {
	return &collapseDocValueReader{sr: sr, fields: fields}, nil
}

type collapseDocValueReader struct {
	sr     *stubReaderWithCollapse
	fields []string
}

func (dvr *collapseDocValueReader) VisitDocValues(id index.IndexInternalID, visitor index.DocValueVisitor) error {
	return dvr.sr.DocumentVisitFieldTerms(id, dvr.fields, visitor)
}

func (dvr *collapseDocValueReader) BytesRead() uint64 {
	return 0
}
