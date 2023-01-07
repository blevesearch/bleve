//  Copyright (c) 2013 Couchbase, Inc.
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

package searcher

import (
	"math"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/upsidedown"
	"github.com/blevesearch/bleve/v2/index/upsidedown/store/gtreap"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

func TestTermSearcher(t *testing.T) {

	var queryTerm = "beer"
	var queryField = "desc"
	var queryBoost = 3.0
	var queryExplain = search.SearcherOptions{Explain: true}

	analysisQueue := index.NewAnalysisQueue(1)
	i, err := upsidedown.NewUpsideDownCouch(
		gtreap.Name,
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
	doc := document.NewDocument("a")
	doc.AddField(document.NewTextField("desc", []uint64{}, []byte("beer")))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("b")
	doc.AddField(document.NewTextField("desc", []uint64{}, []byte("beer")))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("c")
	doc.AddField(document.NewTextField("desc", []uint64{}, []byte("beer")))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("d")
	doc.AddField(document.NewTextField("desc", []uint64{}, []byte("beer")))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("e")
	doc.AddField(document.NewTextField("desc", []uint64{}, []byte("beer")))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("f")
	doc.AddField(document.NewTextField("desc", []uint64{}, []byte("beer")))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("g")
	doc.AddField(document.NewTextField("desc", []uint64{}, []byte("beer")))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("h")
	doc.AddField(document.NewTextField("desc", []uint64{}, []byte("beer")))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("i")
	doc.AddField(document.NewTextField("desc", []uint64{}, []byte("beer")))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}
	doc = document.NewDocument("j")
	doc.AddField(document.NewTextField("title", []uint64{}, []byte("cat")))
	err = i.Update(doc)
	if err != nil {
		t.Fatal(err)
	}

	indexReader, err := i.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	searcher, err := NewTermSearcher(nil, indexReader, queryTerm, queryField, queryBoost, queryExplain)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := searcher.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	searcher.SetQueryNorm(2.0)
	docCount, err := indexReader.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	idf := 1.0 + math.Log(float64(docCount)/float64(searcher.Count()+1.0))
	expectedQueryWeight := 3 * idf * 3 * idf
	if expectedQueryWeight != searcher.Weight() {
		t.Errorf("expected weight %v got %v", expectedQueryWeight, searcher.Weight())
	}

	if searcher.Count() != 9 {
		t.Errorf("expected count of 9, got %d", searcher.Count())
	}

	ctx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(1, 0),
	}
	docMatch, err := searcher.Next(ctx)
	if err != nil {
		t.Errorf("expected result, got %v", err)
	}
	if !docMatch.IndexInternalID.Equals(index.IndexInternalID("a")) {
		t.Errorf("expected result ID to be 'a', got '%s", docMatch.IndexInternalID)
	}
	ctx.DocumentMatchPool.Put(docMatch)
	docMatch, err = searcher.Advance(ctx, index.IndexInternalID("c"))
	if err != nil {
		t.Errorf("expected result, got %v", err)
	}
	if !docMatch.IndexInternalID.Equals(index.IndexInternalID("c")) {
		t.Errorf("expected result ID to be 'c' got '%s'", docMatch.IndexInternalID)
	}

	// try advancing past end
	ctx.DocumentMatchPool.Put(docMatch)
	docMatch, err = searcher.Advance(ctx, index.IndexInternalID("z"))
	if err != nil {
		t.Fatal(err)
	}
	if docMatch != nil {
		t.Errorf("expected nil, got %v", docMatch)
	}

	// try pushing next past end
	ctx.DocumentMatchPool.Put(docMatch)
	docMatch, err = searcher.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if docMatch != nil {
		t.Errorf("expected nil, got %v", docMatch)
	}
}
