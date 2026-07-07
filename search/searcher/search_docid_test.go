//  Copyright (c) 2015 Couchbase, Inc.
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
	"context"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

func testDocIDSearcher(t *testing.T, indexed, searched, wanted []string) {
	dir := t.TempDir()
	analysisQueue := index.NewAnalysisQueue(1)
	i, err := scorch.NewScorch(
		scorch.Name,
		map[string]interface{}{
			"path": dir,
		},
		analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = i.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := i.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	for _, id := range indexed {
		doc := document.NewDocument(id)
		doc.AddField(document.NewTextField("desc", []uint64{}, []byte("beer")))
		err = i.Update(doc)
		if err != nil {
			t.Fatal(err)
		}
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

	explainOff := search.SearcherOptions{Explain: false}

	searcher, err := NewDocIDSearcher(context.TODO(), indexReader, searched, 1.0, explainOff)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := searcher.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	ctx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(searcher.DocumentMatchPoolSize(), 0),
	}

	// Check the sequence
	for i, id := range wanted {
		m, err := searcher.Next(ctx)
		if err != nil {
			t.Fatal(err)
		}
		gotID, err := indexReader.ExternalID(m.IndexInternalID)
		if err != nil {
			t.Fatal(err)
		}
		if gotID != id {
			t.Fatalf("expected %v at position %d, got %v", id, i, gotID)
		}
		ctx.DocumentMatchPool.Put(m)
	}
	m, err := searcher.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if m != nil {
		t.Fatalf("expected nil past the end of the sequence, got %v", m.IndexInternalID)
	}
	ctx.DocumentMatchPool.Put(m)

	searcher, err = NewDocIDSearcher(context.TODO(), indexReader, searched, 1.0, explainOff)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := searcher.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// Check seeking
	for _, target := range wanted {
		iid, err := indexReader.InternalID(target)
		if err != nil {
			t.Fatal(err)
		}
		m, err := searcher.Advance(ctx, iid)
		if err != nil {
			t.Fatal(err)
		}
		if m == nil {
			t.Fatalf("advancing to %v returned nil", target)
		}
		eid, err := indexReader.ExternalID(m.IndexInternalID)
		if err != nil {
			t.Fatal(err)
		}
		if eid != target {
			t.Fatalf("advancing to %v returned %v instead of %v", target, eid, target)
		}
		ctx.DocumentMatchPool.Put(m)

	}
	// Seek after the end of the sequence
	after := "zzz"

	iid, err := indexReader.InternalID(after)
	if err != nil {
		t.Fatal(err)
	}

	m, err = searcher.Advance(ctx, iid)
	if err != nil {
		t.Fatal(err)
	}
	if m != nil {
		t.Fatalf("advancing past the end of the sequence should return nil, got %v", m)
	}
	ctx.DocumentMatchPool.Put(m)
}

func TestDocIDSearcherEmptySearchEmptyIndex(t *testing.T) {
	testDocIDSearcher(t, nil, nil, nil)
}

func TestDocIDSearcherEmptyIndex(t *testing.T) {
	testDocIDSearcher(t, nil, []string{"aa", "bb"}, nil)
}

func TestDocIDSearcherEmptySearch(t *testing.T) {
	testDocIDSearcher(t, []string{"aa", "bb"}, nil, nil)
}

func TestDocIDSearcherValid(t *testing.T) {
	// Test missing, out of order and duplicate inputs
	testDocIDSearcher(t, []string{"aa", "bb", "cc"},
		[]string{"ee", "bb", "aa", "bb"},
		[]string{"aa", "bb"})
}
