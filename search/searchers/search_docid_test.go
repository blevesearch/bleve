//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package searchers

import (
	"testing"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/gtreap"
	"github.com/blevesearch/bleve/index/upside_down"
)

func testDocIDSearcher(t *testing.T, indexed, searched, wanted []string) {
	analysisQueue := index.NewAnalysisQueue(1)
	i, err := upside_down.NewUpsideDownCouch(gtreap.Name, nil, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = i.Open()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range indexed {
		err = i.Update(&document.Document{
			ID: id,
			Fields: []document.Field{
				document.NewTextField("desc", []uint64{}, []byte("beer")),
			},
		})
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

	searcher, err := NewDocIDSearcher(indexReader, searched, 1.0, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := searcher.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	if searcher.Count() != uint64(len(wanted)) {
		t.Fatalf("expected count %v got %v", len(wanted), searcher.Count())
	}

	// Check the sequence
	for i, id := range wanted {
		m, err := searcher.Next()
		if err != nil {
			t.Fatal(err)
		}
		if id != m.ID {
			t.Fatalf("expected %v at position %v, got %v", id, i, m.ID)
		}
	}
	m, err := searcher.Next()
	if err != nil {
		t.Fatal(err)
	}
	if m != nil {
		t.Fatalf("expected nil past the end of the sequence, got %v", m.ID)
	}

	// Check seeking
	for _, id := range wanted {
		if len(id) != 2 {
			t.Fatalf("expected identifier must be 2 characters long, got %v", id)
		}
		before := id[:1]
		for _, target := range []string{before, id} {
			m, err := searcher.Advance(target)
			if err != nil {
				t.Fatal(err)
			}
			if m == nil || m.ID != id {
				t.Fatalf("advancing to %v returned %v instead of %v", before, m, id)
			}
		}
	}
	// Seek after the end of the sequence
	after := "zzz"
	m, err = searcher.Advance(after)
	if err != nil {
		t.Fatal(err)
	}
	if m != nil {
		t.Fatalf("advancing past the end of the sequence should return nil, got %v", m)
	}
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
