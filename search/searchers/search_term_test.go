//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package searchers

import (
	"math"
	"testing"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/gtreap"
	"github.com/blevesearch/bleve/index/upside_down"
)

func TestTermSearcher(t *testing.T) {

	var queryTerm = "beer"
	var queryField = "desc"
	var queryBoost = 3.0
	var queryExplain = true

	analysisQueue := index.NewAnalysisQueue(1)
	i, err := upside_down.NewUpsideDownCouch(gtreap.Name, nil, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = i.Open()
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "a",
		Fields: []document.Field{
			document.NewTextField("desc", []uint64{}, []byte("beer")),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "b",
		Fields: []document.Field{
			document.NewTextField("desc", []uint64{}, []byte("beer")),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "c",
		Fields: []document.Field{
			document.NewTextField("desc", []uint64{}, []byte("beer")),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "d",
		Fields: []document.Field{
			document.NewTextField("desc", []uint64{}, []byte("beer")),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "e",
		Fields: []document.Field{
			document.NewTextField("desc", []uint64{}, []byte("beer")),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "f",
		Fields: []document.Field{
			document.NewTextField("desc", []uint64{}, []byte("beer")),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "g",
		Fields: []document.Field{
			document.NewTextField("desc", []uint64{}, []byte("beer")),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "h",
		Fields: []document.Field{
			document.NewTextField("desc", []uint64{}, []byte("beer")),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "i",
		Fields: []document.Field{
			document.NewTextField("desc", []uint64{}, []byte("beer")),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = i.Update(&document.Document{
		ID: "j",
		Fields: []document.Field{
			document.NewTextField("title", []uint64{}, []byte("cat")),
		},
	})
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

	searcher, err := NewTermSearcher(indexReader, queryTerm, queryField, queryBoost, queryExplain)
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
	docCount, err := i.DocCount()
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

	docMatch, err := searcher.Next(nil)
	if err != nil {
		t.Errorf("expected result, got %v", err)
	}
	if docMatch.ArrangeID() != "a" {
		t.Errorf("expected result ID to be 'a', got '%s", docMatch.ArrangeID())
	}
	docMatch, err = searcher.Advance("c", nil)
	if err != nil {
		t.Errorf("expected result, got %v", err)
	}
	if docMatch.ArrangeID() != "c" {
		t.Errorf("expected result ID to be 'c' got '%s'", docMatch.ArrangeID())
	}

	// try advancing past end
	docMatch, err = searcher.Advance("z", nil)
	if err != nil {
		t.Fatal(err)
	}
	if docMatch != nil {
		t.Errorf("expected nil, got %v", docMatch)
	}

	// try pushing next past end
	docMatch, err = searcher.Next(nil)
	if err != nil {
		t.Fatal(err)
	}
	if docMatch != nil {
		t.Errorf("expected nil, got %v", docMatch)
	}
}
