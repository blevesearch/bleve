//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import (
	"math"
	"testing"

	"github.com/couchbaselabs/bleve/document"
	"github.com/couchbaselabs/bleve/index/store/inmem"
	"github.com/couchbaselabs/bleve/index/upside_down"
)

func TestTermSearcher(t *testing.T) {

	query := TermQuery{
		Term:     "beer",
		Field:    "desc",
		BoostVal: 3.0,
		Explain:  true,
	}

	inMemStore, _ := inmem.Open()
	i := upside_down.NewUpsideDownCouch(inMemStore)
	i.Update(&document.Document{
		ID: "a",
		Fields: []document.Field{
			document.NewTextField("desc", []byte("beer")),
		},
	})
	i.Update(&document.Document{
		ID: "b",
		Fields: []document.Field{
			document.NewTextField("desc", []byte("beer")),
		},
	})
	i.Update(&document.Document{
		ID: "c",
		Fields: []document.Field{
			document.NewTextField("desc", []byte("beer")),
		},
	})
	i.Update(&document.Document{
		ID: "d",
		Fields: []document.Field{
			document.NewTextField("desc", []byte("beer")),
		},
	})
	i.Update(&document.Document{
		ID: "e",
		Fields: []document.Field{
			document.NewTextField("desc", []byte("beer")),
		},
	})
	i.Update(&document.Document{
		ID: "f",
		Fields: []document.Field{
			document.NewTextField("desc", []byte("beer")),
		},
	})
	i.Update(&document.Document{
		ID: "g",
		Fields: []document.Field{
			document.NewTextField("desc", []byte("beer")),
		},
	})
	i.Update(&document.Document{
		ID: "h",
		Fields: []document.Field{
			document.NewTextField("desc", []byte("beer")),
		},
	})
	i.Update(&document.Document{
		ID: "i",
		Fields: []document.Field{
			document.NewTextField("desc", []byte("beer")),
		},
	})
	i.Update(&document.Document{
		ID: "j",
		Fields: []document.Field{
			document.NewTextField("title", []byte("cat")),
		},
	})

	searcher, err := NewTermSearcher(i, &query)
	if err != nil {
		t.Fatal(err)
	}
	defer searcher.Close()

	searcher.SetQueryNorm(2.0)
	idf := 1.0 + math.Log(float64(i.DocCount())/float64(searcher.Count()+1.0))
	expectedQueryWeight := 3 * idf * 3 * idf
	if expectedQueryWeight != searcher.Weight() {
		t.Errorf("expected weight %v got %v", expectedQueryWeight, searcher.Weight())
	}

	if searcher.Count() != 9 {
		t.Errorf("expected count of 9, got %d", searcher.Count())
	}

	docMatch, err := searcher.Next()
	if err != nil {
		t.Errorf("expected result, got %v", err)
	}
	if docMatch.ID != "a" {
		t.Errorf("expected result ID to be 'a', got '%s", docMatch.ID)
	}
	docMatch, err = searcher.Advance("c")
	if err != nil {
		t.Errorf("expected result, got %v", err)
	}
	if docMatch.ID != "c" {
		t.Errorf("expected result ID to be 'c' got '%s'", docMatch.ID)
	}

	// try advancing past end
	docMatch, err = searcher.Advance("z")
	if err != nil {
		t.Fatal(err)
	}
	if docMatch != nil {
		t.Errorf("expected nil, got %v", docMatch)
	}

	// try pushing next past end
	docMatch, err = searcher.Next()
	if err != nil {
		t.Fatal(err)
	}
	if docMatch != nil {
		t.Errorf("expected nil, got %v", docMatch)
	}
}
