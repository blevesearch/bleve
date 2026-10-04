//  Copyright (c) 2026 Couchbase, Inc.
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

package bleve

import (
	"fmt"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

// The other tests compare the two paths on an index of 1600 documents, in segments
// of 200. This is the same comparison on segments past the sizes at which the
// algorithms work in windows (4096 and 8192 docs), with sort values of a thousand
// distinct strings and a million distinct numbers, and postings of a hundred
// thousand docs: every kind of request the path serves, against the regular path.
func TestPerSegmentSearchMatchesRegularSearchOnLargeSegments(t *testing.T) {
	if testing.Short() {
		t.Skip("builds an index of 100000 documents")
	}
	idx, cleanup := buildSortBenchIndex(t, 100000, 8, "bm25")
	defer cleanup()

	term := func(s string) query.Query { return termQueryOn("body", s) }
	queries := map[string]query.Query{
		"term 10%":  term("mk10"),
		"term 50%":  term("mk50"),
		"term rare": term("mk01"),
		"or":        query.NewDisjunctionQuery([]query.Query{term("mk10"), term("mk1"), term("mk01")}),
		"or of 5": query.NewDisjunctionQuery([]query.Query{
			term("mk50"), term("mk10"), term("mk1"), term("mk01"), term("w3")}),
		"and": query.NewConjunctionQuery([]query.Query{term("mk50"), term("mk10")}),
		"and of 3": query.NewConjunctionQuery([]query.Query{
			term("mk100"), term("mk50"), term("w0")}),
		"must not":    query.NewBooleanQuery([]query.Query{term("mk50")}, nil, []query.Query{term("mk10")}),
		"must should": query.NewBooleanQuery([]query.Query{term("mk50")}, []query.Query{term("mk10")}, nil),
	}
	f := func(v float64) *float64 { return &v }
	type variant struct {
		name string
		mod  func(r *SearchRequest)
	}
	variants := []variant{
		{"score", func(r *SearchRequest) {}},
		{"score, explain", func(r *SearchRequest) { r.Explain = true }},
		{"n", func(r *SearchRequest) {
			r.SortByCustom(search.SortOrder{&search.SortField{Field: "n", Type: search.SortFieldAsNumber}})
		}},
		{"-n, from", func(r *SearchRequest) {
			r.From = 30
			r.SortByCustom(search.SortOrder{&search.SortField{Field: "n", Type: search.SortFieldAsNumber, Desc: true}})
		}},
		{"s, n", func(r *SearchRequest) {
			r.SortByCustom(search.SortOrder{&search.SortField{Field: "s"},
				&search.SortField{Field: "n", Type: search.SortFieldAsNumber, Desc: true}})
		}},
		{"_id", func(r *SearchRequest) { r.SortByCustom(search.SortOrder{&search.SortDocID{}}) }},
		{"score, n", func(r *SearchRequest) {
			r.SortByCustom(search.SortOrder{&search.SortScore{Desc: true},
				&search.SortField{Field: "n", Type: search.SortFieldAsNumber}})
		}},
		{"facets", func(r *SearchRequest) {
			r.AddFacet("s", NewFacetRequest("s", 10))
			fr := NewFacetRequest("n", 3)
			fr.AddNumericRange("low", nil, f(300000))
			fr.AddNumericRange("mid", f(300000), f(700000))
			fr.AddNumericRange("high", f(700000), nil)
			r.AddFacet("n", fr)
		}},
		{"facets, size 0", func(r *SearchRequest) { r.Size = 0; r.AddFacet("s", NewFacetRequest("s", 10)) }},
		{"facets, sorted", func(r *SearchRequest) {
			r.SortByCustom(search.SortOrder{&search.SortField{Field: "n", Type: search.SortFieldAsNumber}, &search.SortDocID{}})
			r.AddFacet("s", NewFacetRequest("s", 5))
		}},
		{"after", func(r *SearchRequest) {
			r.SortByCustom(search.SortOrder{&search.SortField{Field: "n", Type: search.SortFieldAsNumber}, &search.SortDocID{}})
			r.SearchAfter = []string{"500000", "50000"}
		}},
		{"before", func(r *SearchRequest) {
			r.SortByCustom(search.SortOrder{&search.SortField{Field: "n", Type: search.SortFieldAsNumber, Desc: true}, &search.SortDocID{}})
			r.SearchBefore = []string{"500000", "50000"}
		}},
		{"score none", func(r *SearchRequest) { r.Score = ScoreNone }},
	}
	for qname, q := range queries {
		for _, v := range variants {
			for _, size := range []int{10, 100} {
				what := fmt.Sprintf("%s / %s / size %d", qname, v.name, size)
				mkReq := func() *SearchRequest {
					r := NewSearchRequestOptions(q, size, 0, false)
					v.mod(r)
					return r
				}
				old, got := runBothPaths(t, idx, mkReq, what)
				switch v.name {
				case "score", "score, explain", "facets", "facets, size 0":
					compareSearchResults(t, what, old, got)
				case "score, n":
					compareScoreTiedResults(t, what, old, got)
				case "score none":
					// the regular path stops early when scores are off, and counts
					// less: the hits are the same
					if len(old.Hits) != len(got.Hits) {
						t.Fatalf("%s: %d hits, want %d", what, len(got.Hits), len(old.Hits))
					}
					for i := range old.Hits {
						if old.Hits[i].ID != got.Hits[i].ID {
							t.Fatalf("%s: hit %d is %s, want %s", what, i, got.Hits[i].ID, old.Hits[i].ID)
						}
					}
				default:
					compareSortedResults(t, what, old, got)
				}
				if len(old.Facets) > 0 {
					compareFacets(t, what, old, got)
				}
				if v.name == "score, explain" {
					for i, h := range got.Hits {
						if h.Expl == nil || h.Expl.Value != h.Score {
							t.Fatalf("%s: hit %d: explanation %v, score %v", what, i, h.Expl, h.Score)
						}
					}
				}
			}
		}
	}
}

// A search of several indexes (an alias) merges what the indexes' searches give by
// the hits' sort values, and adds their facets and totals: with the per segment
// path serving each of them, it comes out as with the regular path.
func TestPerSegmentSearchThroughAnAlias(t *testing.T) {
	idx1, cleanup1 := perSegmentSortTestIndex(t, index.BM25Scoring)
	defer cleanup1()
	idx2, cleanup2 := perSegmentSortTestIndex(t, index.BM25Scoring)
	defer cleanup2()
	alias := NewIndexAlias(idx1, idx2)

	f := func(v float64) *float64 { return &v }
	cases := map[string]func(r *SearchRequest){
		"sorted": func(r *SearchRequest) {
			r.SortByCustom(search.SortOrder{sortField("n", asNumber), &search.SortDocID{}})
		},
		"sorted, from": func(r *SearchRequest) {
			r.From = 7
			r.SortByCustom(search.SortOrder{sortField("s"), &search.SortDocID{Desc: true}})
		},
		"score": func(r *SearchRequest) {},
		"facets": func(r *SearchRequest) {
			r.AddFacet("s", NewFacetRequest("s", 5))
			fr := NewFacetRequest("n", 2)
			fr.AddNumericRange("low", nil, f(10))
			fr.AddNumericRange("high", f(10), nil)
			r.AddFacet("n", fr)
		},
		"after": func(r *SearchRequest) {
			r.SortByCustom(search.SortOrder{sortField("n", asNumber), &search.SortDocID{}})
			r.SearchAfter = []string{"9", "doc500"}
		},
		"before": func(r *SearchRequest) {
			r.SortByCustom(search.SortOrder{sortField("n", asNumber), &search.SortDocID{}})
			r.SearchBefore = []string{"9", "doc500"}
		},
	}
	for name, mod := range cases {
		mkReq := func() *SearchRequest {
			r := NewSearchRequestOptions(termQueryOn("body", "common"), 25, 0, false)
			mod(r)
			return r
		}
		perSegmentSearchEnabled.Store(false)
		old, err := alias.Search(mkReq())
		perSegmentSearchEnabled.Store(true)
		if err != nil {
			t.Fatalf("%s: regular: %v", name, err)
		}
		before := perSegmentSearches.Load()
		got, err := alias.Search(mkReq())
		if err != nil {
			t.Fatalf("%s: per segment: %v", name, err)
		}
		if perSegmentSearches.Load() != before+2 {
			t.Fatalf("%s: %d per segment searches, want one for each of the two indexes", name, perSegmentSearches.Load()-before)
		}
		if name == "score" {
			compareSearchResultsIgnoringIndex(t, "alias "+name, old, got)
		} else {
			compareSortedResultsIgnoringIndex(t, "alias "+name, old, got)
		}
		if len(old.Facets) > 0 {
			compareFacets(t, "alias "+name, old, got)
		}
	}
}
