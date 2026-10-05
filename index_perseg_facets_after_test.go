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
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

func facetTestQueries() map[string]func() query.Query {
	return map[string]func() query.Query{
		"term": func() query.Query { return termQueryOn("body", "common") },
		"or": func() query.Query {
			return query.NewDisjunctionQuery([]query.Query{termQueryOn("body", "even"), termQueryOn("body", "mid")})
		},
		"must not": func() query.Query {
			return query.NewBooleanQuery([]query.Query{termQueryOn("body", "common")}, nil,
				[]query.Query{termQueryOn("body", "five")})
		},
	}
}

// facet sets of the test index: terms (of a field of one value, of one of several
// and of one that some docs lack), ranges of numbers and of dates, a field that
// doesn't exist, and several at once
func facetTestRequests() map[string]func(r *SearchRequest) {
	f := func(v float64) *float64 { return &v }
	day := func(d int) time.Time { return time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, d) }
	return map[string]func(r *SearchRequest){
		"terms":         func(r *SearchRequest) { r.AddFacet("s", NewFacetRequest("s", 5)) },
		"terms of many": func(r *SearchRequest) { r.AddFacet("s", NewFacetRequest("s", 100)) },
		"terms of a multivalued field": func(r *SearchRequest) {
			r.AddFacet("tags", NewFacetRequest("tags", 4))
		},
		"numeric ranges": func(r *SearchRequest) {
			fr := NewFacetRequest("n", 3)
			fr.AddNumericRange("low", nil, f(8))
			fr.AddNumericRange("mid", f(8), f(16))
			fr.AddNumericRange("high", f(16), nil)
			r.AddFacet("n", fr)
		},
		"date ranges": func(r *SearchRequest) {
			fr := NewFacetRequest("d", 2)
			fr.AddDateTimeRange("first", day(0), day(10))
			fr.AddDateTimeRange("rest", day(10), day(40))
			r.AddFacet("d", fr)
		},
		"a field that isn't there": func(r *SearchRequest) { r.AddFacet("x", NewFacetRequest("nothing", 3)) },
		"several": func(r *SearchRequest) {
			r.AddFacet("s", NewFacetRequest("s", 3))
			r.AddFacet("tags", NewFacetRequest("tags", 3))
			fr := NewFacetRequest("n", 2)
			fr.AddNumericRange("low", nil, f(10))
			fr.AddNumericRange("high", f(10), nil)
			r.AddFacet("n", fr)
		},
	}
}

func compareFacets(t *testing.T, what string, old, got *SearchResult) {
	t.Helper()
	if !reflect.DeepEqual(old.Facets, got.Facets) {
		t.Fatalf("%s: facets differ:\n  regular: %v\n  per segment: %v", what, old.Facets, got.Facets)
	}
}

// Facets count every match, whether it makes the top hits or not, and come out as
// the regular collector's: for every kind of facet, with hits ordered by score or
// by anything else, for a page, none, or one past the end.
func TestPerSegmentFacetedSearchMatchesRegularSearch(t *testing.T) {
	sorts := map[string]func() search.SortOrder{
		"score": func() search.SortOrder { return search.SortOrder{&search.SortScore{Desc: true}} },
		"field": func() search.SortOrder { return search.SortOrder{sortField("n", asNumber, desc), &search.SortDocID{}} },
	}
	pages := []struct{ size, from int }{{10, 0}, {7, 3}, {0, 0}, {20, 5000}}
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		idx, cleanup := perSegmentSortTestIndex(t, model)
		defer cleanup()
		for qname, mkQuery := range facetTestQueries() {
			for fname, addFacets := range facetTestRequests() {
				for sname, mkSort := range sorts {
					for _, pg := range pages {
						what := fmt.Sprintf("%s/%s/facets %s/sort %s size=%d from=%d", model, qname, fname, sname, pg.size, pg.from)
						mkReq := func() *SearchRequest {
							r := NewSearchRequestOptions(mkQuery(), pg.size, pg.from, false)
							r.SortByCustom(mkSort())
							addFacets(r)
							return r
						}
						old, got := runBothPaths(t, idx, mkReq, what)
						if sname == "score" {
							compareSearchResults(t, what, old, got)
						} else {
							compareSortedResults(t, what, old, got)
						}
						compareFacets(t, what, old, got)
						if len(got.Facets) == 0 {
							t.Fatalf("%s: no facets", what)
						}
					}
				}
			}
		}
	}
}

// Facets that aren't empty prove something: at least some counted something.
func TestPerSegmentFacetsCountSomething(t *testing.T) {
	idx, cleanup := perSegmentSortTestIndex(t, index.DefaultScoringModel)
	defer cleanup()
	r := NewSearchRequestOptions(termQueryOn("body", "common"), 0, 0, false)
	r.AddFacet("s", NewFacetRequest("s", 5))
	res, err := idx.Search(r)
	if err != nil {
		t.Fatal(err)
	}
	fr := res.Facets["s"]
	if fr == nil || fr.Total == 0 || len(fr.Terms.Terms()) == 0 {
		t.Fatalf("facet %+v counted nothing", fr)
	}
}

// afterValues are what a search after is given to continue from a hit: the values
// its sorts were decoded to (the score, which isn't among those, as a number).
func afterValues(so search.SortOrder, h *search.DocumentMatch) []string {
	rv := make([]string, len(so))
	for i, s := range so {
		if s.RequiresScoring() {
			rv[i] = strconv.FormatFloat(h.Score, 'f', -1, 64)
		} else {
			rv[i] = h.DecodedSort[i]
		}
	}
	return rv
}

// sorts of the pagination tests, all of which end in something unique
func paginationSorts() map[string]func() search.SortOrder {
	return map[string]func() search.SortOrder{
		"id":        func() search.SortOrder { return search.SortOrder{&search.SortDocID{}} },
		"number id": func() search.SortOrder { return search.SortOrder{sortField("n", asNumber), &search.SortDocID{}} },
		"number desc id": func() search.SortOrder {
			return search.SortOrder{sortField("n", asNumber, desc), &search.SortDocID{}}
		},
		"string number id": func() search.SortOrder {
			return search.SortOrder{sortField("s"), sortField("n", asNumber, desc), &search.SortDocID{}}
		},
		"date id": func() search.SortOrder {
			return search.SortOrder{sortField("d", asDate), &search.SortDocID{Desc: true}}
		},
		"distance id": func() search.SortOrder {
			g, err := search.NewSortGeoDistance("loc", "km", -121.0, 37.8, false)
			if err != nil {
				panic(err)
			}
			return search.SortOrder{g, &search.SortDocID{}}
		},
	}
}

// A page after a hit is the regular path's, for every sort: walking through the
// results page by page, from the one before to the end, each page continuing
// from the last hit of the page of the regular path, comes out the same.
func TestPerSegmentSearchAfterWalksLikeTheRegularSearch(t *testing.T) {
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		idx, cleanup := perSegmentSortTestIndex(t, model)
		defer cleanup()
		for sname, mkSort := range paginationSorts() {
			for qname, mkQuery := range facetTestQueries() {
				var after []string
				pageNo := 0
				// (not to the end: docs that lack a value sort last, and the regular
				// search can't continue from one of those: a walk to the end of a field
				// that some docs lack goes round in circles in both paths)
				for pageNo < 12 {
					what := fmt.Sprintf("%s/%s/%s page %d after %v", model, qname, sname, pageNo, after)
					mkReq := func() *SearchRequest {
						r := NewSearchRequestOptions(mkQuery(), 37, 0, false)
						r.SortByCustom(mkSort())
						r.SearchAfter = after
						return r
					}
					old, got := runBothPaths(t, idx, mkReq, what)
					compareSortedResults(t, what, old, got)
					if len(old.Hits) == 0 {
						break
					}
					last := old.Hits[len(old.Hits)-1]
					after = afterValues(mkSort(), last)
					pageNo++
				}
				if pageNo < 3 {
					t.Fatalf("%s/%s: only %d pages: the walk proves little", qname, sname, pageNo)
				}
			}
		}
	}
}

// A page before a hit, walking backwards from the end, is the regular path's.
func TestPerSegmentSearchBeforeWalksLikeTheRegularSearch(t *testing.T) {
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		idx, cleanup := perSegmentSortTestIndex(t, model)
		defer cleanup()
		for sname, mkSort := range paginationSorts() {
			qname, mkQuery := "or", facetTestQueries()["or"]
			// the last page is the one before the end: search before values past all
			var before []string
			first := NewSearchRequestOptions(mkQuery(), 5000, 0, false)
			first.SortByCustom(mkSort())
			all, err := idx.Search(first)
			if err != nil {
				t.Fatal(err)
			}
			if len(all.Hits) < 100 {
				t.Fatalf("%s: %d hits", sname, len(all.Hits))
			}
			// start from three quarters in (past that, docs that lack a value of a field
			// sort, which a search can't continue from), then go back page by page
			before = afterValues(mkSort(), all.Hits[len(all.Hits)*3/4])
			pageNo := 0
			for pageNo < 12 {
				what := fmt.Sprintf("%s/%s/%s page %d before %v", model, qname, sname, pageNo, before)
				mkReq := func() *SearchRequest {
					r := NewSearchRequestOptions(mkQuery(), 37, 0, false)
					r.SortByCustom(mkSort())
					r.SearchBefore = before
					return r
				}
				old, got := runBothPaths(t, idx, mkReq, what)
				compareSortedResults(t, what, old, got)
				if len(old.Hits) == 0 {
					break
				}
				before = afterValues(mkSort(), old.Hits[0])
				pageNo++
			}
			if pageNo < 3 {
				t.Fatalf("%s/%s: only %d pages: the walk proves little", qname, sname, pageNo)
			}
		}
	}
}

// Search after values at the edges, and ones that tie hits on every sort (those
// aren't after them: all of the ties are left out, and the total still counts them).
func TestPerSegmentSearchAfterAtTheEdges(t *testing.T) {
	idx, cleanup := perSegmentSortTestIndex(t, index.DefaultScoringModel)
	defer cleanup()
	cases := map[string]struct {
		sort  func() search.SortOrder
		after []string
	}{
		"before everything":       {func() search.SortOrder { return search.SortOrder{sortField("n", asNumber)} }, []string{"-1"}},
		"past everything":         {func() search.SortOrder { return search.SortOrder{sortField("n", asNumber)} }, []string{"1000"}},
		"a value of many docs":    {func() search.SortOrder { return search.SortOrder{sortField("n", asNumber)} }, []string{"10"}},
		"a value of many, desc":   {func() search.SortOrder { return search.SortOrder{sortField("n", asNumber, desc)} }, []string{"10"}},
		"a string of many docs":   {func() search.SortOrder { return search.SortOrder{sortField("s")} }, []string{"cat"}},
		"a string that isn't any": {func() search.SortOrder { return search.SortOrder{sortField("s")} }, []string{"cb"}},
		"the first id":            {func() search.SortOrder { return search.SortOrder{&search.SortDocID{}} }, []string{"doc0"}},
		"an id that isn't any":    {func() search.SortOrder { return search.SortOrder{&search.SortDocID{}} }, []string{"doc0a"}},
		"a score of the last page": {func() search.SortOrder {
			return search.SortOrder{&search.SortScore{Desc: true}, &search.SortDocID{}}
		}, []string{"0.00001", "doc1"}},
		"a score above all": {func() search.SortOrder {
			return search.SortOrder{&search.SortScore{Desc: true}, &search.SortDocID{}}
		}, []string{"1000", "doc1"}},
	}
	for name, c := range cases {
		for _, size := range []int{10, 0, 1000} {
			what := fmt.Sprintf("%s size=%d", name, size)
			mkReq := func() *SearchRequest {
				r := NewSearchRequestOptions(termQueryOn("body", "common"), size, 0, false)
				r.SortByCustom(c.sort())
				r.SearchAfter = c.after
				return r
			}
			old, got := runBothPaths(t, idx, mkReq, what)
			compareSortedResults(t, what, old, got)
		}
	}
}

// With no sort at all (a request that wasn't made by NewSearchRequest) hits are in
// the order they were found, as the regular path has them.
func TestPerSegmentSearchWithoutASort(t *testing.T) {
	idx, cleanup := perSegmentSortTestIndex(t, index.DefaultScoringModel)
	defer cleanup()
	mkReq := func() *SearchRequest {
		return &SearchRequest{Query: termQueryOn("body", "even"), Size: 15, From: 4}
	}
	old, got := runBothPaths(t, idx, mkReq, "no sort")
	compareSortedResults(t, "no sort", old, got)
}

// Facets and search after together: the facets count what was left out of the page.
func TestPerSegmentFacetsWithSearchAfter(t *testing.T) {
	idx, cleanup := perSegmentSortTestIndex(t, index.BM25Scoring)
	defer cleanup()
	mkReq := func() *SearchRequest {
		r := NewSearchRequestOptions(termQueryOn("body", "common"), 25, 0, true)
		r.SortByCustom(search.SortOrder{sortField("n", asNumber), &search.SortDocID{}})
		r.SearchAfter = []string{"9", "doc500"}
		r.AddFacet("s", NewFacetRequest("s", 5))
		return r
	}
	old, got := runBothPaths(t, idx, mkReq, "facets and search after")
	compareSortedResults(t, "facets and search after", old, got)
	compareFacets(t, "facets and search after", old, got)
	for i, h := range got.Hits {
		if h.Expl == nil {
			t.Fatalf("hit %d has no explanation", i)
		}
	}
}

// A score among the sorts of a pagination is as exact as the scores are: those of
// this path are float32 (the value given to continue from is the float64 of one),
// those of the regular path float64. Continuing from a page of one path in the
// other is therefore not exact, where hits tie on score: the ones that are not
// after the last hit can differ by a rounding. What is exact is a walk within the
// path: the pages, one after the other, are the hits of a search that has no
// pages, in order and with none missing or twice.
func TestPerSegmentSearchAfterAndBeforeByScoreWalkWithoutGaps(t *testing.T) {
	idx, cleanup := perSegmentSortTestIndex(t, index.BM25Scoring)
	defer cleanup()
	so := func() search.SortOrder {
		return search.SortOrder{&search.SortScore{Desc: true}, &search.SortDocID{}}
	}
	q := facetTestQueries()["or"]
	run := func(mod func(r *SearchRequest), size int) []*search.DocumentMatch {
		before := perSegmentSearches.Load()
		r := NewSearchRequestOptions(q(), size, 0, false)
		r.SortByCustom(so())
		mod(r)
		res, err := idx.Search(r)
		if err != nil {
			t.Fatal(err)
		}
		if perSegmentSearches.Load() != before+1 {
			t.Fatal("the per segment path was not taken")
		}
		return res.Hits
	}
	all := run(func(*SearchRequest) {}, 5000)
	if len(all) < 200 {
		t.Fatalf("%d hits", len(all))
	}
	ids := func(hits []*search.DocumentMatch) []string {
		rv := make([]string, len(hits))
		for i, h := range hits {
			rv[i] = h.ID
		}
		return rv
	}

	// forwards, from the start
	var walked []*search.DocumentMatch
	var after []string
	for pages := 0; ; pages++ {
		page := run(func(r *SearchRequest) { r.SearchAfter = after }, 37)
		if len(page) == 0 {
			break
		}
		if pages > len(all)/37+2 {
			t.Fatalf("pages after one another go on past the %d hits there are", len(all))
		}
		walked = append(walked, page...)
		after = afterValues(so(), page[len(page)-1])
	}
	if !reflect.DeepEqual(ids(walked), ids(all)) {
		t.Fatalf("pages after one another are %d hits, not the %d of a search with no pages",
			len(walked), len(all))
	}

	// backwards, from the end, the pages in reverse
	var back []*search.DocumentMatch
	before := afterValues(so(), all[len(all)-1])
	// (the last hit itself is not before itself)
	back = append(back, all[len(all)-1])
	for pages := 0; ; pages++ {
		page := run(func(r *SearchRequest) { r.SearchBefore = before }, 37)
		if len(page) == 0 {
			break
		}
		if pages > len(all)/37+2 {
			t.Fatalf("pages before one another go on past the %d hits there are", len(all))
		}
		back = append(append([]*search.DocumentMatch(nil), page...), back...)
		before = afterValues(so(), page[0])
	}
	if !reflect.DeepEqual(ids(back), ids(all)) {
		t.Fatalf("pages before one another are %d hits, not the %d of a search with no pages",
			len(back), len(all))
	}
}
