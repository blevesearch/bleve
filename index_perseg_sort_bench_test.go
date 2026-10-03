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
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

// buildSortBenchIndex is the corpus of buildAllocBenchIndexModel with fields to
// sort by: n, a number of many values (a price, a timestamp), and s, a keyword of
// a thousand values with a skewed share.
func buildSortBenchIndex(b *testing.B, numDocs, numSegments int, model string) (Index, func()) {
	b.Helper()
	dir := createTmpIndexPath(b)
	im := NewIndexMapping()
	if model == "bm25" {
		im.ScoringModel = index.BM25Scoring
	}
	im.DefaultMapping.AddFieldMappingsAt("s", mapping.NewKeywordFieldMapping())
	idx, err := NewUsing(dir, im, scorch.Name, scorch.Name, map[string]interface{}{
		"scorchMergePlanOptions": map[string]interface{}{"MaxSegmentSize": 2},
	})
	if err != nil {
		b.Fatal(err)
	}
	rnd := rand.New(rand.NewSource(1))
	zipf := rand.NewZipf(rnd, 1.2, 4, 4999)
	names := rand.NewZipf(rnd, 1.1, 2, 999)
	markers := []struct {
		term string
		pct  float64
	}{{"mk100", 100}, {"mk50", 50}, {"mk10", 10}, {"mk1", 1}, {"mk01", 0.1}}
	per := numDocs / numSegments
	for bt := 0; bt < numSegments; bt++ {
		batch := idx.NewBatch()
		for i := bt * per; i < (bt+1)*per; i++ {
			var sb strings.Builder
			for k := 3 + rnd.Intn(38); k > 0; k-- {
				sb.WriteString("w")
				sb.WriteString(strconv.FormatUint(zipf.Uint64(), 10))
				sb.WriteByte(' ')
			}
			for _, m := range markers {
				if rnd.Float64()*100 < m.pct {
					for r := 1 + rnd.Intn(3); r > 0; r-- {
						sb.WriteString(m.term)
						sb.WriteByte(' ')
					}
				}
			}
			doc := map[string]interface{}{
				"body": sb.String(),
				"n":    float64(rnd.Intn(1000000)),
				"s":    "name" + strconv.FormatUint(names.Uint64(), 10),
			}
			if err := batch.Index(strconv.Itoa(i), doc); err != nil {
				b.Fatal(err)
			}
		}
		persisted := make(chan error, 1)
		batch.SetPersistedCallback(func(err error) { persisted <- err })
		if err := idx.Batch(batch); err != nil {
			b.Fatal(err)
		}
		if err := <-persisted; err != nil {
			b.Fatal(err)
		}
	}
	return idx, func() { _ = idx.Close(); cleanupTmpIndexPath(b, dir) }
}

// BenchmarkSortedSearch compares a search sorted by a field on the per segment
// path and on the regular one:
//
//	go test -run xxx -bench SortedSearch -benchmem -benchtime 100x
//
// PERSEG_SORT_ONLY=regular|perseg runs one of the two.
func BenchmarkSortedSearch(b *testing.B) {
	idx, cleanup := buildSortBenchIndex(b, 200000, 8, os.Getenv("PERSEG_MODEL"))
	defer cleanup()
	term := func(t string) query.Query {
		q := query.NewTermQuery(t)
		q.SetField("body")
		return q
	}
	queries := []struct {
		name string
		q    query.Query
	}{
		{"term10pct", term("mk10")},
		{"term50pct", term("mk50")},
		{"or", query.NewDisjunctionQuery([]query.Query{term("mk10"), term("mk1"), term("mk01")})},
		{"and", query.NewConjunctionQuery([]query.Query{term("mk50"), term("mk10")})},
	}
	sorts := []struct {
		name string
		mk   func() search.SortOrder
	}{
		{"n", func() search.SortOrder {
			return search.SortOrder{&search.SortField{Field: "n", Type: search.SortFieldAsNumber}}
		}},
		{"n-desc", func() search.SortOrder {
			return search.SortOrder{&search.SortField{Field: "n", Type: search.SortFieldAsNumber, Desc: true}}
		}},
		{"s", func() search.SortOrder { return search.SortOrder{&search.SortField{Field: "s"}} }},
		{"s+n", func() search.SortOrder {
			return search.SortOrder{&search.SortField{Field: "s"}, &search.SortField{Field: "n", Type: search.SortFieldAsNumber}}
		}},
	}
	only := os.Getenv("PERSEG_SORT_ONLY")
	for _, qy := range queries {
		for _, st := range sorts {
			for _, size := range []int{10, 100} {
				for _, path := range []struct {
					name    string
					enabled bool
				}{{"regular", false}, {"perseg", true}} {
					if only != "" && only != path.name {
						continue
					}
					name := qy.name + "/" + st.name + "/size" + strconv.Itoa(size) + "/" + path.name
					b.Run(name, func(b *testing.B) {
						perSegmentSearchEnabled.Store(path.enabled)
						defer perSegmentSearchEnabled.Store(true)
						mk := func() *SearchRequest {
							r := NewSearchRequestOptions(qy.q, size, 0, false)
							r.SortByCustom(st.mk())
							return r
						}
						for i := 0; i < 5; i++ {
							if _, err := idx.Search(mk()); err != nil {
								b.Fatal(err)
							}
						}
						b.ReportAllocs()
						b.ResetTimer()
						for i := 0; i < b.N; i++ {
							if _, err := idx.Search(mk()); err != nil {
								b.Fatal(err)
							}
						}
					})
				}
			}
		}
	}
}

// BenchmarkFacetedSearch compares searches with facets, and a page after a hit,
// on the per segment path and on the regular one:
//
//	go test -run xxx -bench FacetedSearch -benchmem -benchtime 20x
func BenchmarkFacetedSearch(b *testing.B) {
	idx, cleanup := buildSortBenchIndex(b, 200000, 8, os.Getenv("PERSEG_MODEL"))
	defer cleanup()
	term := func(t string) query.Query {
		q := query.NewTermQuery(t)
		q.SetField("body")
		return q
	}
	queries := []struct {
		name string
		q    query.Query
	}{
		{"term10pct", term("mk10")},
		{"or", query.NewDisjunctionQuery([]query.Query{term("mk10"), term("mk1"), term("mk01")})},
	}
	lo, hi := 500000.0, 500000.0
	shapes := []struct {
		name string
		mod  func(r *SearchRequest)
	}{
		{"facet-terms", func(r *SearchRequest) { r.AddFacet("s", NewFacetRequest("s", 10)) }},
		{"facet-ranges", func(r *SearchRequest) {
			fr := NewFacetRequest("n", 2)
			fr.AddNumericRange("low", nil, &lo)
			fr.AddNumericRange("high", &hi, nil)
			r.AddFacet("n", fr)
		}},
		{"facet-terms-size0", func(r *SearchRequest) { r.Size = 0; r.AddFacet("s", NewFacetRequest("s", 10)) }},
		{"after-n", func(r *SearchRequest) {
			r.SortByCustom(search.SortOrder{&search.SortField{Field: "n", Type: search.SortFieldAsNumber}, &search.SortDocID{}})
			r.SearchAfter = []string{"500000", "100"}
		}},
	}
	only := os.Getenv("PERSEG_SORT_ONLY")
	for _, qy := range queries {
		for _, sh := range shapes {
			for _, path := range []struct {
				name    string
				enabled bool
			}{{"regular", false}, {"perseg", true}} {
				if only != "" && only != path.name {
					continue
				}
				b.Run(qy.name+"/"+sh.name+"/"+path.name, func(b *testing.B) {
					perSegmentSearchEnabled.Store(path.enabled)
					defer perSegmentSearchEnabled.Store(true)
					mk := func() *SearchRequest {
						r := NewSearchRequestOptions(qy.q, 10, 0, false)
						sh.mod(r)
						return r
					}
					for i := 0; i < 3; i++ {
						if _, err := idx.Search(mk()); err != nil {
							b.Fatal(err)
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						if _, err := idx.Search(mk()); err != nil {
							b.Fatal(err)
						}
					}
				})
			}
		}
	}
}
