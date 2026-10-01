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
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
)

// TestParallelSegmentSearchEligible pins which request shapes may let §7
// fan out the root disjunction: only pure descending-score ranking without
// facets or pagination cursors, because each shard keeps just its top-K by
// score.
func TestParallelSegmentSearchEligible(t *testing.T) {
	q := query.NewDisjunctionQuery([]query.Query{
		query.NewTermQuery("a"), query.NewTermQuery("b"), query.NewTermQuery("c"),
	})
	mk := func() *SearchRequest { return NewSearchRequest(q) }

	cases := []struct {
		name string
		req  func() *SearchRequest
		want bool
	}{
		{"default (score desc)", mk, true},
		{"explicit -_score", func() *SearchRequest { r := mk(); r.SortBy([]string{"-_score"}); return r }, true},
		{"score ascending", func() *SearchRequest { r := mk(); r.SortBy([]string{"_score"}); return r }, false},
		{"field sort", func() *SearchRequest { r := mk(); r.SortBy([]string{"k"}); return r }, false},
		{"field then score", func() *SearchRequest { r := mk(); r.SortBy([]string{"k", "-_score"}); return r }, false},
		{"facets", func() *SearchRequest { r := mk(); r.AddFacet("kf", NewFacetRequest("k", 5)); return r }, false},
		{"search_after", func() *SearchRequest { r := mk(); r.SearchAfter = []string{"0.5"}; return r }, false},
		{"search_before", func() *SearchRequest { r := mk(); r.SearchBefore = []string{"0.5"}; return r }, false},
		{"empty sort order", func() *SearchRequest { r := mk(); r.Sort = search.SortOrder{}; return r }, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := parallelSegmentSearchEligible(c.req()); got != c.want {
				t.Fatalf("eligible = %v, want %v", got, c.want)
			}
		})
	}
}
