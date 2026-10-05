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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

// matchNothing is a match of a text that analyzes to no tokens: it is searched for
// with a searcher of no matches.
func matchNothing() query.Query {
	q := query.NewMatchQuery("")
	q.SetField("body")
	return q
}

// Clauses that can't match anything are clauses a tree of per segment searchers
// has to take, whether or not they're alone: they are regular searchers, of
// several kinds, and could not be told from a regular tree otherwise.
func TestPerSegmentClausesThatMatchNothing(t *testing.T) {
	cases := []shapeCase{
		{"and with a match of nothing", func() query.Query {
			return query.NewConjunctionQuery([]query.Query{termQueryOn("body", "common"), matchNothing()})
		}, true, false},
		{"or with a match of nothing", func() query.Query {
			return query.NewDisjunctionQuery([]query.Query{termQueryOn("body", "common"), matchNothing(), termQueryOn("body", "even")})
		}, true, false},
		{"bool: should and a must that matches nothing", func() query.Query {
			return booleanOf([]query.Query{matchNothing()}, terms("even"), nil, 0)
		}, true, false},
		{"bool: must and a should that matches nothing", func() query.Query {
			return booleanOf(terms("even"), []query.Query{matchNothing()}, nil, 0)
		}, true, false},
		{"bool: a required should that matches nothing", func() query.Query {
			return booleanOf(terms("even"), []query.Query{matchNothing()}, nil, 1)
		}, true, false},
		{"bool: must not that matches nothing", func() query.Query {
			return booleanOf(terms("even"), terms("five"), []query.Query{matchNothing()}, 0)
		}, true, false},
		{"bool: nested, with all of them", func() query.Query {
			return booleanOf([]query.Query{query.NewConjunctionQuery([]query.Query{matchNothing(), termQueryOn("body", "even")})},
				[]query.Query{query.NewDisjunctionQuery([]query.Query{matchNothing()})}, terms("rare"), 0)
		}, true, false},
	}

	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()
	for _, sc := range cases {
		for _, sz := range []struct{ size, from int }{{10, 0}, {5000, 0}, {0, 0}} {
			what := fmt.Sprintf("%s size=%d", sc.name, sz.size)
			mk := func() *SearchRequest { return NewSearchRequestOptions(sc.q(), sz.size, sz.from, false) }

			perSegmentSearchEnabled.Store(false)
			old, err := idx.Search(mk())
			perSegmentSearchEnabled.Store(true)
			if err != nil {
				t.Fatalf("%s: regular: %v", what, err)
			}
			got, err := idx.Search(mk())
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
			compareCompositeResults(t, what, old, got)
		}
	}
}

// A disjunction with more clauses than the limit is an error, the same one on
// both paths.
func TestPerSegmentDisjunctionClauseLimit(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()

	orig := searcher.DisjunctionMaxClauseCount
	searcher.DisjunctionMaxClauseCount = 2
	defer func() { searcher.DisjunctionMaxClauseCount = orig }()

	q := func() query.Query { return query.NewDisjunctionQuery(terms("common", "even", "five")) }
	for _, enabled := range []bool{false, true} {
		perSegmentSearchEnabled.Store(enabled)
		_, err := idx.Search(NewSearchRequest(q()))
		perSegmentSearchEnabled.Store(true)
		if err == nil || !strings.Contains(err.Error(), "TooManyClauses") {
			t.Fatalf("per segment path %v: expected a TooManyClauses error, got %v", enabled, err)
		}
	}
	// and at the limit it is fine
	searcher.DisjunctionMaxClauseCount = 3
	if _, err := idx.Search(NewSearchRequest(q())); err != nil {
		t.Fatalf("3 clauses at a limit of 3: %v", err)
	}
}

// The per segment searcher is not a Searcher, and a query that has one builds it
// itself, apart from the regular searcher: the two kinds can't be combined, as the
// compiler would not have it.
func TestPerSegmentSearchersAreTheirOwnKind(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()
	adv, _ := idx.Advanced()
	reader, err := adv.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()

	m := NewIndexMapping()
	opts := search.SearcherOptions{}
	for name, q := range map[string]query.Query{
		"term":         termQueryOn("body", "common"),
		"conjunction":  query.NewConjunctionQuery(terms("common", "even")),
		"disjunction":  query.NewDisjunctionQuery(terms("common", "even")),
		"boolean":      booleanOf(terms("common"), terms("even"), terms("five"), 0),
		"match":        query.NewMatchQuery("common even"),
		"query string": query.NewQueryStringQuery("+body:common body:even"),
	} {
		if mq, ok := q.(*query.MatchQuery); ok {
			mq.SetField("body")
		}
		pq, ok := q.(query.PerSegmentQuery)
		if !ok {
			t.Fatalf("%s: %T has no per segment searcher", name, q)
		}
		ps, err := pq.PerSegmentSearcher(context.Background(), reader, m, opts)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if _, isSearcher := ps.(search.Searcher); isSearcher {
			t.Fatalf("%s: %T is a Searcher as well", name, ps)
		}
		if _, isSearcher := ps.(interface {
			Next(*search.SearchContext) (*search.DocumentMatch, error)
		}); isSearcher {
			t.Fatalf("%s: %T can be iterated one DocumentMatch at a time", name, ps)
		}
		if _, ok := ps.(search.OptimizedPerSegmentSearcher); !ok && name != "match" {
			t.Fatalf("%s: %T has no optimized path", name, ps)
		}
		if err := ps.Close(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}
