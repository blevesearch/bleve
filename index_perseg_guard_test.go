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
	"errors"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/collector"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

// plainReader hides everything an index reader offers beyond index.IndexReader
// itself, in particular the per segment postings.
type plainReader struct {
	index.IndexReader
}

func termQueryOn(field, term string) *query.TermQuery {
	q := query.NewTermQuery(term)
	q.SetField(field)
	return q
}

// What a query can be searched by the per segment path is the query's to say, per
// reader: search.ErrPerSegmentUnsupported is what its PerSegmentSearcher returns
// when it can't, and the search goes to the regular path then.
func TestPerSegmentSearcherIsRefusedWhenItCannotServe(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()

	adv, err := idx.Advanced()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := adv.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()

	m := NewIndexMapping()
	ctx := context.Background()
	opts := search.SearcherOptions{}

	build := func(q query.Query, r index.IndexReader, o search.SearcherOptions) error {
		t.Helper()
		ps, err := q.(query.PerSegmentQuery).PerSegmentSearcher(ctx, r, m, o)
		if err == nil {
			_ = ps.Close()
		}
		return err
	}
	term := termQueryOn("body", "common")

	// the scorch snapshot is a per segment index reader
	if _, ok := reader.(searcher.PerSegmentIndexReader); !ok {
		t.Fatal("expected the scorch reader to be a PerSegmentIndexReader")
	}
	for name, o := range map[string]search.SearcherOptions{
		"scored":          opts,
		"without scores":  {Score: ScoreNone},
		"with an explain": {Explain: true},
	} {
		if err := build(term, reader, o); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}

	refused := map[string]struct {
		q query.Query
		r index.IndexReader
		o search.SearcherOptions
	}{
		// a reader that can't give the postings by segment
		"a reader that isn't a per segment one": {term, plainReader{reader}, opts},
		// term vectors need the matches' positions
		"term vectors": {term, reader, search.SearcherOptions{IncludeTermVectors: true}},
		"a clause with term vectors": {query.NewConjunctionQuery(terms("common", "even")), reader,
			search.SearcherOptions{IncludeTermVectors: true}},
		"a fuzzy match": {func() query.Query {
			q := query.NewMatchQuery("common")
			q.SetField("body")
			q.SetFuzziness(1)
			return q
		}(), reader, opts},
		"a boolean with a filter": {func() query.Query {
			b := booleanOf(terms("common"), nil, nil, 0)
			b.Filter = termQueryOn("body", "even")
			return b
		}(), reader, opts},
		"a boolean that only excludes": {booleanOf(nil, nil, terms("common"), 0), reader, opts},
		"a disjunction with scores broken down": {func() query.Query {
			d := query.NewDisjunctionQuery(terms("common", "even"))
			d.RetrieveScoreBreakdown(true)
			return d
		}(), reader, opts},
	}
	for name, c := range refused {
		if err := build(c.q, c.r, c.o); !errors.Is(err, search.ErrPerSegmentUnsupported) {
			t.Errorf("%s: expected ErrPerSegmentUnsupported, got %v", name, err)
		}
	}

	// the search that goes to the regular path is answered all the same
	if res, err := idx.Search(NewSearchRequest(func() query.Query {
		q := query.NewMatchQuery("common")
		q.SetField("body")
		q.SetFuzziness(1)
		return q
	}())); err != nil || res.Total == 0 {
		t.Errorf("a fuzzy match: %v, %v", res, err)
	}
}

// requests the per segment path must stay out of, even though they are for a
// term query: neither the searcher nor the collector may be the per segment
// ones, and the answer has to be the one the regular path gives.
func TestPerSegmentSearchStaysOutOfUnsupportedRequests(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.BM25Scoring, nil)
	defer cleanup()

	req := func() *SearchRequest {
		return NewSearchRequestOptions(termQueryOn("body", "common"), 10, 0, false)
	}

	type testCase struct {
		name string
		ctx  func() context.Context
		req  func() *SearchRequest
	}
	bg := func() context.Context { return context.Background() }
	cases := []testCase{
		{
			name: "pre search data with bm25 stats",
			ctx:  bg,
			req: func() *SearchRequest {
				r := req()
				r.PreSearchData = map[string]interface{}{
					search.BM25PreSearchDataKey: &search.BM25Stats{
						DocCount:         5000,
						FieldCardinality: map[string]int{"body": 100000},
					},
				}
				return r
			},
		},
		{
			name: "pre search data with synonyms",
			ctx:  bg,
			req: func() *SearchRequest {
				r := req()
				r.PreSearchData = map[string]interface{}{
					search.SynonymPreSearchDataKey: search.FieldTermSynonymMap{
						"body": {"common": []string{"rare"}},
					},
				}
				return r
			},
		},
		{
			name: "nested search",
			ctx: func() context.Context {
				return context.WithValue(context.Background(), search.NestedSearchKey, true)
			},
			req: req,
		},
		{
			name: "score fusion",
			ctx: func() context.Context {
				return context.WithValue(context.Background(), search.ScoreFusionKey, true)
			},
			req: req,
		},
		{
			name: "custom document match handler",
			ctx: func() context.Context {
				return context.WithValue(context.Background(), search.MakeDocumentMatchHandlerKey,
					search.MakeDocumentMatchHandler(collector.MakeTopNDocumentMatchHandler))
			},
			req: req,
		},
	}

	for _, tc := range cases {
		before := perSegmentSearches.Load()
		res, err := idx.SearchInContext(tc.ctx(), tc.req())
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if res.Total == 0 {
			t.Errorf("%s: no hits", tc.name)
		}
		if perSegmentSearches.Load() != before {
			t.Errorf("%s: went through the per segment path", tc.name)
		}
	}

	// the synonym case really did search the synonym too
	r := req()
	r.PreSearchData = map[string]interface{}{
		search.SynonymPreSearchDataKey: search.FieldTermSynonymMap{
			"body": {"rare": []string{"common"}},
		},
	}
	r.Query = termQueryOn("body", "rare")
	res, err := idx.Search(r)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total < 100 {
		t.Errorf("synonym of 'rare' to 'common' should match many docs, got %d", res.Total)
	}

	// sanity: the same request, with nothing in the way, does use the path
	before := perSegmentSearches.Load()
	if _, err := idx.SearchInContext(context.Background(), req()); err != nil {
		t.Fatal(err)
	}
	if perSegmentSearches.Load() != before+1 {
		t.Error("the plain request should have gone through the per segment path")
	}
}

// the same request, through index aliases, answers the same whether the per
// segment path is enabled or not.
func TestPerSegmentSearchThroughIndexAlias(t *testing.T) {
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		idx1, cleanup1 := perSegmentTestIndex(t, model, nil)
		idx2, cleanup2 := perSegmentTestIndex(t, model, nil)

		for name, alias := range map[string]IndexAlias{
			"one index":   NewIndexAlias(idx1),
			"two indexes": NewIndexAlias(idx1, idx2),
		} {
			mk := func() *SearchRequest {
				return NewSearchRequestOptions(termQueryOn("body", "common"), 25, 3, false)
			}

			perSegmentSearchEnabled.Store(false)
			old, err := alias.Search(mk())
			perSegmentSearchEnabled.Store(true)
			if err != nil {
				t.Fatalf("%s/%s: %v", model, name, err)
			}
			before := perSegmentSearches.Load()
			got, err := alias.Search(mk())
			if err != nil {
				t.Fatalf("%s/%s: %v", model, name, err)
			}
			t.Logf("%s/%s: per segment searches used: %d", model, name, perSegmentSearches.Load()-before)

			if old.Total == 0 {
				t.Fatalf("%s/%s: no hits", model, name)
			}
			// both indexes have the same content, so a tie between them can come
			// back in either order: which index a hit is from isn't compared
			compareSearchResultsIgnoringIndex(t, model+"/"+name, old, got)
		}
		cleanup2()
		cleanup1()
	}
}

// A per segment searcher that can't be built closes what it had built by then: every
// term searcher that was started is finished, whether the query was refused halfway
// or failed, and whatever its shape.
func TestPerSegmentSearcherBuildsThatFailCloseTheirClauses(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()
	adv, err := idx.Advanced()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := adv.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()

	termSearchers := func() (started, finished interface{}) {
		stats := idx.StatsMap()["index"].(map[string]interface{})
		return stats["term_searchers_started"], stats["term_searchers_finished"]
	}
	fuzzy := func() query.Query {
		q := query.NewMatchQuery("common")
		q.SetField("body")
		q.SetFuzziness(1)
		return q
	}
	filtered := booleanOf(terms("common"), terms("even"), nil, 0)
	filtered.Filter = termQueryOn("body", "five")

	orig := searcher.DisjunctionMaxClauseCount
	searcher.DisjunctionMaxClauseCount = 2
	defer func() { searcher.DisjunctionMaxClauseCount = orig }()

	for name, q := range map[string]query.Query{
		// the clause that is refused comes last, after the others were built
		"an and with a fuzzy match last": query.NewConjunctionQuery([]query.Query{termQueryOn("body", "common"), termQueryOn("body", "even"), fuzzy()}),
		"an or with a fuzzy match last":  query.NewDisjunctionQuery([]query.Query{termQueryOn("body", "common"), fuzzy()}),
		"a boolean with a filter":        filtered,
		"a boolean that only excludes":   booleanOf(nil, nil, terms("common"), 0),
		"a boolean, its should refused":  booleanOf(terms("common"), []query.Query{fuzzy()}, terms("five"), 0),
		// too many clauses: all of them built, then the disjunction fails
		"an or of too many clauses": query.NewDisjunctionQuery(terms("common", "even", "five")),
	} {
		before1, before2 := termSearchers()
		ps, err := q.(query.PerSegmentQuery).PerSegmentSearcher(context.Background(), reader, NewIndexMapping(), search.SearcherOptions{})
		if err == nil {
			_ = ps.Close()
			t.Errorf("%s: expected an error", name)
		}
		started, finished := termSearchers()
		// the readers of the snapshot count; compare what this build changed
		if started == before1 && finished == before2 {
			continue // no term searcher was started
		}
		if toInt(started)-toInt(before1) != toInt(finished)-toInt(before2) {
			t.Errorf("%s: %d term searchers started, %d finished", name,
				toInt(started)-toInt(before1), toInt(finished)-toInt(before2))
		}
	}
}

func toInt(v interface{}) int {
	switch n := v.(type) {
	case uint64:
		return int(n)
	case int:
		return n
	case int64:
		return int(n)
	}
	return 0
}
