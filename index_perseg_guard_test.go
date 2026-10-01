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

// TermQuery.Searcher is what decides, per reader, whether a per segment
// searcher is handed out.
func TestPerSegmentTermQuerySearcherChecksTheReader(t *testing.T) {
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
	opts := search.SearcherOptions{}
	optedIn := context.WithValue(context.Background(), search.PerSegmentSearchKey, true)
	notOptedIn := context.Background()

	kind := func(ctx context.Context, r index.IndexReader, o search.SearcherOptions) string {
		t.Helper()
		s, err := termQueryOn("body", "common").Searcher(ctx, r, m, o)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		if _, ok := s.(search.PerSegmentSearcher); ok {
			return "per-segment"
		}
		if _, ok := s.(*searcher.TermSearcher); ok {
			return "regular"
		}
		return "other"
	}

	// the scorch snapshot is a per segment index reader, and the caller opted in
	if _, ok := reader.(searcher.PerSegmentIndexReader); !ok {
		t.Fatal("expected the scorch reader to be a PerSegmentIndexReader")
	}
	if got := kind(optedIn, reader, opts); got != "per-segment" {
		t.Errorf("scorch reader, opted in: got %s searcher", got)
	}

	// a reader that is not one never gets it, even if the caller opted in
	if got := kind(optedIn, plainReader{reader}, opts); got != "regular" {
		t.Errorf("plain reader, opted in: got %s searcher", got)
	}

	// without the opt in nobody gets it
	if got := kind(notOptedIn, reader, opts); got != "regular" {
		t.Errorf("scorch reader, not opted in: got %s searcher", got)
	}

	// a search that wants no scores is served too
	if got := kind(optedIn, reader, search.SearcherOptions{Score: ScoreNone}); got != "per-segment" {
		t.Errorf("scorch reader, opted in, score none: got %s searcher", got)
	}

	// and neither does anything that needs more than a score
	for name, o := range map[string]search.SearcherOptions{
		"explain":      {Explain: true},
		"term vectors": {IncludeTermVectors: true},
	} {
		if got := kind(optedIn, reader, o); got != "regular" {
			t.Errorf("scorch reader, opted in, %s: got %s searcher", name, got)
		}
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
