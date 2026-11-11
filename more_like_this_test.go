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
	"encoding/json"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

var mltTestDocs = map[string]string{
	"hotel_10025": "Quiet boutique hotel on the harbourfront with sea-view rooms and a heated rooftop pool.",
	"hotel_10063": "Boutique inn near the marina; harbourfront walks, rooftop terrace and heated pool.",
	"hotel_10098": "Modern hotel with a heated indoor pool near the city gardens.",
	"hotel_10142": "Budget lodge beside the airport terminal with 24-hour shuttle and parking.",
}

type mltIndexFactory func(t *testing.T, ids ...string) Index

// the tests run against both index types: upsidedown keeps the document ID
// as its internal ID, scorch (what cbft uses) numbers documents
var mltIndexFactories = map[string]mltIndexFactory{
	"upsidedown": func(t *testing.T, ids ...string) Index {
		t.Helper()
		idx, err := NewMemOnly(NewIndexMapping())
		if err != nil {
			t.Fatal(err)
		}
		return mltIndexDocs(t, idx, ids...)
	},
	"scorch": func(t *testing.T, ids ...string) Index {
		t.Helper()
		idx, err := NewUsing(t.TempDir(), NewIndexMapping(), scorch.Name, scorch.Name, nil)
		if err != nil {
			t.Fatal(err)
		}
		return mltIndexDocs(t, idx, ids...)
	},
}

func mltIndexDocs(t *testing.T, idx Index, ids ...string) Index {
	t.Helper()
	for _, id := range ids {
		if err := idx.Index(id, map[string]interface{}{"description": mltTestDocs[id]}); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = idx.Close() })
	return idx
}

func forEachMLTIndexType(t *testing.T, test func(t *testing.T, newIndex mltIndexFactory)) {
	for name, factory := range mltIndexFactories {
		t.Run(name, func(t *testing.T) {
			test(t, factory)
		})
	}
}

func mltHitIDs(sr *SearchResult) []string {
	rv := make([]string, 0, len(sr.Hits))
	for _, hit := range sr.Hits {
		rv = append(rv, hit.ID)
	}
	return rv
}

func assertHitIDs(t *testing.T, sr *SearchResult, want ...string) {
	t.Helper()
	got := mltHitIDs(sr)
	if len(got) != len(want) {
		t.Fatalf("expected hits %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected hits %v, got %v", want, got)
		}
	}
}

func assertHitSet(t *testing.T, sr *SearchResult, want ...string) {
	t.Helper()
	got := mltHitIDs(sr)
	if len(got) != len(want) {
		t.Fatalf("expected hits %v in any order, got %v", want, got)
	}
	seen := make(map[string]bool, len(got))
	for _, id := range got {
		seen[id] = true
	}
	for _, id := range want {
		if !seen[id] {
			t.Fatalf("expected hits %v in any order, got %v", want, got)
		}
	}
}

func TestMoreLikeThisSingleIndex(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		idx := newIndex(t, "hotel_10025", "hotel_10063", "hotel_10098", "hotel_10142")

		req := NewSearchRequest(query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"}))
		req.Explain = true
		sr, err := idx.Search(req)
		if err != nil {
			t.Fatal(err)
		}
		// 10063 shares boutique, harbourfront, rooftop, heated and pool;
		// 10098 shares hotel, heated and pool; 10142 shares nothing;
		// the source document itself is excluded
		assertHitIDs(t, sr, "hotel_10063", "hotel_10098")
		if _, ok := sr.Request.Query.(*query.MoreLikeThisQuery); !ok {
			t.Errorf("expected the echoed request to carry the more_like_this query, got %T", sr.Request.Query)
		}
		for _, hit := range sr.Hits {
			if hit.Expl == nil {
				t.Errorf("expected an explanation for hit %s", hit.ID)
			}
		}
	})
}

func TestMoreLikeThisSingleIndexLikeText(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		idx := newIndex(t, "hotel_10025", "hotel_10063", "hotel_10098", "hotel_10142")

		q := query.NewMoreLikeThisQuery([]string{"description"}, nil)
		q.LikeText = "boutique harbourfront rooftop pool"
		sr, err := idx.Search(NewSearchRequest(q))
		if err != nil {
			t.Fatal(err)
		}
		// nothing is excluded with like_text; 10025 and 10063 share every
		// term, 10098 only pool, 10142 none
		got := mltHitIDs(sr)
		if len(got) != 3 || got[2] != "hotel_10098" {
			t.Fatalf("expected 10025 and 10063 ahead of 10098, got %v", got)
		}
	})
}

func TestMoreLikeThisSingleIndexMin(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		idx := newIndex(t, "hotel_10025", "hotel_10063", "hotel_10098", "hotel_10142")

		q := query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"})
		q.SetMin(4) // 10098 shares only three terms
		sr, err := idx.Search(NewSearchRequest(q))
		if err != nil {
			t.Fatal(err)
		}
		assertHitIDs(t, sr, "hotel_10063")
	})
}

func TestMoreLikeThisSingleIndexUnknownDoc(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		idx := newIndex(t, "hotel_10025", "hotel_10063")

		sr, err := idx.Search(NewSearchRequest(
			query.NewMoreLikeThisQuery([]string{"description"}, []string{"does_not_exist"})))
		if err != nil {
			t.Fatal(err)
		}
		if sr.Total != 0 {
			t.Errorf("expected no hits for an unknown document, got %v", mltHitIDs(sr))
		}
	})
}

func TestMoreLikeThisCollectStats(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		idx := newIndex(t, "hotel_10025", "hotel_10063", "hotel_10098", "hotel_10142")
		if err := idx.Index("rep", map[string]interface{}{"description": "pool pool pool"}); err != nil {
			t.Fatal(err)
		}
		advanced, err := idx.Advanced()
		if err != nil {
			t.Fatal(err)
		}
		reader, err := advanced.Reader()
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()

		q := query.NewMoreLikeThisQuery([]string{"description"}, []string{"rep", "missing"})
		stats, err := q.CollectStats(context.Background(), reader, idx.Mapping())
		if err != nil {
			t.Fatal(err)
		}
		// only the document this reader holds is reported
		if len(stats.FoundIDs) != 1 || stats.FoundIDs[0] != "rep" || len(stats.SourceDocs) != 1 ||
			stats.SourceDocs[0].ID != "rep" || len(stats.SourceDocs[0].Terms) != 1 {
			t.Fatalf("unexpected stats %+v", stats)
		}
		// tf comes from the posting entry of the source document, df from
		// the term dictionary: pool occurs three times in rep and in four
		// documents, out of five in the index
		if st := stats.SourceDocs[0].Terms[0]; st.Field != "description" || st.Term != "pool" ||
			st.TF != 3 || st.DF != 4 || st.DocCount != 5 {
			t.Errorf("unexpected term stat %+v", st)
		}

		// a document this reader does not hold contributes nothing
		q = query.NewMoreLikeThisQuery([]string{"description"}, []string{"missing"})
		stats, err = q.CollectStats(context.Background(), reader, idx.Mapping())
		if err != nil {
			t.Fatal(err)
		}
		if len(stats.FoundIDs) != 0 || len(stats.SourceDocs) != 0 || len(stats.Terms) != 0 {
			t.Errorf("expected empty stats for a missing document, got %+v", stats)
		}

		// like_text analyzes the text with the field's analyzer and looks
		// the document frequencies up
		q = query.NewMoreLikeThisQuery([]string{"description"}, nil)
		q.LikeText = "Pool pool unknownword"
		stats, err = q.CollectStats(context.Background(), reader, idx.Mapping())
		if err != nil {
			t.Fatal(err)
		}
		if len(stats.SourceDocs) != 0 || len(stats.Terms) != 2 {
			t.Fatalf("unexpected like_text stats %+v", stats)
		}
		if st := stats.Terms[0]; st.Term != "pool" || st.TF != 2 || st.DF != 4 || st.DocCount != 5 {
			t.Errorf("unexpected like_text term stat %+v", st)
		}
		if st := stats.Terms[1]; st.Term != "unknownword" || st.TF != 1 || st.DF != 0 {
			t.Errorf("unexpected like_text term stat %+v", st)
		}

		// an invalid node is refused here too, not only by Searcher
		q = query.NewMoreLikeThisQuery(nil, []string{"rep"})
		if _, err = q.CollectStats(context.Background(), reader, idx.Mapping()); err == nil {
			t.Error("expected a validation error for a node without fields")
		}
	})
}

func TestMoreLikeThisFieldWithoutDocValues(t *testing.T) {
	// the terms of a document are read from doc values; a field without
	// them is refused rather than recovered by scanning the index
	im := NewIndexMapping()
	fm := mapping.NewTextFieldMapping()
	fm.DocValues = false
	im.DefaultMapping.AddFieldMappingsAt("description", fm)
	idx, err := NewMemOnly(im)
	if err != nil {
		t.Fatal(err)
	}
	mltIndexDocs(t, idx, "hotel_10025", "hotel_10063")

	_, err = idx.Search(NewSearchRequest(
		query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"})))
	if err == nil || !strings.Contains(err.Error(), "no doc values") {
		t.Errorf("expected a doc values error, got %v", err)
	}

	// a non-text field is refused as well
	im = NewIndexMapping()
	im.DefaultMapping.AddFieldMappingsAt("description", mapping.NewKeywordFieldMapping())
	im.DefaultMapping.AddFieldMappingsAt("stars", mapping.NewNumericFieldMapping())
	idx2, err := NewMemOnly(im)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx2.Close() })
	if err := idx2.Index("h", map[string]interface{}{"description": "x", "stars": 3}); err != nil {
		t.Fatal(err)
	}
	_, err = idx2.Search(NewSearchRequest(
		query.NewMoreLikeThisQuery([]string{"stars"}, []string{"h"})))
	if err == nil || !strings.Contains(err.Error(), "not a text field") {
		t.Errorf("expected a field type error, got %v", err)
	}
	// keyword fields are text fields with doc values: accepted
	if _, err = idx2.Search(NewSearchRequest(
		query.NewMoreLikeThisQuery([]string{"description"}, []string{"h"}))); err != nil {
		t.Errorf("expected a keyword field to be accepted, got %v", err)
	}

	// dynamically mapped fields follow docvalues_dynamic
	im = NewIndexMapping()
	im.DocValuesDynamic = false
	idx3, err := NewMemOnly(im)
	if err != nil {
		t.Fatal(err)
	}
	mltIndexDocs(t, idx3, "hotel_10025")
	_, err = idx3.Search(NewSearchRequest(
		query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"})))
	if err == nil || !strings.Contains(err.Error(), "no doc values") {
		t.Errorf("expected a doc values error for a dynamic field, got %v", err)
	}
}

func TestMoreLikeThisMultiField(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		idx := newIndex(t)
		docs := map[string]map[string]interface{}{
			"src":   {"title": "harbour lights", "description": "quiet harbourfront rooms"},
			"both":  {"title": "harbour view", "description": "harbourfront terrace"},
			"title": {"title": "city lights", "description": "airport shuttle"},
			"none":  {"title": "budget lodge", "description": "parking"},
		}
		for id, doc := range docs {
			if err := idx.Index(id, doc); err != nil {
				t.Fatal(err)
			}
		}
		q := query.NewMoreLikeThisQuery([]string{"title", "description"}, []string{"src"})
		sr, err := idx.Search(NewSearchRequest(q))
		if err != nil {
			t.Fatal(err)
		}
		// "both" shares harbour and harbourfront across the two fields,
		// "title" only lights in the title; terms from either field count
		assertHitIDs(t, sr, "both", "title")
	})
}

func TestMoreLikeThisAlias(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		// the source document lives in the first index only; phase A has
		// to find it there and phase B must still search both indexes
		i1 := newIndex(t, "hotel_10025", "hotel_10142")
		i2 := newIndex(t, "hotel_10063", "hotel_10098")
		alias := NewIndexAlias(i1, i2)

		q := query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"})
		req := NewSearchRequest(q)
		req.Explain = true
		sr, err := alias.Search(req)
		if err != nil {
			t.Fatal(err)
		}
		assertHitIDs(t, sr, "hotel_10063", "hotel_10098")
		if sr.Status.Successful != 2 || sr.Status.Failed != 0 {
			t.Errorf("unexpected status %+v", sr.Status)
		}
		// the rewrite is internal: the caller's request and its echo
		// still carry the more_like_this node
		if req.Query != q {
			t.Error("expected the caller's query to be restored after the search")
		}
		if _, ok := sr.Request.Query.(*query.MoreLikeThisQuery); !ok {
			t.Errorf("expected the echoed request to carry the more_like_this query, got %T", sr.Request.Query)
		}
	})
}

func TestMoreLikeThisAliasNested(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		// the same through an alias of aliases, as a cbft coordinator
		// sees remote nodes
		i1 := newIndex(t, "hotel_10025")
		i2 := newIndex(t, "hotel_10063")
		i3 := newIndex(t, "hotel_10098", "hotel_10142")
		inner := NewIndexAlias(i2, i3)
		root := NewIndexAlias(i1, inner)

		sr, err := root.Search(NewSearchRequest(
			query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"})))
		if err != nil {
			t.Fatal(err)
		}
		// each member scores with its own statistics, so with
		// one-document members the order between 10063 and 10098 is not
		// meaningful; both must be found and the source document must not
		assertHitSet(t, sr, "hotel_10063", "hotel_10098")
	})
}

func TestMoreLikeThisAliasLikeText(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		i1 := newIndex(t, "hotel_10025", "hotel_10142")
		i2 := newIndex(t, "hotel_10063", "hotel_10098")
		alias := NewIndexAlias(i1, i2)

		q := query.NewMoreLikeThisQuery([]string{"description"}, nil)
		q.LikeText = "boutique harbourfront rooftop pool"
		sr, err := alias.Search(NewSearchRequest(q))
		if err != nil {
			t.Fatal(err)
		}
		got := mltHitIDs(sr)
		if len(got) != 3 || got[2] != "hotel_10098" {
			t.Fatalf("expected 10025 and 10063 ahead of 10098, got %v", got)
		}
	})
}

func TestMoreLikeThisAliasMissingDoc(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		i1 := newIndex(t, "hotel_10025")
		i2 := newIndex(t, "hotel_10063")
		alias := NewIndexAlias(i1, i2)

		sr, err := alias.Search(NewSearchRequest(
			query.NewMoreLikeThisQuery([]string{"description"}, []string{"missing"})))
		if err != nil {
			t.Fatal(err)
		}
		if sr.Total != 0 {
			t.Errorf("expected no hits for an unknown document, got %v", mltHitIDs(sr))
		}
	})
}

func TestMoreLikeThisAliasInConjunction(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		// a more_like_this node inside a larger query is substituted in
		// place
		i1 := newIndex(t, "hotel_10025", "hotel_10142")
		i2 := newIndex(t, "hotel_10063", "hotel_10098")
		alias := NewIndexAlias(i1, i2)

		city := query.NewTermQuery("city")
		city.SetField("description")
		cq := query.NewConjunctionQuery([]query.Query{
			query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"}),
			city,
		})
		sr, err := alias.Search(NewSearchRequest(cq))
		if err != nil {
			t.Fatal(err)
		}
		assertHitIDs(t, sr, "hotel_10098")
	})
}

func TestMoreLikeThisAliasSourceInNestedAlias(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		// the source document sits two levels down: the inner alias merges
		// its members' contributions and the root merges that again
		i1 := newIndex(t, "hotel_10142")
		i2 := newIndex(t, "hotel_10025")
		i3 := newIndex(t, "hotel_10063", "hotel_10098")
		inner := NewIndexAlias(i2, i3)
		root := NewIndexAlias(i1, inner)

		q := query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"})
		q.SetMin(4)
		sr, err := root.Search(NewSearchRequest(q))
		if err != nil {
			t.Fatal(err)
		}
		// 10098 shares only three terms and min is 4
		assertHitIDs(t, sr, "hotel_10063")

		q = query.NewMoreLikeThisQuery([]string{"description"}, nil)
		q.LikeText = "boutique harbourfront rooftop pool"
		sr, err = root.Search(NewSearchRequest(q))
		if err != nil {
			t.Fatal(err)
		}
		assertHitSet(t, sr, "hotel_10025", "hotel_10063", "hotel_10098")
	})
}

func TestMoreLikeThisAliasDuplicateSourceDoc(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		// the same source document held by two members is counted once,
		// so its term frequencies are not doubled
		i1 := newIndex(t, "hotel_10025", "hotel_10063")
		i2 := newIndex(t, "hotel_10025", "hotel_10098")
		alias := NewIndexAlias(i1, i2)

		q := query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"})
		q.MinTermFreq = 2 // every term occurs once in the document
		sr, err := alias.Search(NewSearchRequest(q))
		if err != nil {
			t.Fatal(err)
		}
		if sr.Total != 0 {
			t.Errorf("expected no hits with min_term_freq 2, got %v", mltHitIDs(sr))
		}
		q.MinTermFreq = 1
		sr, err = alias.Search(NewSearchRequest(q))
		if err != nil {
			t.Fatal(err)
		}
		assertHitIDs(t, sr, "hotel_10063", "hotel_10098")
	})
}

func TestMoreLikeThisAliasScoreFusionKeepsBoost(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		i1 := newIndex(t, "hotel_10025", "hotel_10142")
		i2 := newIndex(t, "hotel_10063", "hotel_10098")
		alias := NewIndexAlias(i1, i2)

		q := query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"})
		q.SetBoost(2)
		req := NewSearchRequest(q)
		req.Score = ScoreRRF
		sr, err := alias.Search(req)
		if err != nil {
			t.Fatal(err)
		}
		assertHitSet(t, sr, "hotel_10063", "hotel_10098")
		// the rescorer lifts and restores the top-level boost on the
		// caller's query, which must not be the rewritten one
		if q.Boost() != 2 {
			t.Errorf("expected the caller's boost to survive, got %v", q.Boost())
		}
		if req.Query != q {
			t.Error("expected the caller's query to be left in the request")
		}
	})
}

func TestMoreLikeThisAliasGlobalBM25(t *testing.T) {
	newBM25Index := func(t *testing.T, ids ...string) Index {
		t.Helper()
		im := NewIndexMapping()
		im.ScoringModel = index.BM25Scoring
		idx, err := NewUsing(t.TempDir(), im, scorch.Name, scorch.Name, nil)
		if err != nil {
			t.Fatal(err)
		}
		return mltIndexDocs(t, idx, ids...)
	}
	i1 := newBM25Index(t, "hotel_10025", "hotel_10142")
	i2 := newBM25Index(t, "hotel_10063", "hotel_10098")
	alias := NewIndexAlias(i1, i2)
	if err := alias.SetIndexMapping(i1.Mapping()); err != nil {
		t.Fatal(err)
	}

	// global scoring adds the bm25 preSearch next to the mlt one; the
	// field cardinality gathered must cover the compared field or the
	// term searchers fail in the second phase
	ctx := context.WithValue(context.Background(), search.SearchTypeKey, search.GlobalScoring)
	q := query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"})
	sr, err := alias.SearchInContext(ctx, NewSearchRequest(q))
	if err != nil {
		t.Fatal(err)
	}
	assertHitIDs(t, sr, "hotel_10063", "hotel_10098")
}

func TestMoreLikeThisAliasInBooleanFilter(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		i1 := newIndex(t, "hotel_10025", "hotel_10142")
		i2 := newIndex(t, "hotel_10063", "hotel_10098")
		alias := NewIndexAlias(i1, i2)

		// the similar documents narrowed by a filter clause
		bq := NewBooleanQuery()
		bq.AddMust(query.NewMatchAllQuery())
		bq.AddFilter(query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"}))
		sr, err := alias.Search(NewSearchRequest(bq))
		if err != nil {
			t.Fatal(err)
		}
		assertHitSet(t, sr, "hotel_10063", "hotel_10098")
	})
}

func TestMoreLikeThisAliasValidation(t *testing.T) {
	i1 := mltIndexFactories["upsidedown"](t, "hotel_10025")
	i2 := mltIndexFactories["upsidedown"](t, "hotel_10063")
	alias := NewIndexAlias(i1, i2)

	// an invalid node is refused at the alias just as on a single index
	bad := &query.MoreLikeThisQuery{LikeIDs: []string{"hotel_10025"}, MaxQueryTerms: 5}
	if _, err := alias.Search(NewSearchRequest(bad)); err == nil {
		t.Error("expected a validation error for a node without fields")
	}
	if _, err := i1.Search(NewSearchRequest(bad)); err == nil {
		t.Error("expected a validation error for a node without fields")
	}
}

func TestMLTPreSearchResultProcessorMerge(t *testing.T) {
	term := func(name string, tf, df, n uint64) *search.MLTTermStat {
		return &search.MLTTermStat{Field: "f", Term: name, TF: tf, DF: df, DocCount: n}
	}
	contribution := func(key string, stats *search.MLTStats) *SearchResult {
		return &SearchResult{MLTStats: map[string]*search.MLTStats{key: stats}}
	}
	// termFigures is the comparable part of a term stat
	type termFigures struct {
		Field, Term  string
		TF, DF, Docs uint64
	}
	figures := func(st *search.MLTTermStat) termFigures {
		return termFigures{st.Field, st.Term, st.TF, st.DF, st.DocCount}
	}
	candidatesByTerm := func(stats *search.MLTStats) map[string]termFigures {
		rv := make(map[string]termFigures)
		for _, st := range stats.Candidates() {
			rv[st.Term] = figures(st)
		}
		return rv
	}
	// merge runs the contributions through a fresh processor in the given
	// order; every member is named "" as NewMemOnly indexes are
	merge := func(contributions ...*SearchResult) *search.MLTStats {
		p := newMLTPreSearchResultProcessor()
		for _, c := range contributions {
			p.add(c, "")
		}
		sr := &SearchResult{}
		p.finalize(sr)
		return sr.MLTStats["0"]
	}

	// like_ids: member 1 holds a, member 2 holds a copy of a and b
	m1 := contribution("0", &search.MLTStats{
		FoundIDs: []string{"a"},
		SourceDocs: []*search.MLTSourceDoc{{ID: "a", Terms: []*search.MLTTermStat{
			term("pool", 1, 4, 100), term("rooftop", 1, 2, 100),
		}}},
	})
	m2 := contribution("0", &search.MLTStats{
		FoundIDs: []string{"a", "b"},
		SourceDocs: []*search.MLTSourceDoc{
			{ID: "a", Terms: []*search.MLTTermStat{term("pool", 1, 6, 300), term("rooftop", 1, 3, 300)}},
			{ID: "b", Terms: []*search.MLTTermStat{term("pool", 2, 6, 300), term("heated", 1, 9, 300)}},
		},
	})
	m3 := contribution("0", &search.MLTStats{})

	want := map[string]termFigures{
		// tf 1 (a, counted once) + 2 (b); df and N from both members
		"pool": {"f", "pool", 3, 10, 400},
		// only a has it, but both members hold a and both reported it
		"rooftop": {"f", "rooftop", 1, 5, 400},
		// only b has it, held by member 2 alone
		"heated": {"f", "heated", 1, 9, 300},
	}
	for name, order := range map[string][]*SearchResult{
		"m1 m2 m3": {m1, m2, m3},
		"m2 m1 m3": {m2, m1, m3},
		"m3 m2 m1": {m3, m2, m1},
	} {
		merged := merge(order...)
		if merged == nil || len(merged.FoundIDs) != 2 || merged.FoundIDs[0] != "a" || merged.FoundIDs[1] != "b" ||
			len(merged.SourceDocs) != 2 {
			t.Fatalf("%s: unexpected merged stats %+v", name, merged)
		}
		got := candidatesByTerm(merged)
		if len(got) != len(want) {
			t.Fatalf("%s: expected %d terms, got %+v", name, len(want), got)
		}
		for termName, w := range want {
			if got[termName] != w {
				t.Errorf("%s: %s: expected %+v, got %+v", name, termName, w, got[termName])
			}
		}
	}

	// the merged form of a nested alias is merged again the same way: a
	// document it accounts for is not counted twice, its df and N count
	// as one more contribution
	inner := merge(m2, m3)
	for name, order := range map[string][]*SearchResult{
		"leaf then inner": {m1, contribution("0", inner)},
		"inner then leaf": {contribution("0", inner), m1},
	} {
		got := candidatesByTerm(merge(order...))
		for termName, w := range want {
			if got[termName] != w {
				t.Errorf("%s: %s: expected %+v, got %+v", name, termName, w, got[termName])
			}
		}
	}

	// like_text: every member answers for the same terms; tf stays, df
	// and the document count add up to index-wide figures, whatever the
	// order and even with members of the same name
	// a member analyzing the text differently may report another tf; the
	// largest one is kept so that the order of the answers does not matter
	t1 := contribution("0", &search.MLTStats{Terms: []*search.MLTTermStat{term("pool", 1, 4, 100)}})
	t2 := contribution("0", &search.MLTStats{Terms: []*search.MLTTermStat{term("pool", 2, 6, 300)}})
	for name, order := range map[string][]*SearchResult{"t1 t2": {t1, t2}, "t2 t1": {t2, t1}} {
		merged := merge(order...)
		if merged == nil || len(merged.Terms) != 1 || len(merged.SourceDocs) != 0 {
			t.Fatalf("%s: unexpected merged like_text stats %+v", name, merged)
		}
		if st := figures(merged.Terms[0]); st != (termFigures{"f", "pool", 2, 10, 400}) {
			t.Errorf("%s: unexpected like_text merge %+v", name, st)
		}
	}
}

// valueQuery is a query type that is not a pointer, as an embedder may
// define: comparing two interface values holding it would panic
type valueQuery struct {
	ids []string
}

func (q valueQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping,
	options search.SearcherOptions) (search.Searcher, error) {
	return query.NewMatchAllQuery().Searcher(ctx, i, m, options)
}

func TestIndexAliasValueTypeQuery(t *testing.T) {
	i1 := mltIndexFactories["upsidedown"](t, "hotel_10025")
	i2 := mltIndexFactories["upsidedown"](t, "hotel_10063")
	alias := NewIndexAlias(i1, i2)

	sr, err := alias.Search(NewSearchRequest(valueQuery{ids: []string{"x"}}))
	if err != nil {
		t.Fatal(err)
	}
	assertHitSet(t, sr, "hotel_10025", "hotel_10063")
}

func TestMoreLikeThisAliasTwoNodes(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		i1 := newIndex(t, "hotel_10025", "hotel_10142")
		i2 := newIndex(t, "hotel_10063", "hotel_10098")
		alias := NewIndexAlias(i1, i2)

		// documents like 10025 or like 10098: each node is resolved from
		// its own source document and excludes only that one
		dq := query.NewDisjunctionQuery([]query.Query{
			query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"}),
			query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10098"}),
		})
		sr, err := alias.Search(NewSearchRequest(dq))
		if err != nil {
			t.Fatal(err)
		}
		// 10025 comes back through the second node (hotel, heated, pool)
		// and 10098 through the first; 10142 shares nothing with either
		assertHitSet(t, sr, "hotel_10063", "hotel_10098", "hotel_10025")
	})
}

func TestMoreLikeThisAliasFieldErrorIsHard(t *testing.T) {
	// a field problem is an error of the request, at the alias as on the
	// index, not a failed member in an otherwise successful result
	im := NewIndexMapping()
	fm := mapping.NewTextFieldMapping()
	fm.DocValues = false
	im.DefaultMapping.AddFieldMappingsAt("description", fm)
	newIndex := func(ids ...string) Index {
		idx, err := NewMemOnly(im)
		if err != nil {
			t.Fatal(err)
		}
		return mltIndexDocs(t, idx, ids...)
	}
	alias := NewIndexAlias(newIndex("hotel_10025"), newIndex("hotel_10063"))
	req := NewSearchRequest(query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"}))
	if _, err := alias.Search(req); err == nil || !strings.Contains(err.Error(), "no doc values") {
		t.Errorf("expected a doc values error from the alias, got %v", err)
	}
	// an alias with its own mapping checks against that mapping
	alias = NewIndexAlias(newIndex("hotel_10025"), newIndex("hotel_10063"))
	if err := alias.SetIndexMapping(im); err != nil {
		t.Fatal(err)
	}
	if _, err := alias.Search(req); err == nil || !strings.Contains(err.Error(), "no doc values") {
		t.Errorf("expected a doc values error from the alias with a mapping, got %v", err)
	}
}

func TestMoreLikeThisValidateFields(t *testing.T) {
	im := NewIndexMapping()
	// a type mapping with a nested document, a renamed field and a field
	// without doc values
	hotel := mapping.NewDocumentMapping()
	noDV := mapping.NewTextFieldMapping()
	noDV.DocValues = false
	hotel.AddFieldMappingsAt("summary", noDV)
	renamed := mapping.NewTextFieldMapping()
	renamed.Name = "desc"
	hotel.AddFieldMappingsAt("description", renamed)
	review := mapping.NewDocumentMapping()
	review.AddFieldMappingsAt("body", mapping.NewTextFieldMapping())
	hotel.AddSubDocumentMapping("review", review)
	im.AddDocumentMapping("hotel", hotel)

	cases := map[string]bool{
		"review.body": true,  // nested, text, doc values
		"desc":        true,  // the field's own name
		"summary":     false, // no doc values
		"title":       true,  // not mapped: dynamic, docvalues_dynamic on
	}
	for field, ok := range cases {
		q := query.NewMoreLikeThisQuery([]string{field}, []string{"h"})
		err := q.ValidateFields(im)
		if ok && err != nil {
			t.Errorf("%s: expected the field to be accepted, got %v", field, err)
		}
		if !ok && err == nil {
			t.Errorf("%s: expected the field to be refused", field)
		}
	}
	// no mapping to check against: nothing is refused
	if err := query.NewMoreLikeThisQuery([]string{"summary"}, []string{"h"}).ValidateFields(nil); err != nil {
		t.Errorf("expected no error without a mapping, got %v", err)
	}
	// a like_text only analyzes its input: no doc values needed
	lt := query.NewMoreLikeThisQuery([]string{"summary"}, nil)
	lt.LikeText = "some words"
	if err := lt.ValidateFields(im); err != nil {
		t.Errorf("expected like_text to be accepted on a field without doc values, got %v", err)
	}
	// a nested field's values live in child documents: refused for now
	nested := NewIndexMapping()
	reviews := mapping.NewNestedDocumentMapping()
	reviews.AddFieldMappingsAt("body", mapping.NewTextFieldMapping())
	nested.DefaultMapping.AddSubDocumentMapping("reviews", reviews)
	nq := query.NewMoreLikeThisQuery([]string{"reviews.body"}, []string{"h"})
	if err := nq.ValidateFields(nested); err == nil || !strings.Contains(err.Error(), "nested") {
		t.Errorf("expected a nested field to be refused, got %v", err)
	}
	if err := query.NewMoreLikeThisQuery([]string{"description"}, []string{"h"}).ValidateFields(nested); err != nil {
		t.Errorf("expected a field outside the nested mapping to be accepted, got %v", err)
	}
}

func TestMoreLikeThisLikeTextWithoutDocValues(t *testing.T) {
	// the terms of a like_text come from analyzing it, so the field does
	// not need doc values
	im := NewIndexMapping()
	fm := mapping.NewTextFieldMapping()
	fm.DocValues = false
	im.DefaultMapping.AddFieldMappingsAt("description", fm)
	idx, err := NewMemOnly(im)
	if err != nil {
		t.Fatal(err)
	}
	mltIndexDocs(t, idx, "hotel_10025", "hotel_10063", "hotel_10142")
	q := query.NewMoreLikeThisQuery([]string{"description"}, nil)
	q.LikeText = "boutique harbourfront rooftop pool"
	sr, err := idx.Search(NewSearchRequest(q))
	if err != nil {
		t.Fatal(err)
	}
	assertHitSet(t, sr, "hotel_10025", "hotel_10063")
}

func TestIndexAliasMappingThroughNestedAliases(t *testing.T) {
	// the scoring model used to rank the terms is taken from the members,
	// also when the members are aliases without a mapping of their own
	im := NewIndexMapping()
	im.ScoringModel = index.BM25Scoring
	newIndex := func(ids ...string) Index {
		idx, err := NewUsing(t.TempDir(), im, scorch.Name, scorch.Name, nil)
		if err != nil {
			t.Fatal(err)
		}
		return mltIndexDocs(t, idx, ids...)
	}
	inner1 := NewIndexAlias(newIndex("hotel_10025"), newIndex("hotel_10142"))
	inner2 := NewIndexAlias(newIndex("hotel_10063"), newIndex("hotel_10098"))
	root := NewIndexAlias(inner1, inner2)
	if m := root.aliasMapping(); m == nil || !isBM25Enabled(m) {
		t.Errorf("expected the members' bm25 mapping through the nested aliases, got %v", m)
	}
	// and the search works through that tree
	sr, err := root.Search(NewSearchRequest(query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"})))
	if err != nil {
		t.Fatal(err)
	}
	assertHitSet(t, sr, "hotel_10063", "hotel_10098")
}

func TestRewriteOnlyFlags(t *testing.T) {
	// a rewrite_only request only needs the more_like_this lookup
	if got := rewriteOnlyFlags(nil); got != nil {
		t.Errorf("expected nil flags to stay nil, got %+v", got)
	}
	if got := rewriteOnlyFlags(&preSearchFlags{knn: true, synonyms: true, bm25: true}); got != nil {
		t.Errorf("expected no preSearch without a more_like_this node, got %+v", got)
	}
	got := rewriteOnlyFlags(&preSearchFlags{knn: true, synonyms: true, bm25: true, mlt: true})
	if got == nil || !got.mlt || got.knn || got.synonyms || got.bm25 {
		t.Errorf("expected only the mlt flag kept, got %+v", got)
	}
}

func TestMoreLikeThisRequestJSONRoundTrip(t *testing.T) {
	// the node nested in a boolean query, plus the preSearch marker, as
	// a request travels between nodes of a cluster
	var req SearchRequest
	err := json.Unmarshal([]byte(`{
		"query": {"must": {"conjuncts": [
			{"more_like_this": {"fields": ["description"], "like_ids": ["hotel_10025"], "min": 1}},
			{"term": "pool", "field": "description"}
		]}},
		"pre_search_data": {"_mlt_pre_search_data_key": true},
		"size": 5
	}`), &req)
	if err != nil {
		t.Fatal(err)
	}
	nodes := query.ExtractMoreLikeThisQueries(req.Query)
	if len(nodes) != 1 || nodes[0].LikeIDs[0] != "hotel_10025" || nodes[0].Min != "1" ||
		nodes[0].MaxQueryTerms != query.DefaultMoreLikeThisMaxQueryTerms {
		t.Fatalf("unexpected parsed node %+v", nodes)
	}
	if done, ok := req.PreSearchData[search.MLTPreSearchDataKey].(bool); !ok || !done {
		t.Errorf("expected the preSearch marker to be parsed, got %+v", req.PreSearchData)
	}
	data, err := json.Marshal(&req)
	if err != nil {
		t.Fatal(err)
	}
	var back SearchRequest
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	if n := query.ExtractMoreLikeThisQueries(back.Query); len(n) != 1 || n[0].LikeIDs[0] != "hotel_10025" {
		t.Errorf("round trip lost the node: %s", data)
	}
	if done, ok := back.PreSearchData[search.MLTPreSearchDataKey].(bool); !ok || !done {
		t.Errorf("round trip lost the preSearch marker: %s", data)
	}
}

func TestMoreLikeThisMinPercentEndToEnd(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		idx := newIndex(t, "hotel_10025", "hotel_10063", "hotel_10098", "hotel_10142")
		// hotel_10025 analyzes to ten terms; 50% of them is five, which
		// 10063 shares and 10098 (three) does not
		q := query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"})
		q.SetMinPercent(50)
		sr, err := idx.Search(NewSearchRequest(q))
		if err != nil {
			t.Fatal(err)
		}
		assertHitIDs(t, sr, "hotel_10063")
	})
}

func TestMoreLikeThisLikeTextByField(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		idx := newIndex(t)
		docs := map[string]map[string]interface{}{
			"login":     {"title": "Login fails after password reset", "description": "the login page returns a 500 error"},
			"export":    {"title": "Export hangs", "description": "the export page returns a 500 error"},
			"unrelated": {"title": "Dark mode", "description": "add a dark theme to settings"},
		}
		for id, doc := range docs {
			if err := idx.Index(id, doc); err != nil {
				t.Fatal(err)
			}
		}
		// a draft ticket with its own title and body: the title text is
		// compared on the title field only, the body on the description
		q := query.NewMoreLikeThisQuery(nil, nil)
		q.LikeTextByField = map[string]string{
			"title":       "Login broken after reset",
			"description": "login page shows a 500",
		}
		q.SetMin(2)
		sr, err := idx.Search(NewSearchRequest(q))
		if err != nil {
			t.Fatal(err)
		}
		// login shares reset (title) and login, page, 500 (description);
		// export shares only page and 500 in the description
		got := mltHitIDs(sr)
		if len(got) != 2 || got[0] != "login" || got[1] != "export" {
			t.Fatalf("expected login ahead of export, got %v", got)
		}

		// the same words as one text compared on both fields rank the
		// same here, but "login" in the title text is then also looked
		// for in the description, which per-field text avoids
		q = query.NewMoreLikeThisQuery([]string{"title", "description"}, nil)
		q.LikeText = "Login broken after reset login page shows a 500"
		sr, err = idx.Search(NewSearchRequest(q))
		if err != nil {
			t.Fatal(err)
		}
		if len(mltHitIDs(sr)) == 0 {
			t.Error("expected hits for the single-text form")
		}
	})
}

func TestMoreLikeThisRewriteOnlySingleIndex(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		idx := newIndex(t, "hotel_10025", "hotel_10063", "hotel_10098", "hotel_10142")

		req := NewSearchRequest(query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"}))
		req.RewriteOnly = true
		sr, err := idx.Search(req)
		if err != nil {
			t.Fatal(err)
		}
		if len(sr.Hits) != 0 || sr.Total != 0 || sr.Status.Successful != 1 {
			t.Errorf("expected no hits from a rewrite_only request, got %+v", sr)
		}
		if sr.Request != nil {
			t.Error("expected the request echoed only with explain")
		}
		if sr.RewrittenQuery == nil {
			t.Fatal("expected the rewritten query")
		}
		bq, ok := sr.RewrittenQuery.Query.(*query.BooleanQuery)
		if !ok {
			t.Fatalf("expected a boolean query, got %T", sr.RewrittenQuery.Query)
		}
		disj := bq.Must.(*query.ConjunctionQuery).Conjuncts[0].(*query.DisjunctionQuery)
		if len(disj.Disjuncts) != 10 || disj.Notes == nil {
			t.Errorf("expected the ten analyzed terms with notes, got %d terms, notes %s", len(disj.Disjuncts), disj.Notes)
		}
		var notes query.MLTTermNotes
		if err := json.Unmarshal(disj.Disjuncts[0].(*query.TermQuery).Notes, &notes); err != nil {
			t.Fatal(err)
		}
		if len(notes.Sources) != 1 || notes.Sources[0] != "hotel_10025" || notes.TF != 1 || notes.N != 4 || notes.DF == 0 {
			t.Errorf("unexpected term notes %+v", notes)
		}

		// the rewritten query runs as a plain query and finds the same
		// documents as the more_like_this query
		back, err := idx.Search(NewSearchRequest(sr.RewrittenQuery.Query))
		if err != nil {
			t.Fatal(err)
		}
		assertHitIDs(t, back, "hotel_10063", "hotel_10098")

		// and it survives a JSON round trip of the result
		data, err := json.Marshal(sr)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(data), `"rewritten_query":{"must"`) || !strings.Contains(string(data), `"notes":{"sources":["hotel_10025"]`) {
			t.Errorf("unexpected result JSON %s", data)
		}
		var decoded SearchResult
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded.RewrittenQuery == nil {
			t.Fatal("expected the rewritten query to decode")
		}
		again, err := idx.Search(NewSearchRequest(decoded.RewrittenQuery.Query))
		if err != nil {
			t.Fatal(err)
		}
		assertHitIDs(t, again, "hotel_10063", "hotel_10098")

		// reading the source document's terms counts towards the cost
		if strings.Contains(t.Name(), "scorch") && sr.Cost == 0 {
			t.Error("expected the doc value reads of the source document in the cost")
		}

		// a normal run returns neither the query nor notes
		sr, err = idx.Search(NewSearchRequest(query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"})))
		if err != nil {
			t.Fatal(err)
		}
		if sr.RewrittenQuery != nil || len(sr.Hits) != 2 {
			t.Errorf("expected hits and no rewritten query from a normal run, got %+v", sr)
		}
	})
}

func TestMoreLikeThisRewriteOnlyAlias(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		i1 := newIndex(t, "hotel_10025", "hotel_10142")
		i2 := newIndex(t, "hotel_10063", "hotel_10098")
		alias := NewIndexAlias(i1, i2)

		q := query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"})
		req := NewSearchRequest(q)
		req.RewriteOnly = true
		sr, err := alias.Search(req)
		if err != nil {
			t.Fatal(err)
		}
		// both members answered the lookup, nothing was searched
		if len(sr.Hits) != 0 || sr.Status.Total != 2 || sr.Status.Successful != 2 {
			t.Errorf("expected an empty result over two members, got %+v", sr)
		}
		if sr.RewrittenQuery == nil {
			t.Fatal("expected the rewritten query")
		}
		if req.Query != q {
			t.Error("expected the caller's query untouched")
		}
		back, err := alias.Search(NewSearchRequest(sr.RewrittenQuery.Query))
		if err != nil {
			t.Fatal(err)
		}
		assertHitIDs(t, back, "hotel_10063", "hotel_10098")

		// a node whose document no member holds rewrites to match_none
		req = NewSearchRequest(query.NewMoreLikeThisQuery([]string{"description"}, []string{"missing"}))
		req.RewriteOnly = true
		sr, err = alias.Search(req)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := sr.RewrittenQuery.Query.(*query.MatchNoneQuery); !ok {
			t.Errorf("expected match_none for a missing document, got %T", sr.RewrittenQuery.Query)
		}

		// without a more_like_this node the query comes back as sent
		tq := query.NewTermQuery("pool")
		tq.SetField("description")
		req = NewSearchRequest(tq)
		req.RewriteOnly = true
		sr, err = alias.Search(req)
		if err != nil {
			t.Fatal(err)
		}
		if sr.RewrittenQuery == nil || sr.RewrittenQuery.Query != query.Query(tq) || len(sr.Hits) != 0 {
			t.Errorf("expected the query itself back, got %+v", sr)
		}
	})
}

func TestMoreLikeThisExplainReturnsRewrittenQuery(t *testing.T) {
	forEachMLTIndexType(t, func(t *testing.T, newIndex mltIndexFactory) {
		i1 := newIndex(t, "hotel_10025", "hotel_10142")
		i2 := newIndex(t, "hotel_10063", "hotel_10098")
		for name, idx := range map[string]Index{"alias": NewIndexAlias(i1, i2), "index": newIndex(t, "hotel_10025", "hotel_10063", "hotel_10098")} {
			req := NewSearchRequest(query.NewMoreLikeThisQuery([]string{"description"}, []string{"hotel_10025"}))
			req.Explain = true
			sr, err := idx.Search(req)
			if err != nil {
				t.Fatal(err)
			}
			if len(sr.Hits) != 2 || sr.RewrittenQuery == nil {
				t.Errorf("%s: expected hits and the rewritten query with explain, got %+v", name, sr)
				continue
			}
			if _, ok := sr.Request.Query.(*query.MoreLikeThisQuery); !ok {
				t.Errorf("%s: expected the echoed request to carry the more_like_this query", name)
			}
			bq := sr.RewrittenQuery.Query.(*query.BooleanQuery)
			disj := bq.Must.(*query.ConjunctionQuery).Conjuncts[0].(*query.DisjunctionQuery)
			if disj.Notes == nil || disj.Disjuncts[0].(*query.TermQuery).Notes == nil {
				t.Errorf("%s: expected notes on the rewritten query", name)
			}
			for _, hit := range sr.Hits {
				if hit.Expl == nil {
					t.Errorf("%s: expected an explanation for hit %s", name, hit.ID)
				}
			}
		}
	})
}

func TestSearchRequestRewriteOnlyAndNotesJSON(t *testing.T) {
	var req SearchRequest
	err := json.Unmarshal([]byte(`{
		"query": {"more_like_this": {"fields": ["description"], "like_ids": ["hotel_10025"]}},
		"rewrite_only": true,
		"notes": {"app": "similar-hotels widget"}
	}`), &req)
	if err != nil {
		t.Fatal(err)
	}
	if !req.RewriteOnly || string(req.Notes) != `{"app": "similar-hotels widget"}` {
		t.Errorf("expected rewrite_only and notes parsed, got %v %s", req.RewriteOnly, req.Notes)
	}
	data, err := json.Marshal(&req)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"rewrite_only":true`) || !strings.Contains(string(data), `"notes":{"app":"similar-hotels widget"}`) {
		t.Errorf("expected both to marshal, got %s", data)
	}
	// a plain request carries neither key
	plain, err := json.Marshal(NewSearchRequest(query.NewMatchAllQuery()))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain), "rewrite_only") || strings.Contains(string(plain), "notes") {
		t.Errorf("expected no new keys on a plain request, got %s", plain)
	}
}
