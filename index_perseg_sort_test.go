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
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

// perSegmentSortTestIndex is perSegmentTestIndex with fields to sort by, made to
// have ties (a handful of values for a thousand and more docs), docs that lack a
// field, a field with several values, dates and points; over many segments, with
// updated and deleted docs.
func perSegmentSortTestIndex(t *testing.T, scoringModel string) (Index, func()) {
	t.Helper()
	dir := createTmpIndexPath(t)

	im := NewIndexMapping()
	im.ScoringModel = scoringModel
	im.DefaultMapping.AddFieldMappingsAt("s", mapping.NewKeywordFieldMapping())
	im.DefaultMapping.AddFieldMappingsAt("tags", mapping.NewKeywordFieldMapping())
	im.DefaultMapping.AddFieldMappingsAt("loc", mapping.NewGeoPointFieldMapping())

	idx, err := NewUsing(dir, im, scorch.Name, scorch.Name, map[string]interface{}{
		"scorchMergePlanOptions": map[string]interface{}{"MaxSegmentSize": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	applyBatch := func(b *Batch) {
		persisted := make(chan error, 1)
		b.SetPersistedCallback(func(err error) { persisted <- err })
		if err := idx.Batch(b); err != nil {
			t.Fatal(err)
		}
		if err := <-persisted; err != nil {
			t.Fatal(err)
		}
	}

	rnd := rand.New(rand.NewSource(7))
	words := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
	names := []string{"ant", "bee", "cat", "dog", "eel", "fox", "gnu", "hen", "ibis", "jay", "kiwi", "lynx"}
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	makeDoc := func(i int) map[string]interface{} {
		toks := []string{"common"}
		if i%2 == 0 {
			toks = append(toks, "even")
		}
		if i%5 == 0 {
			toks = append(toks, "five")
		}
		if rnd.Intn(10) < 3 {
			toks = append(toks, "mid")
		}
		for k := rnd.Intn(7); k > 0; k-- {
			toks = append(toks, "dup")
		}
		for k := rnd.Intn(40); k > 0; k-- {
			toks = append(toks, words[rnd.Intn(len(words))])
		}
		rnd.Shuffle(len(toks), func(a, b int) { toks[a], toks[b] = toks[b], toks[a] })
		doc := map[string]interface{}{
			"body": strings.Join(toks, " "),
			"d":    base.AddDate(0, 0, rnd.Intn(30)).Format(time.RFC3339),
			"loc":  map[string]interface{}{"lon": -122.0 + float64(rnd.Intn(20))*0.1, "lat": 37.0 + float64(rnd.Intn(20))*0.1},
		}
		// docs without a value for a field are the ones to be put first or last
		if i%11 != 0 {
			doc["n"] = float64(rnd.Intn(23))
		}
		if i%13 != 0 {
			doc["s"] = names[rnd.Intn(len(names))]
		}
		if i%7 != 0 {
			tags := []string{}
			for k := 1 + rnd.Intn(3); k > 0; k-- {
				tags = append(tags, names[rnd.Intn(len(names))])
			}
			doc["tags"] = tags
		}
		return doc
	}

	const numDocs = 1600
	for start := 0; start < numDocs; start += 200 {
		b := idx.NewBatch()
		for i := start; i < start+200; i++ {
			if err := b.Index(fmt.Sprintf("doc%d", i), makeDoc(i)); err != nil {
				t.Fatal(err)
			}
		}
		applyBatch(b)
	}
	b := idx.NewBatch()
	for k := 0; k < 150; k++ {
		i := rnd.Intn(numDocs)
		_ = b.Index(fmt.Sprintf("doc%d", i), makeDoc(i))
	}
	applyBatch(b)
	b = idx.NewBatch()
	for k := 0; k < 100; k++ {
		b.Delete(fmt.Sprintf("doc%d", rnd.Intn(numDocs)))
	}
	applyBatch(b)

	return idx, func() {
		_ = idx.Close()
		cleanupTmpIndexPath(t, dir)
	}
}

// compareSortedResults checks that the per segment path's hits are the regular
// path's, to the hit: same total (exact: nothing is pruned when sorting), same
// places, same sort values, same hit numbers; scores are within float32 precision.
func compareSortedResults(t *testing.T, what string, old, got *SearchResult) {
	t.Helper()
	compareSortedResultsOpt(t, what, old, got, false)
}

// compareSortedResultsIgnoringIndex is for the results of several indexes with the
// same content, where hits that tie are in either order between the indexes.
func compareSortedResultsIgnoringIndex(t *testing.T, what string, old, got *SearchResult) {
	t.Helper()
	compareSortedResultsOpt(t, what, old, got, true)
}

func compareSortedResultsOpt(t *testing.T, what string, old, got *SearchResult, ignoreIndex bool) {
	t.Helper()
	if old.Total != got.Total || old.TotalRelation != got.TotalRelation {
		t.Fatalf("%s: total %d (%v), want %d (%v)", what, got.Total, got.TotalRelation, old.Total, old.TotalRelation)
	}
	if !sameScore(old.MaxScore, got.MaxScore) {
		t.Fatalf("%s: max score %v, want %v", what, got.MaxScore, old.MaxScore)
	}
	if len(old.Hits) != len(got.Hits) {
		t.Fatalf("%s: %d hits, want %d", what, len(got.Hits), len(old.Hits))
	}
	for i := range old.Hits {
		o, g := old.Hits[i], got.Hits[i]
		if o.ID != g.ID || (!ignoreIndex && o.Index != g.Index) {
			t.Fatalf("%s: hit %d is %s/%s (sort %q), want %s/%s (sort %q)", what, i, g.Index, g.ID, g.Sort, o.Index, o.ID, o.Sort)
		}
		if strings.Join(o.Sort, "|") != strings.Join(g.Sort, "|") {
			t.Fatalf("%s: hit %d sort %q, want %q", what, i, g.Sort, o.Sort)
		}
		if strings.Join(o.DecodedSort, "|") != strings.Join(g.DecodedSort, "|") {
			t.Fatalf("%s: hit %d decoded sort %q, want %q", what, i, g.DecodedSort, o.DecodedSort)
		}
		if !sameScore(o.Score, g.Score) {
			t.Fatalf("%s: hit %d scores %v, want %v", what, i, g.Score, o.Score)
		}
		if o.HitNumber != g.HitNumber {
			t.Fatalf("%s: hit %d is the %dth match, want the %dth", what, i, g.HitNumber, o.HitNumber)
		}
	}
}

// compareScoreTiedResults is compareSortedResults for hits ordered by score and then
// by something else. Scores that are the same to float32 precision are one tie for
// this path, ordered by the next sort; the regular path computes them in float64,
// where the sums of the same terms in another order differ in the twelfth digit and
// so aren't a tie at all. So the hits whose scores are alike are compared as a set
// (all but the last group, which the end of the page cuts: some of it may be other
// hits of the same score).
func compareScoreTiedResults(t *testing.T, what string, old, got *SearchResult) {
	t.Helper()
	if old.Total != got.Total || old.TotalRelation != got.TotalRelation {
		t.Fatalf("%s: total %d (%v), want %d (%v)", what, got.Total, got.TotalRelation, old.Total, old.TotalRelation)
	}
	if !sameScore(old.MaxScore, got.MaxScore) {
		t.Fatalf("%s: max score %v, want %v", what, got.MaxScore, old.MaxScore)
	}
	if len(old.Hits) != len(got.Hits) {
		t.Fatalf("%s: %d hits, want %d", what, len(got.Hits), len(old.Hits))
	}
	for i := range old.Hits {
		if !sameScore(old.Hits[i].Score, got.Hits[i].Score) {
			t.Fatalf("%s: hit %d scores %v, want %v", what, i, got.Hits[i].Score, old.Hits[i].Score)
		}
	}
	for i := 0; i < len(old.Hits); {
		j := i + 1
		for j < len(old.Hits) && sameScore(old.Hits[j].Score, old.Hits[j-1].Score) {
			j++
		}
		if j < len(old.Hits) {
			want := map[string]bool{}
			for _, h := range old.Hits[i:j] {
				want[h.ID] = true
			}
			for _, h := range got.Hits[i:j] {
				if !want[h.ID] {
					t.Fatalf("%s: hits %d..%d (all scoring %v) include %s, which the regular search has elsewhere",
						what, i, j-1, old.Hits[i].Score, h.ID)
				}
			}
		}
		i = j
	}
}

func sortField(field string, mods ...func(*search.SortField)) *search.SortField {
	f := &search.SortField{Field: field}
	for _, m := range mods {
		m(f)
	}
	return f
}

func desc(f *search.SortField) { f.Desc = true }
func missingFirst(f *search.SortField) {
	f.Missing = search.SortFieldMissingFirst
}
func asNumber(f *search.SortField) { f.Type = search.SortFieldAsNumber }
func asDate(f *search.SortField)   { f.Type = search.SortFieldAsDate }
func minMode(f *search.SortField)  { f.Mode = search.SortFieldMin }
func maxMode(f *search.SortField)  { f.Mode = search.SortFieldMax }

// Hits ordered by something other than the score come out of the per segment path
// as they do of the regular one: for sorts by numbers, strings, dates, the values
// of a field that has several, docs that have no value, the document id, a
// distance, the score as one sort among others, in both directions; ties on every
// sort going to the doc found first; a page from anywhere, past the end, and none.
func TestPerSegmentSortedSearchMatchesRegularSearch(t *testing.T) {
	geo, err := search.NewSortGeoDistance("loc", "km", -121.0, 37.8, false)
	if err != nil {
		t.Fatal(err)
	}
	_ = geo
	sorts := map[string]func() search.SortOrder{
		"number":               func() search.SortOrder { return search.SortOrder{sortField("n", asNumber)} },
		"number desc":          func() search.SortOrder { return search.SortOrder{sortField("n", asNumber, desc)} },
		"number missing first": func() search.SortOrder { return search.SortOrder{sortField("n", asNumber, missingFirst)} },
		"number auto":          func() search.SortOrder { return search.SortOrder{sortField("n")} },
		"string":               func() search.SortOrder { return search.SortOrder{sortField("s")} },
		"string desc":          func() search.SortOrder { return search.SortOrder{sortField("s", desc)} },
		"date":                 func() search.SortOrder { return search.SortOrder{sortField("d", asDate)} },
		"tags min":             func() search.SortOrder { return search.SortOrder{sortField("tags", minMode)} },
		"tags max desc":        func() search.SortOrder { return search.SortOrder{sortField("tags", maxMode, desc)} },
		"two fields":           func() search.SortOrder { return search.SortOrder{sortField("s"), sortField("n", asNumber, desc)} },
		"three fields": func() search.SortOrder {
			return search.SortOrder{sortField("n", asNumber), sortField("s", desc), sortField("tags", minMode)}
		},
		"field then score": func() search.SortOrder {
			return search.SortOrder{sortField("s"), &search.SortScore{Desc: true}}
		},
		"score then field": func() search.SortOrder {
			return search.SortOrder{&search.SortScore{Desc: true}, sortField("n", asNumber)}
		},
		"score ascending": func() search.SortOrder { return search.SortOrder{&search.SortScore{}} },
		"id":              func() search.SortOrder { return search.SortOrder{&search.SortDocID{}} },
		"id desc":         func() search.SortOrder { return search.SortOrder{&search.SortDocID{Desc: true}} },
		"field then id": func() search.SortOrder {
			return search.SortOrder{sortField("n", asNumber), &search.SortDocID{}}
		},
		"own score sort": func() search.SortOrder {
			return search.SortOrder{&ownScoreSort{search.SortScore{Desc: true}}}
		},
		"own score sort then field": func() search.SortOrder {
			return search.SortOrder{sortField("s"), &ownScoreSort{search.SortScore{Desc: true}}, sortField("n", asNumber)}
		},
		"own sort reading the score": func() search.SortOrder {
			return search.SortOrder{&ownScoreReader{search.SortDocID{Desc: true}}, sortField("n", asNumber)}
		},
		"own id sort": func() search.SortOrder {
			return search.SortOrder{sortField("n", asNumber), &ownIDSort{search.SortDocID{Desc: true}}}
		},
		"distance": func() search.SortOrder {
			g, err := search.NewSortGeoDistance("loc", "km", -121.0, 37.8, false)
			if err != nil {
				t.Fatal(err)
			}
			return search.SortOrder{g}
		},
	}
	queries := map[string]func() query.Query{
		"term": func() query.Query { return termQueryOn("body", "common") },
		"or": func() query.Query {
			return query.NewDisjunctionQuery([]query.Query{
				termQueryOn("body", "even"), termQueryOn("body", "five"), termQueryOn("body", "mid")})
		},
		"and": func() query.Query {
			return query.NewConjunctionQuery([]query.Query{termQueryOn("body", "common"), termQueryOn("body", "even")})
		},
		"must not": func() query.Query {
			return query.NewBooleanQuery([]query.Query{termQueryOn("body", "common")}, nil,
				[]query.Query{termQueryOn("body", "five")})
		},
	}
	pages := []struct{ size, from int }{{10, 0}, {7, 3}, {50, 120}, {5000, 0}, {20, 3000}, {0, 0}}

	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		idx, cleanup := perSegmentSortTestIndex(t, model)
		defer cleanup()
		for sname, mkSort := range sorts {
			for qname, mkQuery := range queries {
				for _, pg := range pages {
					what := fmt.Sprintf("%s/%s/%s size=%d from=%d", model, sname, qname, pg.size, pg.from)
					mkReq := func() *SearchRequest {
						r := NewSearchRequestOptions(mkQuery(), pg.size, pg.from, false)
						r.SortByCustom(mkSort())
						return r
					}
					old, got := runBothPaths(t, idx, mkReq, what)
					compareSortedResults(t, what, old, got)
				}
			}
		}
	}
}

// The sort values of the hits are really read from the doc values, not all
// missing: a comparison of two paths that both see nothing proves nothing.
func TestPerSegmentSortedSearchSeesTheValues(t *testing.T) {
	idx, cleanup := perSegmentSortTestIndex(t, index.DefaultScoringModel)
	defer cleanup()
	for _, field := range []string{"n", "s", "d", "tags"} {
		r := NewSearchRequestOptions(termQueryOn("body", "common"), 20, 0, false)
		r.SortByCustom(search.SortOrder{sortField(field)})
		res, err := idx.Search(r)
		if err != nil {
			t.Fatal(err)
		}
		distinct := map[string]bool{}
		for _, h := range res.Hits {
			distinct[h.Sort[0]] = true
		}
		if len(res.Hits) == 0 || len(distinct) < 1 || distinct[search.HighTerm] || distinct[search.LowTerm] {
			t.Fatalf("sorting by %s: values %v, want real values", field, distinct)
		}
	}
}

// Explanations work with any sort: the hits are explained as with score order.
func TestPerSegmentSortedSearchExplains(t *testing.T) {
	idx, cleanup := perSegmentSortTestIndex(t, index.BM25Scoring)
	defer cleanup()
	mkReq := func() *SearchRequest {
		q := query.NewDisjunctionQuery([]query.Query{termQueryOn("body", "even"), termQueryOn("body", "five")})
		r := NewSearchRequestOptions(q, 15, 2, true)
		r.SortByCustom(search.SortOrder{sortField("s"), sortField("n", asNumber, desc)})
		return r
	}
	old, got := runBothPaths(t, idx, mkReq, "explain")
	compareSortedResults(t, "explain", old, got)
	for i, h := range got.Hits {
		if h.Expl == nil || h.Expl.Value != h.Score {
			t.Fatalf("hit %d: explanation %v, score %v", i, h.Expl, h.Score)
		}
	}
}

// ownScoreSort and ownIDSort are sorts of an application's own: by score, and by
// document id, as far as the collector can tell (what they say they require).
type ownScoreSort struct{ search.SortScore }
type ownIDSort struct{ search.SortDocID }

// ownScoreReader doesn't say it requires the score, but its value is made of the
// score of the DocumentMatch it is asked about (the whole part, to 3 digits).
type ownScoreReader struct{ search.SortDocID }

// (Copy has to be theirs too: the one of the sort they embed makes one of that.)
func (s *ownScoreSort) Copy() search.SearchSort { c := *s; return &c }
func (s *ownIDSort) Copy() search.SearchSort    { c := *s; return &c }
func (s *ownScoreReader) Copy() search.SearchSort {
	c := *s
	return &c
}
func (s *ownScoreReader) RequiresDocID() bool { return false }
func (s *ownScoreReader) Value(i *search.DocumentMatch) string {
	return fmt.Sprintf("%03d", int(i.Score))
}

// Requests it can't serve stay with the regular path, whatever the sort.
func TestPerSegmentSortedSearchIneligibleRequests(t *testing.T) {
	idx, cleanup := perSegmentSortTestIndex(t, index.DefaultScoringModel)
	defer cleanup()
	tq := func() query.Query { return termQueryOn("body", "common") }
	mk := map[string]func() *SearchRequest{
		"sorted, with locations": func() *SearchRequest {
			r := NewSearchRequest(tq())
			r.SortBy([]string{"n"})
			r.IncludeLocations = true
			return r
		},
	}
	for name, f := range mk {
		before := perSegmentSearches.Load()
		if _, err := idx.Search(f()); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if perSegmentSearches.Load() != before {
			t.Errorf("%s: should not have gone through the per segment path", name)
		}
	}
}
