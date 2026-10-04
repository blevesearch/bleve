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
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

// perSegmentTestIndex builds an index of a few hundred documents spread over
// many segments, with updated and deleted documents so that segments carry
// deletions. Merging is made practically impossible, to keep the segments.
func perSegmentTestIndex(t *testing.T, scoringModel string, extraConfig map[string]interface{}) (Index, func()) {
	t.Helper()
	dir := createTmpIndexPath(t)

	im := NewIndexMapping()
	im.ScoringModel = scoringModel

	config := map[string]interface{}{
		"scorchMergePlanOptions": map[string]interface{}{
			// the planner only merges segments smaller than half of this
			"MaxSegmentSize": 2,
		},
	}
	for k, v := range extraConfig {
		config[k] = v
	}
	idx, err := NewUsing(dir, im, scorch.Name, scorch.Name, config)
	if err != nil {
		t.Fatal(err)
	}

	// waiting for a batch to be persisted before moving on to the next one
	// keeps the persister from merging the in memory segments of the batches
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

	rnd := rand.New(rand.NewSource(42))
	words := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta"}
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
		if i == 777 {
			toks = append(toks, "rare")
		}
		for k := rnd.Intn(7); k > 0; k-- {
			toks = append(toks, "dup")
		}
		// different field lengths -> different norms
		for k := rnd.Intn(40); k > 0; k-- {
			toks = append(toks, words[rnd.Intn(len(words))])
		}
		rnd.Shuffle(len(toks), func(a, b int) { toks[a], toks[b] = toks[b], toks[a] })
		return map[string]interface{}{"body": strings.Join(toks, " "), "flag": i%3 == 0}
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
	// updates delete the old copy of a doc from its segment
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

func segmentCountAndDeletions(t *testing.T, idx Index) (segments int, anyDeleted bool) {
	t.Helper()
	adv, err := idx.Advanced()
	if err != nil {
		t.Fatal(err)
	}
	r, err := adv.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	snap := r.(*scorch.IndexSnapshot)
	for _, s := range snap.Segments() {
		if d := s.Deleted(); d != nil && !d.IsEmpty() {
			anyDeleted = true
		}
	}
	return len(snap.Segments()), anyDeleted
}

type perSegmentSearchCase struct {
	field string
	term  string
	boost float64
	size  int
	from  int
}

func (c perSegmentSearchCase) request() *SearchRequest {
	tq := query.NewTermQuery(c.term)
	tq.SetField(c.field)
	if c.boost != 0 {
		tq.SetBoost(c.boost)
	}
	return NewSearchRequestOptions(tq, c.size, c.from, false)
}

// the per segment path scores in float32, so a score is within float32
// precision of the one the regular path computes in float64
func sameScore(a, b float64) bool {
	if a == b {
		return true
	}
	return math.Abs(a-b) <= 1e-5*math.Max(math.Abs(a), math.Abs(b))
}

// runBothPaths runs the request with the per segment path off and on, and
// returns both results. The request must be served by the per segment path.
func runBothPaths(t *testing.T, idx Index, mkReq func() *SearchRequest, what string) (old, perSeg *SearchResult) {
	t.Helper()

	perSegmentSearchEnabled.Store(false)
	before := perSegmentSearches.Load()
	old, err := idx.Search(mkReq())
	perSegmentSearchEnabled.Store(true)
	if err != nil {
		t.Fatalf("%s: regular path: %v", what, err)
	}
	if perSegmentSearches.Load() != before {
		t.Fatalf("%s: per segment path ran while disabled", what)
	}

	perSeg, err = idx.Search(mkReq())
	if err != nil {
		t.Fatalf("%s: per segment path: %v", what, err)
	}
	if perSegmentSearches.Load() != before+1 {
		t.Fatalf("%s: the per segment path was not taken", what)
	}
	return old, perSeg
}

// compareSearchResults checks that the per segment path's result is the regular
// path's, as far as float32 scoring lets it be: the same total and max score,
// scores that agree, and the same hits in the same places, except where scores
// are so close that float32 rounding may order them differently.
func compareSearchResults(t *testing.T, what string, old, got *SearchResult) {
	t.Helper()
	compareSearchResultsOpt(t, what, old, got, false)
}

// compareSearchResultsIgnoringIndex is for results of several indexes with the
// same content, where a tie may be broken either way between the indexes.
func compareSearchResultsIgnoringIndex(t *testing.T, what string, old, got *SearchResult) {
	t.Helper()
	compareSearchResultsOpt(t, what, old, got, true)
}

func compareSearchResultsOpt(t *testing.T, what string, old, got *SearchResult, ignoreIndex bool) {
	t.Helper()
	// A search that skipped blocks that couldn't make the top hits says so, and
	// then its total is a lower bound of the real one.
	if got.TotalRelation == TotalRelationGte && old.TotalRelation == TotalRelationEq {
		if got.Total > old.Total || got.Total < uint64(len(got.Hits)) {
			t.Fatalf("%s: total %d (a lower bound), the real total is %d with %d hits",
				what, got.Total, old.Total, len(got.Hits))
		}
	} else {
		if old.Total != got.Total {
			t.Fatalf("%s: total %d, want %d", what, got.Total, old.Total)
		}
		if old.TotalRelation != got.TotalRelation {
			t.Fatalf("%s: total relation %v, want %v", what, got.TotalRelation, old.TotalRelation)
		}
	}
	if !sameScore(old.MaxScore, got.MaxScore) {
		t.Fatalf("%s: max score %v, want %v", what, got.MaxScore, old.MaxScore)
	}
	if len(old.Hits) != len(got.Hits) {
		t.Fatalf("%s: %d hits, want %d", what, len(got.Hits), len(old.Hits))
	}
	seen := map[string]bool{}
	for i := range old.Hits {
		o, g := old.Hits[i], got.Hits[i]
		if !sameScore(o.Score, g.Score) {
			t.Fatalf("%s: hit %d scores %v, want %v", what, i, g.Score, o.Score)
		}
		if i > 0 && got.Hits[i-1].Score < g.Score {
			t.Fatalf("%s: hit %d scores %v, above the hit before it, %v", what, i, g.Score, got.Hits[i-1].Score)
		}
		if key := g.Index + "/" + g.ID; seen[key] {
			t.Fatalf("%s: hit %s twice", what, key)
		} else {
			seen[key] = true
		}

		// the same doc, unless its score is close to a neighbor's
		nearNeighbor := (i > 0 && sameScore(o.Score, old.Hits[i-1].Score)) ||
			(i+1 < len(old.Hits) && sameScore(o.Score, old.Hits[i+1].Score)) ||
			i+1 == len(old.Hits) // what follows the last hit isn't known
		if o.ID != g.ID && !nearNeighbor {
			t.Fatalf("%s: hit %d is %s, want %s", what, i, g.ID, o.ID)
		}
		if !ignoreIndex && o.Index != g.Index {
			t.Fatalf("%s: hit %d index %q, want %q", what, i, g.Index, o.Index)
		}
		if strings.Join(o.Sort, ",") != strings.Join(g.Sort, ",") {
			t.Fatalf("%s: hit %d sort %v, want %v", what, i, g.Sort, o.Sort)
		}
		if len(o.Locations) != len(g.Locations) || len(o.Fields) != len(g.Fields) {
			t.Fatalf("%s: hit %d locations/fields differ", what, i)
		}
	}
}

func TestPerSegmentSearchMatchesRegularSearch(t *testing.T) {
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		t.Run(model, func(t *testing.T) {
			idx, cleanup := perSegmentTestIndex(t, model, nil)
			defer cleanup()

			segments, anyDeleted := segmentCountAndDeletions(t, idx)
			if segments < 5 || !anyDeleted {
				t.Fatalf("test index needs many segments with deletions, has %d segments (deletions: %v)",
					segments, anyDeleted)
			}

			terms := []string{"common", "even", "five", "mid", "dup", "rare", "alpha", "theta", "missing"}
			sizes := []struct{ size, from int }{
				{10, 0}, {10, 7}, {1, 0}, {0, 0}, {3, 1}, {50, 100}, {5000, 0}, {10, 5000}, {11, 0}, {200, 0},
			}
			for _, term := range terms {
				for _, sz := range sizes {
					for _, boost := range []float64{0, 2.5} {
						c := perSegmentSearchCase{field: "body", term: term, boost: boost, size: sz.size, from: sz.from}
						what := fmt.Sprintf("%s %+v", model, c)
						old, got := runBothPaths(t, idx, c.request, what)
						compareSearchResults(t, what, old, got)
					}
				}
			}

			// the default search field, _all
			c := perSegmentSearchCase{term: "common", size: 25}
			old, got := runBothPaths(t, idx, c.request, model+" _all")
			compareSearchResults(t, model+" _all", old, got)
		})
	}
}

func TestPerSegmentSearchHitNumbers(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()

	c := perSegmentSearchCase{field: "body", term: "common", size: 40, from: 3}
	// HitNumber isn't in a SearchResult, but it's what breaks ties, so it has
	// to agree for the ordering to: every score tie must resolve by doc order.
	got, err := idx.Search(c.request())
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i < len(got.Hits); i++ {
		a, b := got.Hits[i-1], got.Hits[i]
		if a.Score == b.Score && a.HitNumber >= b.HitNumber {
			t.Fatalf("hits %d,%d tie on score but hit numbers are %d,%d", i-1, i, a.HitNumber, b.HitNumber)
		}
	}
}

// requests that the per segment path doesn't serve must keep going to the
// regular path, and answer as before.
func TestPerSegmentSearchIneligibleRequests(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()

	tq := func() query.Query {
		q := query.NewTermQuery("common")
		q.SetField("body")
		return q
	}
	mk := map[string]func() *SearchRequest{
		"locations": func() *SearchRequest {
			r := NewSearchRequest(tq())
			r.IncludeLocations = true
			return r
		},
		"highlight": func() *SearchRequest {
			r := NewSearchRequest(tq())
			r.Highlight = NewHighlight()
			return r
		},
		"a prefix query": func() *SearchRequest {
			return NewSearchRequest(query.NewPrefixQuery("comm"))
		},
		"a fuzzy match": func() *SearchRequest {
			q := query.NewMatchQuery("common")
			q.SetFuzziness(1)
			return NewSearchRequest(q)
		},
		"a phrase": func() *SearchRequest {
			return NewSearchRequest(query.NewMatchPhraseQuery("common even"))
		},
		"boolean with a filter": func() *SearchRequest {
			b := query.NewBooleanQuery([]query.Query{tq()}, nil, nil)
			b.Filter = termQueryOn("body", "even")
			return NewSearchRequest(b)
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

func TestPerSegmentSearchFallsBackOnOlderSegments(t *testing.T) {
	// zap v17 segments can't hand out block iterators
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, map[string]interface{}{
		"forceSegmentType":    "zap",
		"forceSegmentVersion": 17,
	})
	defer cleanup()

	c := perSegmentSearchCase{field: "body", term: "common", size: 10}
	before := perSegmentSearches.Load()
	res, err := idx.Search(c.request())
	if err != nil {
		t.Fatal(err)
	}
	if res.Total == 0 {
		t.Fatal("expected hits")
	}
	if perSegmentSearches.Load() != before {
		t.Fatal("per segment path taken on segments that don't support it")
	}
}

func TestPerSegmentSearchTermSearcherStats(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()

	for _, term := range []string{"common", "rare", "missing"} {
		for i := 0; i < 3; i++ {
			c := perSegmentSearchCase{field: "body", term: term, size: 5}
			if _, err := idx.Search(c.request()); err != nil {
				t.Fatal(err)
			}
		}
	}
	// every term searcher that was started got closed, per segment readers too
	stats := idx.StatsMap()["index"].(map[string]interface{})
	started, finished := stats["term_searchers_started"], stats["term_searchers_finished"]
	if started != finished {
		t.Fatalf("term searchers started %v, finished %v", started, finished)
	}
}

var _ mapping.IndexMapping = (*mapping.IndexMappingImpl)(nil)

// the TopN collector is only built for the searches that need it
func TestPerSegmentSearchDoesNotBuildTopNCollector(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()

	run := func(req *SearchRequest) (perSeg, topN uint64) {
		t.Helper()
		ps, tn := perSegmentSearches.Load(), topNCollectorsBuilt.Load()
		if _, err := idx.Search(req); err != nil {
			t.Fatal(err)
		}
		return perSegmentSearches.Load() - ps, topNCollectorsBuilt.Load() - tn
	}

	c := perSegmentSearchCase{field: "body", term: "common", size: 10}
	if ps, tn := run(c.request()); ps != 1 || tn != 0 {
		t.Errorf("per segment search: %d per segment collectors, %d TopN collectors built", ps, tn)
	}
	noScore := c.request()
	noScore.Score = ScoreNone
	if ps, tn := run(noScore); ps != 1 || tn != 0 {
		t.Errorf("unscored per segment search: %d per segment collectors, %d TopN collectors built", ps, tn)
	}

	// and it is for a search that isn't served that way
	withLocations := c.request()
	withLocations.IncludeLocations = true
	if ps, tn := run(withLocations); ps != 0 || tn != 1 {
		t.Errorf("search with locations: %d per segment collectors, %d TopN collectors built", ps, tn)
	}
	perSegmentSearchEnabled.Store(false)
	ps, tn := run(c.request())
	perSegmentSearchEnabled.Store(true)
	if ps != 0 || tn != 1 {
		t.Errorf("search with the path off: %d per segment collectors, %d TopN collectors built", ps, tn)
	}
}
