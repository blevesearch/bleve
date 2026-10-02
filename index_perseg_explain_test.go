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
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

// compareExplanations checks that an explanation has the structure of the one
// the regular path builds for the same doc: the same messages, in the same tree.
// Values are compared within float32 precision: the per segment path scores in
// float32.
// explanationMessage is the message of an explanation without the term in the
// saturation of bm25, which the regular path leaves empty (its reader doesn't tell
// the term of a posting) and the per segment path fills in.
func explanationMessage(m string) string {
	if strings.HasPrefix(m, "saturation(term:") {
		if i := strings.Index(m, "), k1="); i > 0 {
			return "saturation(term:" + m[i:]
		}
	}
	return m
}

func compareExplanations(t *testing.T, what, path string, got, want *search.Explanation) {
	t.Helper()
	compareExplanationsOrdered(t, what, path, got, want, true)
}

// compareExplanationsOrdered is compareExplanations, with the children of a node
// matched by their messages instead of by their places if ordered is false. A
// disjunction of more than DisjunctionHeapTakeover clauses is a heap in the
// regular path, which lists the clauses that match in an order of its own.
func compareExplanationsOrdered(t *testing.T, what, path string, got, want *search.Explanation, ordered bool) {
	t.Helper()
	if got == nil || want == nil {
		t.Fatalf("%s: %s: explanation %v, want %v", what, path, got, want)
	}
	if explanationMessage(got.Message) != explanationMessage(want.Message) {
		t.Fatalf("%s: %s: message %q, want %q", what, path, got.Message, want.Message)
	}
	if got.PartialMatch != want.PartialMatch {
		t.Fatalf("%s: %s: partial match %v, want %v", what, path, got.PartialMatch, want.PartialMatch)
	}
	if diff := math.Abs(got.Value - want.Value); diff > 1e-5*math.Max(1, math.Abs(want.Value)) {
		t.Fatalf("%s: %s: %q is %v, want %v", what, path, got.Message, got.Value, want.Value)
	}
	if len(got.Children) != len(want.Children) {
		t.Fatalf("%s: %s: %d children, want %d\n got: %v\nwant: %v", what, path,
			len(got.Children), len(want.Children), got, want)
	}
	if ordered {
		for i := range got.Children {
			compareExplanationsOrdered(t, what, fmt.Sprintf("%s/%d", path, i), got.Children[i], want.Children[i], true)
		}
		return
	}
	used := make([]bool, len(want.Children))
	for i, g := range got.Children {
		found := false
		for j, w := range want.Children {
			// the ids in the messages are the same for both, so the messages tell the clauses
			if !used[j] && explanationMessage(g.Message) == explanationMessage(w.Message) {
				used[j], found = true, true
				compareExplanationsOrdered(t, what, fmt.Sprintf("%s/%d", path, i), g, w, false)
				break
			}
		}
		if !found {
			t.Fatalf("%s: %s: child %q has no counterpart", what, path, g.Message)
		}
	}
}

// The hits of a search for the per segment path come with explanations, built
// once the hits are known; the score at the root is the score the hit was ranked
// by, to the bit, and the tree is that of the regular path.
func TestPerSegmentExplain(t *testing.T) {
	terms := func(names ...string) []query.Query {
		rv := make([]query.Query, len(names))
		for i, n := range names {
			rv[i] = termQueryOn("body", n)
		}
		return rv
	}
	boosted := termQueryOn("body", "w1")
	boosted.SetBoost(2.5)
	match := func(text string, op query.MatchQueryOperator) query.Query {
		m := query.NewMatchQuery(text)
		m.SetField("body")
		m.SetOperator(op)
		return m
	}
	boolQ := query.NewBooleanQuery(terms("w0"), terms("hot"), terms("w2"))
	minShould := query.NewBooleanQuery(terms("w2"), terms("w0", "w1", "hot"), nil)
	minShould.SetMinShould(2)
	minTwo := query.NewDisjunctionQuery(terms("w0", "w1", "w2", "hot"))
	minTwo.SetMin(2)
	boostedOr := query.NewDisjunctionQuery(terms("w0", "w1", "w2"))
	boostedOr.SetBoost(2.5)
	boostedAnd := query.NewConjunctionQuery(terms("w0", "w1"))
	boostedAnd.SetBoost(3)
	matchNothing := func() query.Query {
		m := query.NewMatchQuery("")
		m.SetField("body")
		return m
	}
	// the queries for which the regular path lists the clauses in an order of its own
	unordered := map[string]bool{"or of 12": true}
	// the queries that match nothing, on both paths
	matchesNothing := map[string]bool{"and with a match of nothing": true}
	queries := []struct {
		name string
		q    func() query.Query
	}{
		{"term", func() query.Query { return termQueryOn("body", "hot") }},
		{"boosted term", func() query.Query { return boosted }},
		{"and", func() query.Query { return query.NewConjunctionQuery(terms("w0", "w1")) }},
		{"and of 3", func() query.Query { return query.NewConjunctionQuery(terms("w0", "w1", "common")) }},
		{"or", func() query.Query { return query.NewDisjunctionQuery(terms("w0", "w1", "w2", "hot")) }},
		{"or of 7", func() query.Query {
			return query.NewDisjunctionQuery(terms("w0", "w1", "w2", "w3", "w4", "w5", "hot"))
		}},
		{"boolean: musts only", func() query.Query { return query.NewBooleanQuery(terms("w0", "w1"), nil, nil) }},
		{"boolean: shoulds only", func() query.Query { return query.NewBooleanQuery(nil, terms("w0", "w1", "hot"), nil) }},
		{"boolean: shoulds, must not", func() query.Query { return query.NewBooleanQuery(nil, terms("w0", "w1"), terms("w2")) }},
		{"boolean: required shoulds", func() query.Query { return minShould }},
		{"boolean: composite clauses", func() query.Query {
			return query.NewBooleanQuery([]query.Query{query.NewDisjunctionQuery(terms("w0", "w1"))},
				[]query.Query{query.NewConjunctionQuery(terms("w2", "w3"))},
				[]query.Query{query.NewDisjunctionQuery(terms("w4", "w5"))})
		}},
		{"boolean in and", func() query.Query {
			return query.NewConjunctionQuery([]query.Query{query.NewBooleanQuery(terms("w0"), terms("w1"), nil), termQueryOn("body", "w2")})
		}},
		{"or with a minimum of two", func() query.Query { return minTwo }},
		{"boosted or", func() query.Query { return boostedOr }},
		{"boosted and", func() query.Query { return boostedAnd }},
		{"or with a match of nothing", func() query.Query {
			return query.NewDisjunctionQuery([]query.Query{matchNothing(), termQueryOn("body", "w0"), termQueryOn("body", "w1")})
		}},
		{"and with a match of nothing", func() query.Query {
			return query.NewConjunctionQuery([]query.Query{matchNothing(), termQueryOn("body", "w0")})
		}},
		{"or of 12", func() query.Query {
			return query.NewDisjunctionQuery(terms("w0", "w1", "w2", "w3", "w4", "w5", "w6", "w8", "w13", "w21", "w34", "hot"))
		}},
		{"or with a rare term", func() query.Query { return query.NewDisjunctionQuery(terms("w0", "w1", "w400")) }},
		{"and in or", func() query.Query {
			return query.NewDisjunctionQuery([]query.Query{query.NewConjunctionQuery(terms("w0", "w1")), termQueryOn("body", "hot")})
		}},
		{"or in and", func() query.Query {
			return query.NewConjunctionQuery([]query.Query{query.NewDisjunctionQuery(terms("w0", "w1")), termQueryOn("body", "common")})
		}},
		{"boolean", func() query.Query { return boolQ }},
		{"match or", func() query.Query { return match("w0 w1 w2", query.MatchQueryOperatorOr) }},
		{"match and", func() query.Query { return match("w0 w1", query.MatchQueryOperatorAnd) }},
		{"query string", func() query.Query { return query.NewQueryStringQuery("+body:w0 body:hot -body:w2") }},
	}
	// the ways of finding the hits, which score through different code
	configs := []struct {
		name  string
		algo  string
		bitmp int
	}{
		{"default", "", 16},
		{"wand", "wand", 1 << 30},
		{"maxscore, bitmaps", "maxscore", 1},
	}

	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		for _, deletions := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deletions=%v", model, deletions), func(t *testing.T) {
				idx, cleanup := skipTestIndex(t, model, deletions)
				defer cleanup()

				for _, qc := range queries {
					// the regular path's explanation of every match is the reference
					perSegmentSearchEnabled.Store(false)
					ref, err := idx.Search(NewSearchRequestOptions(qc.q(), 12000, 0, true))
					perSegmentSearchEnabled.Store(true)
					if err != nil {
						t.Fatalf("%s: %v", qc.name, err)
					}
					refExpl := make(map[string]*search.Explanation, len(ref.Hits))
					for _, h := range ref.Hits {
						refExpl[h.ID] = h.Expl
					}

					for _, cfg := range configs {
						restoreA := searcher.SetPerSegmentDisjunctionAlgo(cfg.algo)
						restoreC := searcher.SetPerSegmentConjunctionBitmapMinCandidates(cfg.bitmp)
						for _, size := range []int{1, 10, 30} {
							what := fmt.Sprintf("%s/%s/size=%d", qc.name, cfg.name, size)
							before := perSegmentSearches.Load()
							res, err := idx.Search(NewSearchRequestOptions(qc.q(), size, 0, true))
							if err != nil {
								t.Fatalf("%s: %v", what, err)
							}
							if perSegmentSearches.Load() != before+1 {
								t.Fatalf("%s: not served by the per segment path", what)
							}
							if matchesNothing[qc.name] {
								if len(res.Hits) != 0 || len(refExpl) != 0 {
									t.Fatalf("%s: %d hits (the regular path: %d), want none", what, len(res.Hits), len(refExpl))
								}
								continue
							}
							if len(res.Hits) == 0 {
								t.Fatalf("%s: no hits", what)
							}
							for i, h := range res.Hits {
								if h.Expl == nil {
									t.Fatalf("%s: hit %d (%s) has no explanation", what, i, h.ID)
								}
								if h.Expl.Value != h.Score {
									t.Fatalf("%s: hit %d (%s) scored %v, explained as %v", what, i, h.ID,
										h.Score, h.Expl.Value)
								}
								want, ok := refExpl[h.ID]
								if !ok {
									t.Fatalf("%s: hit %s is not a match for the regular path", what, h.ID)
								}
								compareExplanationsOrdered(t, what, h.ID, h.Expl, want, !unordered[qc.name])
							}
						}
						restoreC()
						restoreA()
					}
				}
			})
		}
	}
}

// Explaining is for the hits returned: a search that doesn't ask for it builds no
// explanation, and one that does has them for the hits of the page and no others.
func TestPerSegmentExplainIsForTheHitsReturned(t *testing.T) {
	idx, cleanup := skipTestIndex(t, index.BM25Scoring, false)
	defer cleanup()
	q := func() query.Query { return termQueryOn("body", "hot") }

	res, err := idx.Search(NewSearchRequestOptions(q(), 10, 0, false))
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range res.Hits {
		if h.Expl != nil {
			t.Fatalf("hit %s has an explanation that wasn't asked for", h.ID)
		}
	}

	res, err = idx.Search(NewSearchRequestOptions(q(), 5, 3, true))
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hits) != 5 {
		t.Fatalf("%d hits, want 5", len(res.Hits))
	}
	for _, h := range res.Hits {
		if h.Expl == nil || !strings.Contains(h.Expl.Message, "hot") {
			t.Fatalf("hit %s: explanation %v", h.ID, h.Expl)
		}
	}
}
