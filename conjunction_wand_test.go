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
	"os"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

// buildConjunctionWANDIndex indexes n BM25-scored documents with three
// terms of deliberately different cardinality -- "alpha" (~1%), "beta"
// (~10%), "common" (~80%) -- plus a variable amount of unrelated filler so
// field length (and so BM25 norm) varies enough for block-max bounds to
// actually differ from block to block, exercising real pruning rather than
// windows that all look equally (un)competitive.
func buildConjunctionWANDIndex(t *testing.T, n int) Index {
	t.Helper()

	dir, err := os.MkdirTemp("", "conjwand")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	im := mapping.NewIndexMapping()
	im.DefaultAnalyzer = "standard"
	im.ScoringModel = index.BM25Scoring
	idx, err := New(dir+"/i.bleve", im)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	batch := idx.NewBatch()
	for i := 0; i < n; i++ {
		// 97, 13 and 5 are pairwise coprime, so these three memberships are
		// independent of each other -- unlike e.g. mod-100 vs. mod-5, which
		// would make "alpha" a subset of "common"'s complement and leave
		// every alpha-AND-common query with zero matches.
		var terms []string
		if i%97 == 0 { // ~1% of docs
			terms = append(terms, "alpha")
		}
		if i%13 == 0 { // ~7.7% of docs
			terms = append(terms, "beta")
		}
		if i%5 != 0 { // ~80% of docs
			terms = append(terms, "common")
		}
		fillerCount := (i*7)%23 + 1
		filler := make([]string, fillerCount)
		for j := range filler {
			filler[j] = "filler"
		}
		body := strings.Join(append(terms, filler...), " ")
		if err := batch.Index(fmt.Sprintf("d%05d", i), map[string]interface{}{
			"body": body,
		}); err != nil {
			t.Fatal(err)
		}
		if batch.Size() >= 500 {
			if err := idx.Batch(batch); err != nil {
				t.Fatal(err)
			}
			batch = idx.NewBatch()
		}
	}
	if batch.Size() > 0 {
		if err := idx.Batch(batch); err != nil {
			t.Fatal(err)
		}
	}
	return idx
}

func andQuery(terms ...string) query.Query {
	subs := make([]query.Query, len(terms))
	for i, term := range terms {
		q := query.NewTermQuery(term)
		q.SetField("body")
		subs[i] = q
	}
	return query.NewConjunctionQuery(subs)
}

// TestConjunctionBlockMaxWANDMatchesBaseline compares the block-max WAND
// conjunction path against the pre-existing leapfrog path (forced via
// searcher.EnableConjunctionBlockMaxWAND) across query shapes with
// different clause-size skew and several top-K sizes, some small enough
// relative to the corpus to force real pruning. Hits (ID, Score, order)
// must match exactly regardless of pruning; Total must never be
// overcounted, and any undercount must be reflected in TotalRelation.
func TestConjunctionBlockMaxWANDMatchesBaseline(t *testing.T) {
	const n = 4000
	idx := buildConjunctionWANDIndex(t, n)

	t.Cleanup(func() { searcher.EnableConjunctionBlockMaxWAND = true })

	shapes := []struct {
		name  string
		terms []string
	}{
		{"skewed-2term", []string{"alpha", "common"}},   // ~1% AND ~80%
		{"mid-2term", []string{"beta", "common"}},       // ~10% AND ~80%
		{"3term", []string{"alpha", "beta", "common"}},  // ~1% AND ~10% AND ~80%
		{"comparable-2term", []string{"beta", "alpha"}}, // ~10% AND ~1%, reversed order
	}

	for _, shape := range shapes {
		for _, size := range []int{1, 5, 50, 500} {
			t.Run(fmt.Sprintf("%s/size=%d", shape.name, size), func(t *testing.T) {
				q := andQuery(shape.terms...)

				searcher.EnableConjunctionBlockMaxWAND = true
				reqOn := NewSearchRequest(q)
				reqOn.Size = size
				resOn, err := idx.Search(reqOn)
				if err != nil {
					t.Fatalf("WAND-on search failed: %v", err)
				}

				searcher.EnableConjunctionBlockMaxWAND = false
				reqOff := NewSearchRequest(q)
				reqOff.Size = size
				resOff, err := idx.Search(reqOff)
				if err != nil {
					t.Fatalf("WAND-off search failed: %v", err)
				}
				searcher.EnableConjunctionBlockMaxWAND = true

				if len(resOn.Hits) != len(resOff.Hits) {
					onSet := map[string]bool{}
					for _, h := range resOn.Hits {
						onSet[h.ID] = true
					}
					for _, h := range resOff.Hits {
						if !onSet[h.ID] {
							t.Logf("missing from WAND-on: %s score=%v", h.ID, h.Score)
						}
					}
					t.Fatalf("hit count mismatch: WAND-on=%d WAND-off=%d",
						len(resOn.Hits), len(resOff.Hits))
				}
				for i := range resOn.Hits {
					on, off := resOn.Hits[i], resOff.Hits[i]
					if on.ID != off.ID {
						t.Errorf("hit %d: ID mismatch: WAND-on=%s WAND-off=%s", i, on.ID, off.ID)
					}
					if on.Score != off.Score {
						t.Errorf("hit %d (id=%s): score mismatch: WAND-on=%v WAND-off=%v",
							i, on.ID, on.Score, off.Score)
					}
				}

				if resOn.Total > resOff.Total {
					t.Errorf("WAND-on Total=%d exceeds WAND-off (exact) Total=%d — overcounted",
						resOn.Total, resOff.Total)
				}
				if resOff.TotalRelation != TotalRelationEq {
					t.Errorf("WAND-off baseline TotalRelation=%q, want %q",
						resOff.TotalRelation, TotalRelationEq)
				}
				if resOn.Total < resOff.Total && resOn.TotalRelation != TotalRelationGte {
					t.Errorf("WAND-on undercounted (Total=%d < baseline %d) but "+
						"TotalRelation=%q, want %q — silently claims an exact count it doesn't have",
						resOn.Total, resOff.Total, resOn.TotalRelation, TotalRelationGte)
				}
				// Note: WAND-on may report TotalRelationGte even when its count
				// happens to equal the exact baseline — pruning a window it
				// conservatively can't prove was empty is expected, not a bug.
			})
		}
	}
}

// TestConjunctionBlockMaxWANDHitsAreRealMatches guards the cheapest way the
// new path could be wrong: returning a document that doesn't actually
// satisfy every clause.
func TestConjunctionBlockMaxWANDHitsAreRealMatches(t *testing.T) {
	const n = 4000
	idx := buildConjunctionWANDIndex(t, n)

	req := NewSearchRequest(andQuery("alpha", "common"))
	req.Size = 100
	res, err := idx.Search(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hits) == 0 {
		t.Fatal("expected at least one hit")
	}
	for _, h := range res.Hits {
		var i int
		if _, err := fmt.Sscanf(h.ID, "d%05d", &i); err != nil {
			t.Fatalf("unexpected id %q: %v", h.ID, err)
		}
		if i%97 != 0 {
			t.Errorf("id %s does not match 'alpha'", h.ID)
		}
		if i%5 == 0 {
			t.Errorf("id %s does not match 'common'", h.ID)
		}
	}
}
