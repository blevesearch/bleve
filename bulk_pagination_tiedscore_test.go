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
	index "github.com/blevesearch/bleve_index_api"
)

// buildBulkTiedScoreIndex indexes n BM25-scored documents with deliberately
// coarse term-membership (~33%/~25%/~50%, via mod-3/mod-4/mod-2 -- pairwise
// coprime so memberships are independent) and only 6 distinct filler
// lengths, so groups of hundreds of documents share the exact same
// (term-membership, field-length) combination and therefore the exact same
// BM25 score -- the scenario a HitNumber-based tie-break actually has to
// arbitrate, repeatedly, across a real-sized corpus.
func buildBulkTiedScoreIndex(t *testing.T, n int) Index {
	t.Helper()

	dir, err := os.MkdirTemp("", "bulktied")
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
		var terms []string
		if i%3 == 0 { // ~33%
			terms = append(terms, "alpha")
		}
		if i%4 == 0 { // ~25%
			terms = append(terms, "beta")
		}
		if i%2 == 0 { // ~50%
			terms = append(terms, "common")
		}
		fillerCount := i%6 + 1 // only 6 distinct lengths -> heavy score collisions
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

// TestBulkCollectPaginationWithTiedScores checks that collectBulk's
// HitNumber-based tie-break stays self-consistent under pagination: since
// hc.total increments once per document ScoreBlock hands it (see
// TopNCollector.collectBulk), a document's HitNumber -- and so its relative
// order among score-ties -- must be the same whether it's produced by a
// small paginated request or reconstructed by slicing one exhaustive scan.
// This is the same shape of bug found on wand/block-max (a threshold-driven
// path selectively skipping documents shifted which of two tied-score docs
// "arrived" first), checked fresh here because collectBulk's own block-max
// WAND layer (search_conjunction_block.go's EnableConjunctionBlockMaxWAND)
// is a different mechanism that could reintroduce the same class of issue.
func TestBulkCollectPaginationWithTiedScores(t *testing.T) {
	const n = 6000
	idx := buildBulkTiedScoreIndex(t, n)

	bodyTerm := func(term string) query.Query {
		q := query.NewTermQuery(term)
		q.SetField("body")
		return q
	}

	cases := []struct {
		name string
		q    query.Query
	}{
		{"term", bodyTerm("common")},
		{"conjunction", query.NewConjunctionQuery([]query.Query{bodyTerm("alpha"), bodyTerm("common")})},
		{"disjunction", query.NewDisjunctionQuery([]query.Query{bodyTerm("alpha"), bodyTerm("beta")})},
	}

	pages := []struct {
		name       string
		from, size int
	}{
		{"top5", 0, 5},
		{"top50", 0, 50},
		{"top500", 0, 500},
		{"mid-30-40", 30, 40},
		{"mid-300-100", 300, 100},
		{"deep-999-137", 999, 137},
		{"deep-1999-250", 1999, 250},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			exhaustive := NewSearchRequest(c.q)
			exhaustive.Size = n
			exRes, err := idx.Search(exhaustive)
			if err != nil {
				t.Fatal(err)
			}
			if len(exRes.Hits) < 500 {
				t.Fatalf("corpus/query not dense enough: only %d hits", len(exRes.Hits))
			}

			for _, p := range pages {
				t.Run(p.name, func(t *testing.T) {
					req := NewSearchRequestOptions(c.q, p.size, p.from, false)
					res, err := idx.Search(req)
					if err != nil {
						t.Fatal(err)
					}
					lo, hi := p.from, p.from+p.size
					if hi > len(exRes.Hits) {
						hi = len(exRes.Hits)
					}
					if lo > hi {
						lo = hi
					}
					want := exRes.Hits[lo:hi]
					if len(res.Hits) != len(want) {
						t.Fatalf("From=%d,Size=%d returned %d hits, want %d",
							p.from, p.size, len(res.Hits), len(want))
					}
					for i := range want {
						if res.Hits[i].ID != want[i].ID || res.Hits[i].Score != want[i].Score {
							t.Fatalf("hit %d: From=%d,Size=%d got (id=%s,score=%v), want (id=%s,score=%v) from exhaustive baseline",
								i, p.from, p.size, res.Hits[i].ID, res.Hits[i].Score, want[i].ID, want[i].Score)
						}
					}
				})
			}
		})
	}
}
