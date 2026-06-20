// Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bleve

// Tests for the TotalRelation field of SearchResult (§9/§15 / WAND pruning).
//
// TotalRelation="eq" means the Total count is exact (all matching docs were
// scored and the top-K collector saw every hit).
//
// TotalRelation="gte" means WAND/MAXSCORE pruning fired and skipped some
// lower-scoring candidates — the Total count is a lower bound.  This is only
// possible when SearchRequest.ScoreMode == ScoreModeTopScores (which enables
// SetWANDEnabled on the collector) and the index has enough candidates that the
// threshold rises above some of their MaxImpact bounds.
//
// The two tests below use the package-level New() / Index.Index() / Index.Search()
// API so they cover the full stack including collector + index_impl integration.

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

// buildTotalRelationIndex creates a temporary bleve index with n documents
// indexed in a single batch (one segment).
//
// The first kHighFreq docs have both "alpha" and "beta" repeated many times
// (high BM25 score for both terms).  The remaining docs have only "alpha"
// once (low score; "beta" is absent).
//
// WAND pruning triggers for a "alpha OR beta" disjunction when the threshold
// (from the top-K high-scoring docs that matched BOTH terms) exceeds
// MaxImpact("alpha") — i.e. when the contribution of "beta" alone lifts the
// threshold above what any "alpha-only" doc can achieve.
func buildTotalRelationIndex(t *testing.T, n, kHighFreq int) (Index, string) {
	t.Helper()

	dir, err := os.MkdirTemp("", "totalrelation-*")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	m := mapping.NewIndexMapping()
	m.ScoringModel = index.BM25Scoring

	idx, err := New(dir, m)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	b := idx.NewBatch()

	for i := 0; i < n; i++ {
		var body string
		if i < kHighFreq {
			// High-scoring: "alpha" and "beta" each 20 times. Both terms match.
			body = strings.Repeat("alpha beta ", 20) + fmt.Sprintf("hf%d", i)
		} else {
			// Low-scoring: only "alpha" once. "beta" is absent → WAND prunable.
			body = fmt.Sprintf("alpha lf%d u%d", i, i)
		}
		doc := map[string]interface{}{
			"id":   fmt.Sprintf("doc%d", i),
			"body": body,
		}
		if err := b.Index(doc["id"].(string), doc); err != nil {
			t.Fatalf("Batch.Index doc%d: %v", i, err)
		}
	}
	if err := idx.Batch(b); err != nil {
		t.Fatalf("Batch: %v", err)
	}
	return idx, dir
}

// TestTotalRelationEq verifies that a normal search (no ScoreMode override)
// returns TotalRelation="eq": the collector saw every matching document.
func TestTotalRelationEq(t *testing.T) {
	idx, _ := buildTotalRelationIndex(t, 20, 3)
	defer idx.Close()

	// Single-term query: no disjunction → no per-candidate WAND pruning.
	q := query.NewMatchQuery("alpha")
	q.SetField("body")
	req := NewSearchRequest(q)
	req.Size = 5 // small result window, but no ScoreMode → no WAND

	result, err := idx.Search(req)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if result.TotalRelation != TotalRelationEq {
		t.Errorf("TotalRelation=%q want %q (without ScoreModeTopScores, WAND should not prune)",
			result.TotalRelation, TotalRelationEq)
	}
	if result.Total == 0 {
		t.Error("Total=0: no matching documents found (corpus setup error?)")
	}
}

// TestTotalRelationGteWithWAND verifies that a disjunction (OR) query with
// ScoreMode="top_scores" triggers WAND/MAXSCORE per-candidate pruning and
// sets TotalRelation="gte".
//
// WAND triggers when:
//   threshold > MaxImpact("alpha")  (from high-scoring docs that matched BOTH "alpha" + "beta")
//
// Low-scoring docs that matched ONLY "alpha" (beta absent) have an upper bound
// of MaxImpact("alpha"), which is below the threshold → they are pruned.
//
// Corpus (50 docs, first 5 high-freq):
//   doc0..4 : "alpha beta" × 20 + filler  (high score; both terms match)
//   doc5..49: "alpha lf…"                 (low score; only alpha matches)
func TestTotalRelationGteWithWAND(t *testing.T) {
	const nDocs = 50
	const kHighFreq = 5

	idx, _ := buildTotalRelationIndex(t, nDocs, kHighFreq)
	defer idx.Close()

	// Disjunction query: "alpha OR beta" — required for per-candidate WAND pruning.
	// Use explicit TermQuery clauses so the query goes directly to a
	// DisjunctionSliceSearcher with two distinct TermSearchers.
	alphaQ := query.NewTermQuery("alpha")
	alphaQ.SetField("body")
	betaQ := query.NewTermQuery("beta")
	betaQ.SetField("body")
	bq := query.NewBooleanQuery(nil, []query.Query{alphaQ, betaQ}, nil)
	req := NewSearchRequest(bq)
	req.Size = 3
	req.ScoreMode = ScoreModeTopScores // enables WAND via SetWANDEnabled(true)

	result, err := idx.Search(req)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if result.TotalRelation != TotalRelationGte {
		t.Errorf("TotalRelation=%q want %q; Total=%d hits=%d — WAND pruning did not fire",
			result.TotalRelation, TotalRelationGte, result.Total, len(result.Hits))
	} else {
		t.Logf("WAND pruning confirmed: TotalRelation=%q, Total=%d (lower bound)",
			result.TotalRelation, result.Total)
	}

	if len(result.Hits) == 0 {
		t.Error("no hits returned")
	}
}
