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
	"os"
	"testing"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

// buildWANDEquivIndex builds a deterministic BM25 index whose documents have a
// wide spread of field lengths and per-term frequencies, so that each term ends
// up with a distinct MaxImpact and the MAXSCORE essential/non-essential split is
// actually exercised.  nBatches > 1 produces multiple segments.
func buildWANDEquivIndex(t *testing.T, nBatches, perBatch int) (Index, []string) {
	t.Helper()
	return buildWANDEquivIndexWithConfig(t, nBatches, perBatch, nil)
}

// buildWANDEquivIndexWithConfig is buildWANDEquivIndex with control over the
// segment codec, so the same corpus and the same contract can be checked against
// an older on-disk format.  cfg == nil means the default (current) codec.
func buildWANDEquivIndexWithConfig(t *testing.T, nBatches, perBatch int,
	cfg map[string]interface{}) (Index, []string) {
	t.Helper()

	dir, err := os.MkdirTemp("", "wand_equiv")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	im := mapping.NewIndexMapping()
	im.DefaultAnalyzer = "standard"
	im.ScoringModel = index.BM25Scoring
	var idx Index
	if cfg == nil {
		idx, err = New(dir+"/i.bleve", im)
	} else {
		idx, err = NewUsing(dir+"/i.bleve", im, scorch.Name, scorch.Name, cfg)
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	terms := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for b := 0; b < nBatches; b++ {
		batch := idx.NewBatch()
		for n := 0; n < perBatch; n++ {
			body := ""
			for ti, term := range terms {
				// Different terms appear in different fractions of the corpus,
				// with varying repeat counts -> distinct idf and MaxImpact.
				if (n+ti)%(ti+2) == 0 {
					for rep := 0; rep <= (n+ti)%3; rep++ {
						body += term + " "
					}
				}
			}
			for f := 0; f < n%17; f++ { // vary field length
				body += fmt.Sprintf("filler%d ", f)
			}
			if body == "" {
				body = terms[0]
			}
			if err := batch.Index(fmt.Sprintf("d%02d_%04d", b, n),
				map[string]interface{}{"body": body}); err != nil {
				t.Fatal(err)
			}
		}
		if err := idx.Batch(batch); err != nil {
			t.Fatal(err)
		}
	}
	return idx, terms
}

// TestWANDTopScoresMatchesComplete is the correctness contract for
// ScoreMode=top_scores: WAND / MAXSCORE pruning is an optimization, so the top-k
// it returns must score the same, rank for rank, as ScoreModeComplete.  Only
// two things may differ: Total, which becomes a lower bound once pruning fires
// (reported via TotalRelationGte), and the relative order of documents whose
// scores tie, since that follows collection order and pruning changes which
// documents are collected.
//
// This guards every bound in the pruning chain at once: zapx's per-term
// maxTFNorm (§1/§14), TermSearcher.MaxImpact (§1), the per-segment ceilings
// (§15), and the MAXSCORE essential/non-essential partition (§8).  Any of them
// under-estimating a document's achievable score silently drops that document
// from the result set, which no other test on this branch detects —
// TestTotalRelationGteWithWAND only asserts that pruning *fires*.
//
// Larger Size values matter here: the threshold is lower, so the MAXSCORE
// partition moves more terms into the non-essential set and a wrong partition
// has more opportunity to skip a document that belongs in the results.
func TestWANDTopScoresMatchesComplete(t *testing.T) {
	idx, terms := buildWANDEquivIndex(t, 5, 400)
	assertTopScoresMatchesComplete(t, idx, terms, 1, 5, 10, 50, 200)
}

// assertTopScoresMatchesComplete runs a disjunction over terms in both score
// modes at each size and asserts the contract documented on
// TestWANDTopScoresMatchesComplete.
func assertTopScoresMatchesComplete(t *testing.T, idx Index, terms []string, sizes ...int) {
	t.Helper()

	mkQuery := func() query.Query {
		clauses := make([]query.Query, 0, len(terms))
		for _, term := range terms {
			q := query.NewTermQuery(term)
			q.SetField("body")
			clauses = append(clauses, q)
		}
		return query.NewBooleanQuery(nil, clauses, nil)
	}

	type result struct {
		ids    []string
		scores []float64
		total  uint64
		rel    string
	}
	run := func(size int, mode string) result {
		req := NewSearchRequest(mkQuery())
		req.Size = size
		req.ScoreMode = mode
		res, err := idx.Search(req)
		if err != nil {
			t.Fatalf("Search(size=%d, mode=%q): %v", size, mode, err)
		}
		r := result{total: res.Total, rel: res.TotalRelation}
		for _, h := range res.Hits {
			r.ids = append(r.ids, h.ID)
			r.scores = append(r.scores, h.Score)
		}
		return r
	}

	for _, size := range sizes {
		complete := run(size, ScoreModeComplete)
		pruned := run(size, ScoreModeTopScores)

		if len(complete.ids) != len(pruned.ids) {
			t.Errorf("size=%d: complete returned %d hits, top_scores returned %d",
				size, len(complete.ids), len(pruned.ids))
			continue
		}

		// The score sequence is the recall check and is tie-insensitive: if
		// pruning dropped a document that belongs in the top-k, every later
		// rank shifts up and the scores diverge.  Documents whose scores tie
		// (or differ by float noise) are interchangeable at a given rank —
		// their relative order depends on collection order, which pruning
		// legitimately changes — so ids are only compared at ranks whose score
		// is distinct from its neighbours in both result sets.
		const tol = 1e-9
		tied := func(a, b float64) bool { return math.Abs(a-b) <= tol }

		// A truncated window hides one neighbour: the last returned rank may tie
		// with the first document that did not fit, and nothing in the window
		// reveals that.  Its id is therefore not determined either.  This corpus
		// really does produce such ties — the same document shape occurs once per
		// batch, so the top five documents all score identically — and at size=1
		// the last rank is also the first.
		truncated := len(complete.ids) == size && complete.total > uint64(size)

		mismatch := false
		for i := range complete.scores {
			if !tied(complete.scores[i], pruned.scores[i]) {
				t.Errorf("size=%d rank %d: complete score %v (%s), top_scores score %v (%s) "+
					"— pruning dropped a document that belongs in the top-k",
					size, i, complete.scores[i], complete.ids[i],
					pruned.scores[i], pruned.ids[i])
				mismatch = true
				break
			}
			unique := true
			for _, seq := range [][]float64{complete.scores, pruned.scores} {
				if i > 0 && tied(seq[i-1], seq[i]) {
					unique = false
				}
				if i+1 < len(seq) && tied(seq[i], seq[i+1]) {
					unique = false
				}
			}
			if truncated && i == len(complete.scores)-1 {
				unique = false
			}
			if unique && complete.ids[i] != pruned.ids[i] {
				t.Errorf("size=%d rank %d: complete id=%s, top_scores id=%s "+
					"at the untied score %v — wrong document",
					size, i, complete.ids[i], pruned.ids[i], complete.scores[i])
				mismatch = true
				break
			}
		}

		// Total may shrink under pruning, never grow.
		if pruned.total > complete.total {
			t.Errorf("size=%d: top_scores Total=%d exceeds complete Total=%d",
				size, pruned.total, complete.total)
			mismatch = true
		}
		if !mismatch {
			t.Logf("size=%-3d ok — %d hits identical; Total complete=%d top_scores=%d (%s)",
				size, len(complete.ids), complete.total, pruned.total, pruned.rel)
		}
	}
}

// TestWANDTopScoresMatchesCompleteSingleSegment pins the same contract on a
// single-segment index, where the §15 per-segment ceiling cannot contribute.
// A failure here localises the fault to the per-term bound or the MAXSCORE
// partition rather than to segment skipping.
func TestWANDTopScoresMatchesCompleteSingleSegment(t *testing.T) {
	idx, terms := buildWANDEquivIndex(t, 1, 2000)

	clauses := make([]query.Query, 0, len(terms))
	for _, term := range terms {
		q := query.NewTermQuery(term)
		q.SetField("body")
		clauses = append(clauses, q)
	}
	bq := query.NewBooleanQuery(nil, clauses, nil)

	for _, size := range []int{10, 50, 200} {
		var got [2][]float64
		for mi, mode := range []string{ScoreModeComplete, ScoreModeTopScores} {
			req := NewSearchRequest(bq)
			req.Size = size
			req.ScoreMode = mode
			res, err := idx.Search(req)
			if err != nil {
				t.Fatalf("Search(size=%d, mode=%q): %v", size, mode, err)
			}
			for _, h := range res.Hits {
				got[mi] = append(got[mi], h.Score)
			}
		}
		if len(got[0]) != len(got[1]) {
			t.Errorf("size=%d: %d complete hits vs %d top_scores hits",
				size, len(got[0]), len(got[1]))
			continue
		}
		// Score sequence equality is the recall check; see the note in
		// TestWANDTopScoresMatchesComplete on why ids may swap within ties.
		for i := range got[0] {
			if math.Abs(got[0][i]-got[1][i]) > 1e-9 {
				t.Errorf("size=%d rank %d: complete score %v, top_scores score %v "+
					"— single-segment, so the fault is in the per-term bound or "+
					"the MAXSCORE partition, not §15 segment skipping",
					size, i, got[0][i], got[1][i])
				break
			}
		}
	}
}

// TestWANDTopScoresMatchesCompleteZapV17 pins the same contract when the index
// on disk predates this branch's segment format.  A zapx v17 segment has no §20
// norm column and no §14 MaxTFNorm sidecar, and its Dictionary/PostingsList
// implement neither optional MaxTFNorm interface, so no per-term bound can be
// computed for it at all.
//
// That case has to be distinguished from a bound of zero.  A zero bound means
// "this term can contribute nothing", which is a licence to prune every
// candidate; before maxTFNormUnavailable existed, this test failed hard —
// top_scores returned the wrong documents and Total collapsed to size+1 (e.g.
// Total=2 against complete's 1495).  The equivalent v18 tests could not catch
// it, since every v18 segment supplies a real bound.
//
// This is not a hypothetical migration: an index written by an earlier bleve is
// read in place, never rewritten in the new format, so a deployment that upgrades
// binaries lands here and stays here.  scorch pins the plugin from the version
// recorded in bolt meta at open time, so such an index is homogeneous rather than
// mixed — segments added after the upgrade are written in the old format too.
// The cross-segment bound in IndexSnapshotTermFieldReader.MaxTFNorm is
// nevertheless poisoned by any single segment that cannot supply a bound, which
// keeps a mixed snapshot sound if one ever became reachable.
func TestWANDTopScoresMatchesCompleteZapV17(t *testing.T) {
	idx, terms := buildWANDEquivIndexWithConfig(t, 5, 400, map[string]interface{}{
		"forceSegmentType":    "zap",
		"forceSegmentVersion": 17,
	})
	assertTopScoresMatchesComplete(t, idx, terms, 1, 5, 10, 50, 200)
}
