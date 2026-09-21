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
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

// buildSkewedSegmentIndex indexes n BM25-scored documents whose field length
// alternates in stark, batch-sized steps (short, long, short, long, ...)
// rather than being uniformly distributed within every segment -- so each
// segment's own local average field length is, by construction, far from
// the corpus-wide average across all segments (a segment made of one
// all-short batch or one all-long batch is never a representative sample
// of the whole).
//
// This specifically targets a real bug class found in the bulk-scan port:
// zapx's write-time block-max bound picked, per block, whichever real
// document scored highest under a BM25 estimate computed from that block's
// own SEGMENT's local average field length -- but the query-time scorer
// ranks documents using the corpus-wide average (bleve_index_api's
// IndexSnapshot.FieldCardinality/DocCount, spanning every segment). BM25's
// ranking across documents with different (norm, tf) pairs is not invariant
// to which average is used, so a bound picked as "the best real document"
// under the wrong (local) average can score lower under the right (global)
// one than a document the write-time estimate passed over -- silently
// dropping a genuine top-K match, either via the whole-window skip or the
// phase-1 score prefilter in search_conjunction_block.go. A uniformly-
// distributed corpus (see buildConjunctionWANDIndex) doesn't reliably
// trigger this: with enough i.i.d. samples per segment, the local and
// global averages converge to nearly the same value by the law of large
// numbers, and the bound stays sound in practice even with the underlying
// bug present. This corpus deliberately breaks that convergence.
func buildSkewedSegmentIndex(t *testing.T, n int) Index {
	t.Helper()

	dir, err := os.MkdirTemp("", "conjskew")
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
		if i%97 == 0 {
			terms = append(terms, "alpha")
		}
		if i%13 == 0 {
			terms = append(terms, "beta")
		}
		if i%5 != 0 {
			terms = append(terms, "common")
		}
		// A stark step, not a gentle trend or a fine-grained alternation:
		// the corpus's first half is uniformly short, its second half
		// uniformly long. A per-batch alternation was tried first and
		// didn't survive scorch's background merging -- adjacent short/long
		// batches merge into a blended segment whose local average drifts
		// back toward the corpus-wide one, exactly the convergence this
		// corpus needs to avoid. One contiguous block per extreme is robust
		// to that: segment merges within a block stay homogeneous (short
		// merges with short, long with long) for far longer, since a
		// same-tier size-based merge policy prefers merging segments of
		// similar size drawn from nearby, similarly-aged batches.
		var fillerCount int
		if i < n/2 {
			fillerCount = 1
		} else {
			fillerCount = 120
		}
		filler := make([]string, fillerCount)
		for j := range filler {
			filler[j] = "filler"
		}
		body := strings.Join(append(terms, filler...), " ")
		if err := batch.Index(fmt.Sprintf("d%06d", i), map[string]interface{}{
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

// TestConjunctionBlockMaxWANDAgainstIndependentBaseline is the companion
// TestConjunctionBlockMaxWANDMatchesBaseline was missing: that test only
// ever compares the block-max WAND path against WAND-off *within the same
// build*, both reading through the very same TermQueryScorer.avgDocLength
// (a corpus-wide statistic) -- so it can't distinguish "correct" from "a
// write-time bound computed against a different, wrong statistic," since
// WAND-off never touches the write-time bound machinery at all and neither
// comparison side is an independent computation of the *true* per-document
// score. This test's WAND-off side plays that independent-reference role
// properly: it never reads zapx's block-max metadata (the exact thing that
// was wrong), only real per-document (tf, norm) pairs decoded one at a
// time -- so it stays correct regardless of any bug in how the write-time
// bound was chosen. See buildSkewedSegmentIndex's doc comment for why this
// needs a segment-skewed corpus rather than the smaller uniform one
// TestConjunctionBlockMaxWANDMatchesBaseline already uses: that one's
// per-segment/global averages are close enough, even with a bug present,
// that this exact class of bound violation had been shipping unnoticed.
func TestConjunctionBlockMaxWANDAgainstIndependentBaseline(t *testing.T) {
	const n = 60000
	idx := buildSkewedSegmentIndex(t, n)

	t.Cleanup(func() { searcher.EnableConjunctionBlockMaxWAND = true })

	shapes := []struct {
		name  string
		terms []string
	}{
		{"skewed-2term", []string{"alpha", "common"}},
		{"mid-2term", []string{"beta", "common"}},
		{"3term", []string{"alpha", "beta", "common"}},
	}

	for _, shape := range shapes {
		for _, size := range []int{5, 10, 50} {
			t.Run(fmt.Sprintf("%s/size=%d", shape.name, size), func(t *testing.T) {
				q := andQuery(shape.terms...)

				searcher.EnableConjunctionBlockMaxWAND = true
				reqOn := NewSearchRequest(q)
				reqOn.Size = size
				resOn, err := idx.Search(reqOn)
				if err != nil {
					t.Fatalf("WAND-on search failed: %v", err)
				}

				// The independent reference: real per-document (tf, norm)
				// pairs decoded one at a time via the plain scalar leapfrog
				// path, which never reads zapx's write-time block-max bound
				// at all -- unlike an exhaustive Size=n WAND-on scan, which
				// would still run through the very same buggy bound
				// machinery and just happen not to need it (nothing gets
				// excluded when every match is wanted), so it would
				// silently pass even with the bug this test targets present.
				searcher.EnableConjunctionBlockMaxWAND = false
				reqOff := NewSearchRequest(q)
				reqOff.Size = size
				resOff, err := idx.Search(reqOff)
				if err != nil {
					t.Fatalf("WAND-off search failed: %v", err)
				}
				searcher.EnableConjunctionBlockMaxWAND = true

				if len(resOn.Hits) != len(resOff.Hits) {
					offIDs := map[string]float64{}
					for _, h := range resOff.Hits {
						offIDs[h.ID] = h.Score
					}
					for _, h := range resOn.Hits {
						delete(offIDs, h.ID)
					}
					for id, score := range offIDs {
						t.Logf("missing from WAND-on top-%d: id=%s score=%v (present in the WAND-off baseline, should have made the cut)",
							size, id, score)
					}
					t.Fatalf("hit count mismatch: WAND-on=%d WAND-off=%d",
						len(resOn.Hits), len(resOff.Hits))
				}
				for i := range resOff.Hits {
					on, off := resOn.Hits[i], resOff.Hits[i]
					if on.ID != off.ID {
						t.Errorf("hit %d: ID mismatch: WAND-on=%s WAND-off=%s (score on=%v off=%v)",
							i, on.ID, off.ID, on.Score, off.Score)
					}
					if on.Score != off.Score {
						t.Errorf("hit %d (id=%s): score mismatch: WAND-on=%v WAND-off=%v",
							i, on.ID, on.Score, off.Score)
					}
				}
			})
		}
	}
}
