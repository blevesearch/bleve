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

package searcher

// Nested-searcher correctness tests for §7 parallel segment search.
//
// DisjunctionSliceSearcher.Next() runs the parallel fan-out on its first call
// and then drains a score-ordered cache (parallelResults). That cache cannot
// be sought by ID, but a parent ConjunctionSearcher / BooleanSearcher calls
// Next() in initSearchers and then Advance(ID) during alignment. §7 therefore
// only fans out for the request's root searcher, which the executor marks via
// MarkParallelSegmentSearchRoot; a nested DSS always takes the serial path,
// and Advance() on a parallelized DSS is an error (unreachable safety net).
//
// Each sub-test runs the same searcher tree twice against the same index:
// once serially (no ctx key, EnableParallelSegmentSearch=false) and once with
// search.ParallelSegmentSearchKey requesting the parallel path, marks the
// root exactly as index_impl does, then compares the full result sets (doc
// IDs + scores) and asserts which DSSes actually took the parallel path.

import (
	"context"
	"fmt"
	"math"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/analysis"
	regexpTokenizer "github.com/blevesearch/bleve/v2/analysis/tokenizer/regexp"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

const nestedTestField = "f"

// nestedTestSegments is the number of batches (and therefore on-disk
// segments) the fixture index is built from. Must be >= the default
// ParallelSegmentSearchMinSegs (6) so the parallel gate is satisfied without
// lowering the global.
const nestedTestSegments = 8

// buildNestedScorchIndex indexes nestedTestSegments batches of 5 docs each.
// Per batch i (ids are zero-padded so lexical order == insertion order):
//
//	s{i}_1: "x a"       → in X and in (A|B|C)
//	s{i}_2: "a a b"     → disjunction only (high tf)
//	s{i}_3: "x z"       → X only
//	s{i}_4: even i: "x c c" / odd i: "b x"   → in X and in (A|B|C)
//	s{i}_5: "c"         → disjunction only
//
// Per segment: 4 disjunction matches, 3 X matches, 2 in the intersection.
// Totals over 8 segments: |A|B|C| = 32, |X| = 24, |X ∧ (A|B|C)| = 16.
// Term frequencies and doc lengths vary so the parallel path's score-ordered
// cache is NOT doc-ID ordered.
func buildNestedScorchIndex(t *testing.T, dir string) index.Index {
	t.Helper()
	analyzer := &analysis.DefaultAnalyzer{
		Tokenizer: regexpTokenizer.NewRegexpTokenizer(regexp.MustCompile(`\w+`)),
	}
	aq := index.NewAnalysisQueue(1)
	// The scorch persister merges every unpersisted in-memory segment into one
	// file segment on each flush, and the file merger then collapses small
	// file segments (the planner's budget divides raw live size by the 2000
	// doc floor, so tiny segments are always "over budget"). Make every
	// segment ineligible for file merging (LiveSize < MaxSegmentSize/2 never
	// holds with MaxSegmentSize=2) and (below) wait for each batch to persist
	// before issuing the next so the in-memory merge never sees >1 segment.
	cfg := map[string]interface{}{
		"path": dir,
		"scorchMergePlanOptions": map[string]interface{}{
			"maxSegmentSize": 2,
		},
	}
	idx, err := scorch.NewScorch(scorch.Name, cfg, aq)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Open(); err != nil {
		t.Fatal(err)
	}

	waitPersisted := func(wantFileSegs int) {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			sm := idx.StatsMap()
			mem, _ := sm["num_root_memorysegments"].(uint64)
			file, _ := sm["num_root_filesegments"].(uint64)
			if mem == 0 && int(file) >= wantFileSegs {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for batch to persist (stats=%v)", idx.StatsMap())
	}

	for i := 0; i < nestedTestSegments; i++ {
		b := index.NewBatch()
		d4 := "x c c"
		if i%2 == 1 {
			d4 = "b x"
		}
		docs := []struct{ suffix, terms string }{
			{"1", "x a"},
			{"2", "a a b"},
			{"3", "x z"},
			{"4", d4},
			{"5", "c"},
		}
		for _, d := range docs {
			doc := document.NewDocument(fmt.Sprintf("s%02d_%s", i, d.suffix))
			doc.AddField(document.NewTextFieldCustom(nestedTestField, nil, []byte(d.terms),
				index.IndexField, analyzer))
			b.Update(doc)
		}
		if err := idx.Batch(b); err != nil {
			t.Fatal(err)
		}
		waitPersisted(i + 1)
	}
	return idx
}

type nestedResult struct {
	id    string
	score float64
}

// collectNested drains a Searcher via Next() until nil and returns results in
// the order the searcher produced them.
func collectNested(t *testing.T, s search.Searcher, reader index.IndexReader) []nestedResult {
	t.Helper()
	ctx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(s.DocumentMatchPoolSize()+10, 0),
	}
	var out []nestedResult
	for {
		m, err := s.Next(ctx)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if m == nil {
			break
		}
		ext, err := reader.ExternalID(m.IndexInternalID)
		if err != nil {
			t.Fatalf("ExternalID: %v", err)
		}
		out = append(out, nestedResult{id: ext, score: m.Score})
		ctx.DocumentMatchPool.Put(m)
	}
	return out
}

func fmtNested(rs []nestedResult) string {
	parts := make([]string, len(rs))
	for i, r := range rs {
		parts[i] = fmt.Sprintf("%s=%.4f", r.id, r.score)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func sortedIDs(rs []nestedResult) []string {
	ids := make([]string, len(rs))
	for i, r := range rs {
		ids[i] = r.id
	}
	sort.Strings(ids)
	return ids
}

// compareNested reports duplicates, missing/extra docs and score mismatches
// between the serial and parallel result sets. Returns true when identical.
func compareNested(t *testing.T, label string, serial, parallel []nestedResult) bool {
	t.Helper()
	ok := true

	dup := func(name string, rs []nestedResult) {
		seen := map[string]int{}
		for _, r := range rs {
			seen[r.id]++
		}
		for id, n := range seen {
			if n > 1 {
				ok = false
				t.Errorf("%s: %s returned doc %s %d times", label, name, id, n)
			}
		}
	}
	dup("serial", serial)
	dup("parallel", parallel)

	sm := map[string]float64{}
	for _, r := range serial {
		sm[r.id] = r.score
	}
	pm := map[string]float64{}
	for _, r := range parallel {
		pm[r.id] = r.score
	}
	var missing, extra, scoreDiff []string
	for id, ss := range sm {
		ps, found := pm[id]
		if !found {
			missing = append(missing, id)
			continue
		}
		if math.Abs(ss-ps) > 1e-6*math.Max(1, math.Abs(ss)) {
			scoreDiff = append(scoreDiff, fmt.Sprintf("%s serial=%.6f parallel=%.6f", id, ss, ps))
		}
	}
	for id := range pm {
		if _, found := sm[id]; !found {
			extra = append(extra, id)
		}
	}
	sort.Strings(missing)
	sort.Strings(extra)
	sort.Strings(scoreDiff)

	if len(missing) > 0 {
		ok = false
		t.Errorf("%s: %d docs MISSING from parallel result: %v", label, len(missing), missing)
	}
	if len(extra) > 0 {
		ok = false
		t.Errorf("%s: %d EXTRA docs in parallel result (not in serial): %v", label, len(extra), extra)
	}
	if len(scoreDiff) > 0 {
		ok = false
		t.Errorf("%s: %d docs with SCORE MISMATCH: %v", label, len(scoreDiff), scoreDiff)
	}
	t.Logf("%s: serial   %d docs %s", label, len(serial), fmtNested(serial))
	t.Logf("%s: parallel %d docs %s", label, len(parallel), fmtNested(parallel))
	return ok
}

// nestedTree builds one of the searcher shapes under test. Returns the root
// searcher plus the DSS it contains (for type assertions in the test).
type nestedTree func(ctx context.Context, r index.IndexReader, opts search.SearcherOptions) (search.Searcher, *DisjunctionSliceSearcher, error)

func newNestedDSS(ctx context.Context, r index.IndexReader, opts search.SearcherOptions, min float64) (*DisjunctionSliceSearcher, error) {
	var subs []search.Searcher
	for _, term := range []string{"a", "b", "c"} {
		ts, err := NewTermSearcher(ctx, r, term, nestedTestField, 1.0, opts)
		if err != nil {
			for _, s := range subs {
				_ = s.Close()
			}
			return nil, err
		}
		subs = append(subs, ts)
	}
	s, err := NewDisjunctionSearcher(ctx, r, subs, min, opts)
	if err != nil {
		return nil, err
	}
	dss, ok := s.(*DisjunctionSliceSearcher)
	if !ok {
		_ = s.Close()
		return nil, fmt.Errorf("expected *DisjunctionSliceSearcher, got %T", s)
	}
	return dss, nil
}

func TestParallelSegmentSearchNested(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("§7 declines with GOMAXPROCS < 2; the root case cannot fan out")
	}
	dir, err := os.MkdirTemp("", "parallel-seg-nested-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	idx := buildNestedScorchIndex(t, dir)
	defer idx.Close()

	// Serial baseline must not be affected by the global toggle.
	origParallel := EnableParallelSegmentSearch.Load()
	EnableParallelSegmentSearch.Store(false)
	defer EnableParallelSegmentSearch.Store(origParallel)

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	opts := search.SearcherOptions{} // default scoring (not "none") so no unadorned push-down

	// Confirm the fixture satisfies the real segment gate (>= 6).
	{
		ts, err := NewTermSearcher(context.Background(), reader, "a", nestedTestField, 1.0, opts)
		if err != nil {
			t.Fatal(err)
		}
		numSegs := ts.(*TermSearcher).NumSegments()
		_ = ts.Close()
		t.Logf("fixture index has %d segments (ParallelSegmentSearchMinSegs=%d)", numSegs, ParallelSegmentSearchMinSegs)
		if numSegs < ParallelSegmentSearchMinSegs {
			t.Fatalf("fixture has only %d segments; need >= %d", numSegs, ParallelSegmentSearchMinSegs)
		}
	}

	serialCtx := context.Background()
	// shardK=64 >= the total number of disjunction matches (32), so no shard
	// can ever drop a candidate from its top-K heap regardless of how many of
	// the 8 segments a shard spans (p = min(GOMAXPROCS, 8)): any missing doc
	// is attributable to Advance(), not to K.
	parallelCtx := context.WithValue(context.Background(), search.ParallelSegmentSearchKey, 64)

	trees := []struct {
		name  string
		build nestedTree
		// wantParallel: the DSS is the request root and must take the §7 path
		// when the ctx key is set. False for every nested shape.
		wantParallel bool
	}{
		{
			name: "a_root_disjunction(a,b,c)",
			build: func(ctx context.Context, r index.IndexReader, opts search.SearcherOptions) (search.Searcher, *DisjunctionSliceSearcher, error) {
				dss, err := newNestedDSS(ctx, r, opts, 0)
				if err != nil {
					return nil, nil, err
				}
				return dss, dss, nil
			},
			wantParallel: true,
		},
		{
			name: "b_conjunction(x,disjunction(a,b,c))",
			build: func(ctx context.Context, r index.IndexReader, opts search.SearcherOptions) (search.Searcher, *DisjunctionSliceSearcher, error) {
				dss, err := newNestedDSS(ctx, r, opts, 0)
				if err != nil {
					return nil, nil, err
				}
				x, err := NewTermSearcher(ctx, r, "x", nestedTestField, 1.0, opts)
				if err != nil {
					_ = dss.Close()
					return nil, nil, err
				}
				conj, err := NewConjunctionSearcher(ctx, r, []search.Searcher{x, dss}, opts)
				if err != nil {
					return nil, nil, err
				}
				if _, ok := conj.(*ConjunctionSearcher); !ok {
					_ = conj.Close()
					return nil, nil, fmt.Errorf("expected *ConjunctionSearcher, got %T", conj)
				}
				return conj, dss, nil
			},
		},
		{
			name: "c_boolean(must:x,should:disjunction(a,b,c),min=0)",
			build: func(ctx context.Context, r index.IndexReader, opts search.SearcherOptions) (search.Searcher, *DisjunctionSliceSearcher, error) {
				dss, err := newNestedDSS(ctx, r, opts, 0)
				if err != nil {
					return nil, nil, err
				}
				x, err := NewTermSearcher(ctx, r, "x", nestedTestField, 1.0, opts)
				if err != nil {
					_ = dss.Close()
					return nil, nil, err
				}
				bs, err := NewBooleanSearcher(ctx, r, x, dss, nil, opts)
				if err != nil {
					return nil, nil, err
				}
				return bs, dss, nil
			},
		},
		{
			// BooleanQuery.SetMinShould(1): should clause becomes mandatory,
			// so the result SET (not just scores) depends on Advance().
			name: "c2_boolean(must:x,should:disjunction(a,b,c),min=1)",
			build: func(ctx context.Context, r index.IndexReader, opts search.SearcherOptions) (search.Searcher, *DisjunctionSliceSearcher, error) {
				dss, err := newNestedDSS(ctx, r, opts, 1)
				if err != nil {
					return nil, nil, err
				}
				x, err := NewTermSearcher(ctx, r, "x", nestedTestField, 1.0, opts)
				if err != nil {
					_ = dss.Close()
					return nil, nil, err
				}
				bs, err := NewBooleanSearcher(ctx, r, x, dss, nil, opts)
				if err != nil {
					return nil, nil, err
				}
				return bs, dss, nil
			},
		},
	}

	for _, tc := range trees {
		t.Run(tc.name, func(t *testing.T) {
			// --- serial ---
			sRoot, sDSS, err := tc.build(serialCtx, reader, opts)
			if err != nil {
				t.Fatal(err)
			}
			MarkParallelSegmentSearchRoot(sRoot) // as index_impl does; flag off → no effect
			serial := collectNested(t, sRoot, reader)
			if sDSS.parallelResults != nil {
				t.Errorf("serial run unexpectedly took the parallel path")
			}
			_ = sRoot.Close()

			// --- parallel (requested via ctx key) ---
			pRoot, pDSS, err := tc.build(parallelCtx, reader, opts)
			if err != nil {
				t.Fatal(err)
			}
			// Mark exactly what index_impl marks: the root of the tree. For
			// the nested shapes the root is a conjunction/boolean, so the DSS
			// beneath it stays unmarked and must decline.
			marked := MarkParallelSegmentSearchRoot(pRoot)
			if marked != tc.wantParallel {
				t.Fatalf("MarkParallelSegmentSearchRoot(%T) = %v, want %v", pRoot, marked, tc.wantParallel)
			}
			evalBefore := ParallelSegmentSearchStats.Evaluated.Load()
			ranBefore := ParallelSegmentSearchStats.Ran.Load()
			nestedBefore := ParallelSegmentSearchStats.DeclinedNested.Load()
			parallel := collectNested(t, pRoot, reader)
			if !pDSS.parallelDecided {
				t.Errorf("shouldRunParallel was never consulted on the DSS")
			}
			if tc.wantParallel {
				// One root run is exactly one evaluation that ran: the shard
				// DSSes created by the fan-out must not be evaluated (and
				// must not be counted as nested declines).
				if d := ParallelSegmentSearchStats.Evaluated.Load() - evalBefore; d != 1 {
					t.Errorf("Evaluated moved by %d, want 1", d)
				}
				if d := ParallelSegmentSearchStats.Ran.Load() - ranBefore; d != 1 {
					t.Errorf("Ran moved by %d, want 1", d)
				}
				if d := ParallelSegmentSearchStats.DeclinedNested.Load() - nestedBefore; d != 0 {
					t.Errorf("DeclinedNested moved by %d, want 0 (shard DSSes leaked into the counters)", d)
				}
			}
			tookParallel := pDSS.parallelResults != nil
			switch {
			case tc.wantParallel && !tookParallel:
				t.Errorf("root DSS did not take the parallel path")
			case !tc.wantParallel && tookParallel:
				t.Errorf("nested DSS took the parallel path (cached %d results); "+
					"the root-only guard must decline it", len(pDSS.parallelResults))
			case tookParallel:
				t.Logf("root DSS cached %d score-ordered results; drained %d of them",
					len(pDSS.parallelResults), pDSS.parallelPos)
			default:
				t.Logf("nested DSS correctly declined the parallel path")
			}
			_ = pRoot.Close()

			if compareNested(t, tc.name, serial, parallel) {
				t.Logf("%s: serial and parallel result sets are IDENTICAL (%d docs)", tc.name, len(serial))
			} else {
				t.Logf("%s: serial ids   %v", tc.name, sortedIDs(serial))
				t.Logf("%s: parallel ids %v", tc.name, sortedIDs(parallel))
			}
		})
	}
}

// TestParallelSegmentSearchUnmarkedNeverCaches isolates the root-only guard:
// a DSS that was never passed to MarkParallelSegmentSearchRoot must not take
// the parallel path even when the ctx key explicitly requests it, and the
// decline must be attributed to "nested".
func TestParallelSegmentSearchUnmarkedNeverCaches(t *testing.T) {
	dir, err := os.MkdirTemp("", "parallel-seg-nested-unmarked-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	idx := buildNestedScorchIndex(t, dir)
	defer idx.Close()

	origParallel := EnableParallelSegmentSearch.Load()
	EnableParallelSegmentSearch.Store(false)
	defer EnableParallelSegmentSearch.Store(origParallel)

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	ctx := context.WithValue(context.Background(), search.ParallelSegmentSearchKey, 10)
	dss, err := newNestedDSS(ctx, reader, search.SearcherOptions{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dss.Close()

	evalBefore := ParallelSegmentSearchStats.Evaluated.Load()
	nestedBefore := ParallelSegmentSearchStats.DeclinedNested.Load()
	ranBefore := ParallelSegmentSearchStats.Ran.Load()

	got := collectNested(t, dss, reader)
	if len(got) != 32 {
		t.Errorf("unmarked DSS returned %d docs, want 32", len(got))
	}
	if !dss.parallelDecided {
		t.Error("shouldRunParallel was never consulted")
	}
	if dss.parallelResults != nil {
		t.Errorf("unmarked DSS cached %d parallel results; expected nil", len(dss.parallelResults))
	}
	if d := ParallelSegmentSearchStats.Evaluated.Load() - evalBefore; d != 1 {
		t.Errorf("Evaluated moved by %d, want 1", d)
	}
	if d := ParallelSegmentSearchStats.DeclinedNested.Load() - nestedBefore; d != 1 {
		t.Errorf("DeclinedNested moved by %d, want 1", d)
	}
	if d := ParallelSegmentSearchStats.Ran.Load() - ranBefore; d != 0 {
		t.Errorf("Ran moved by %d, want 0", d)
	}
}

// TestParallelSegmentSearchIneligibleRootDeclines checks the request-shape
// gate: a root DSS marked via MarkParallelSegmentSearchIneligible (the
// executor does this for non-score sorts, facets, search_after/before and
// KNN hybrid requests) must never fan out — even with the explicit ctx key —
// and must count as "declined_request", not "declined_nested", while
// returning exactly the serial results.
func TestParallelSegmentSearchIneligibleRootDeclines(t *testing.T) {
	dir, err := os.MkdirTemp("", "parallel-seg-nested-ineligible-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	idx := buildNestedScorchIndex(t, dir)
	defer idx.Close()

	origParallel := EnableParallelSegmentSearch.Load()
	EnableParallelSegmentSearch.Store(false)
	defer EnableParallelSegmentSearch.Store(origParallel)

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	serialDSS, err := newNestedDSS(context.Background(), reader, search.SearcherOptions{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer serialDSS.Close()
	serial := collectNested(t, serialDSS, reader)

	ctx := context.WithValue(context.Background(), search.ParallelSegmentSearchKey, 64)
	dss, err := newNestedDSS(ctx, reader, search.SearcherOptions{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dss.Close()
	if !MarkParallelSegmentSearchIneligible(dss) {
		t.Fatal("MarkParallelSegmentSearchIneligible returned false for a DSS")
	}

	evalBefore := ParallelSegmentSearchStats.Evaluated.Load()
	reqBefore := ParallelSegmentSearchStats.DeclinedRequest.Load()
	nestedBefore := ParallelSegmentSearchStats.DeclinedNested.Load()
	ranBefore := ParallelSegmentSearchStats.Ran.Load()

	got := collectNested(t, dss, reader)
	if !compareNested(t, "ineligible_root", serial, got) {
		t.Error("ineligible root DSS results differ from serial")
	}
	if dss.parallelResults != nil {
		t.Errorf("ineligible root DSS cached %d parallel results; expected nil", len(dss.parallelResults))
	}
	if d := ParallelSegmentSearchStats.Evaluated.Load() - evalBefore; d != 1 {
		t.Errorf("Evaluated moved by %d, want 1", d)
	}
	if d := ParallelSegmentSearchStats.DeclinedRequest.Load() - reqBefore; d != 1 {
		t.Errorf("DeclinedRequest moved by %d, want 1", d)
	}
	if d := ParallelSegmentSearchStats.DeclinedNested.Load() - nestedBefore; d != 0 {
		t.Errorf("DeclinedNested moved by %d, want 0", d)
	}
	if d := ParallelSegmentSearchStats.Ran.Load() - ranBefore; d != 0 {
		t.Errorf("Ran moved by %d, want 0", d)
	}
}

// TestParallelSegmentSearchTruncationSetsWANDPruned checks that the parallel
// path reports a lower-bound Total: each shard keeps only a top-shardK heap,
// so when any shard evicts a candidate the merged result is truncated and
// DSS.Next must set ctx.WANDPruned (SearchResult.Total gets TotalRelation
// "gte"), even though no MAXSCORE pruning happened. Conversely, a shardK
// large enough to hold every match must leave WANDPruned false.
func TestParallelSegmentSearchTruncationSetsWANDPruned(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("§7 declines with GOMAXPROCS < 2")
	}
	dir, err := os.MkdirTemp("", "parallel-seg-nested-trunc-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	idx := buildNestedScorchIndex(t, dir)
	defer idx.Close()

	origParallel := EnableParallelSegmentSearch.Load()
	EnableParallelSegmentSearch.Store(false)
	defer EnableParallelSegmentSearch.Store(origParallel)

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	// drain runs a marked root DSS with the given shardK to exhaustion via
	// Next() and returns the number of docs it produced plus the final
	// SearchContext. WANDEnabled stays false so MAXSCORE never prunes: any
	// WANDPruned=true is attributable to top-shardK truncation alone.
	drain := func(shardK int) (int, *search.SearchContext) {
		t.Helper()
		ctx := context.WithValue(context.Background(), search.ParallelSegmentSearchKey, shardK)
		dss, err := newNestedDSS(ctx, reader, search.SearcherOptions{}, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer dss.Close()
		if !MarkParallelSegmentSearchRoot(dss) {
			t.Fatal("MarkParallelSegmentSearchRoot returned false for a *DisjunctionSliceSearcher")
		}
		sctx := &search.SearchContext{
			DocumentMatchPool: search.NewDocumentMatchPool(dss.DocumentMatchPoolSize()+10, 0),
		}
		n := 0
		for {
			m, err := dss.Next(sctx)
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			if m == nil {
				break
			}
			n++
			sctx.DocumentMatchPool.Put(m)
		}
		if dss.parallelResults == nil {
			t.Fatal("marked root DSS did not take the parallel path")
		}
		return n, sctx
	}

	// shardK=2 < 4 matches per segment, so every shard evicts: the merged
	// result is truncated and Total must be reported as a lower bound.
	n, sctx := drain(2)
	if n >= 32 {
		t.Fatalf("shardK=2 returned %d docs; expected fewer than the 32 matches", n)
	}
	if !sctx.WANDPruned {
		t.Errorf("shardK=2 truncated the result (%d of 32 docs) but WANDPruned is false", n)
	}

	// shardK=64 >= 32 total matches: no shard can evict, and WAND is off, so
	// the result is complete and WANDPruned must stay false.
	n, sctx = drain(64)
	if n != 32 {
		t.Fatalf("shardK=64 returned %d docs, want 32", n)
	}
	if sctx.WANDPruned {
		t.Error("shardK=64 returned every match but WANDPruned is true")
	}
}

// TestParallelSegmentSearchAdvanceOnParallelizedRootErrors covers the
// defensive fallback in DSS.Advance(): once a (root) DSS has fanned out and
// holds a score-ordered cache, Advance(ID) must fail loudly rather than hand
// back the next cached doc. Unreachable through index_impl (nothing calls
// Advance on the root), so it is exercised directly here.
func TestParallelSegmentSearchAdvanceOnParallelizedRootErrors(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("parallel segment search needs GOMAXPROCS >= 2")
	}
	dir, err := os.MkdirTemp("", "parallel-seg-nested-adv-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	idx := buildNestedScorchIndex(t, dir)
	defer idx.Close()

	origParallel := EnableParallelSegmentSearch.Load()
	EnableParallelSegmentSearch.Store(false)
	defer EnableParallelSegmentSearch.Store(origParallel)

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	ctx := context.WithValue(context.Background(), search.ParallelSegmentSearchKey, 10)
	dss, err := newNestedDSS(ctx, reader, search.SearcherOptions{}, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer dss.Close()
	if !MarkParallelSegmentSearchRoot(dss) {
		t.Fatal("MarkParallelSegmentSearchRoot returned false for a *DisjunctionSliceSearcher")
	}

	sctx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(dss.DocumentMatchPoolSize()+10, 0),
	}
	first, err := dss.Next(sctx)
	if err != nil || first == nil {
		t.Fatalf("first Next: m=%v err=%v", first, err)
	}
	if dss.parallelResults == nil {
		t.Fatal("marked root DSS did not take the parallel path")
	}

	got, err := dss.Advance(sctx, first.IndexInternalID)
	if err == nil {
		t.Fatalf("Advance on a parallelized DSS returned (%v, nil); want an error", got)
	}
	if !strings.Contains(err.Error(), "Advance is not supported") {
		t.Errorf("unexpected Advance error: %v", err)
	}
}

// TestParallelSegmentSearchCountersHookOverrides drives shouldRunParallel
// through a real DSS and checks the observability counters, the event hook,
// and the runtime overrides for the segment, DF and concurrency gates. The 3-segment
// fixture is below the compiled ParallelSegmentSearchMinSegs (6), so the
// segs gate declines until ParallelSegmentSearchMinSegsOverride lowers it.
func TestParallelSegmentSearchCountersHookOverrides(t *testing.T) {
	if runtime.GOMAXPROCS(0) < 2 {
		t.Skip("parallel segment search needs GOMAXPROCS >= 2")
	}
	idx := buildMultiBatchScorchIndex(t, t.TempDir())
	defer func() { _ = idx.Close() }()

	origParallel := EnableParallelSegmentSearch.Load()
	origHook := ParallelSegmentSearchEventHook
	t.Cleanup(func() {
		EnableParallelSegmentSearch.Store(origParallel)
		ParallelSegmentSearchEventHook = origHook
		ParallelSegmentSearchMinSegsOverride.Store(0)
		ParallelSegmentSearchMinDFPerSegOverride.Store(0)
		ParallelSegmentSearchMaxConcurrentOverride.Store(0)
		parallelSearchesActive.Store(0)
	})
	EnableParallelSegmentSearch.Store(true)
	ParallelSegmentSearchMinSegsOverride.Store(0)
	ParallelSegmentSearchMinDFPerSegOverride.Store(0)
	ParallelSegmentSearchMaxConcurrentOverride.Store(0)
	parallelSearchesActive.Store(0)

	ir, err := idx.Reader()
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer func() { _ = ir.Close() }()

	srs, err := newDisjunctionSearcherForTest(t, ir, []string{"alpha", "beta"})
	if err != nil {
		t.Fatalf("newDisjunctionSearcherForTest: %v", err)
	}
	defer func() { _ = srs.Close() }()
	srs.parallelRoot = true

	numSegs := srs.searchers[0].(*TermSearcher).NumSegments()
	if numSegs < 2 || numSegs >= ParallelSegmentSearchMinSegs {
		t.Fatalf("fixture has %d segments; need 2 <= numSegs < %d", numSegs, ParallelSegmentSearchMinSegs)
	}
	totalDF := estimateDF(srs)
	if totalDF == 0 {
		t.Fatal("estimateDF returned 0 for alpha|beta")
	}
	topK := srs.options.TopK

	type event struct {
		name    string
		numSegs int
		totalDF uint64
		topK    int
	}
	var events []event
	ParallelSegmentSearchEventHook = func(name string, n int, df uint64, k int) {
		events = append(events, event{name, n, df, k})
	}

	type snap struct{ eval, ran, disabled, nested, segs, df, conc uint64 }
	take := func() snap {
		st := &ParallelSegmentSearchStats
		return snap{
			st.Evaluated.Load(), st.Ran.Load(), st.DeclinedDisabled.Load(), st.DeclinedNested.Load(),
			st.DeclinedSegs.Load(), st.DeclinedDF.Load(), st.DeclinedConcurrency.Load(),
		}
	}
	noWAND := &search.SearchContext{}

	// step runs shouldRunParallel once and asserts: the bool result, the
	// single counter that moved (plus Evaluated), and the hook event.
	step := func(label string, s *DisjunctionSliceSearcher, wantOK bool, want snap, wantEvent *event) {
		t.Helper()
		before := take()
		events = events[:0]
		ok, _ := shouldRunParallel(s, noWAND)
		if ok != wantOK {
			t.Errorf("%s: shouldRunParallel = %v, want %v", label, ok, wantOK)
		}
		after := take()
		got := snap{
			after.eval - before.eval, after.ran - before.ran, after.disabled - before.disabled,
			after.nested - before.nested, after.segs - before.segs, after.df - before.df, after.conc - before.conc,
		}
		if got != want {
			t.Errorf("%s: counter deltas %+v, want %+v", label, got, want)
		}
		switch {
		case wantEvent == nil && len(events) != 0:
			t.Errorf("%s: hook fired %v, want no event", label, events)
		case wantEvent != nil && len(events) != 1:
			t.Errorf("%s: hook fired %v, want exactly %+v", label, events, *wantEvent)
		case wantEvent != nil && events[0] != *wantEvent:
			t.Errorf("%s: hook event %+v, want %+v", label, events[0], *wantEvent)
		}
	}

	// 1. Compiled default min segs (6) > fixture segments → declined_segs.
	step("default segs gate", srs, false, snap{eval: 1, segs: 1},
		&event{"declined_segs", numSegs, 0, topK})

	// 2. Segs override lets the fixture through; compiled DF floor (150/seg)
	//    then declines it → declined_df with the computed totalDF.
	ParallelSegmentSearchMinSegsOverride.Store(2)
	step("segs override, default DF gate", srs, false, snap{eval: 1, df: 1},
		&event{"declined_df", numSegs, totalDF, topK})

	// 3. DF override of 1/seg passes the DF gate → ran.
	ParallelSegmentSearchMinDFPerSegOverride.Store(1)
	step("segs+DF override", srs, true, snap{eval: 1, ran: 1},
		&event{"ran", numSegs, totalDF, topK})

	// 4. DF override can also tighten the gate.
	ParallelSegmentSearchMinDFPerSegOverride.Store(1_000_000)
	step("DF override tightened", srs, false, snap{eval: 1, df: 1},
		&event{"declined_df", numSegs, totalDF, topK})

	// 5. Concurrency gate at capacity → declined_concurrency.
	ParallelSegmentSearchMinDFPerSegOverride.Store(1)
	parallelSearchesActive.Store(100)
	step("concurrency gate", srs, false, snap{eval: 1, conc: 1},
		&event{"declined_concurrency", numSegs, totalDF, topK})

	// 5b. Concurrency override replaces the computed GOMAXPROCS/p cap in
	//     both directions: with one search active, a cap of 1 declines
	//     (as the default would on a small GOMAXPROCS) and a cap of 2 runs.
	parallelSearchesActive.Store(1)
	ParallelSegmentSearchMaxConcurrentOverride.Store(1)
	step("concurrency override 1, active 1", srs, false, snap{eval: 1, conc: 1},
		&event{"declined_concurrency", numSegs, totalDF, topK})
	ParallelSegmentSearchMaxConcurrentOverride.Store(2)
	step("concurrency override 2, active 1", srs, true, snap{eval: 1, ran: 1},
		&event{"ran", numSegs, totalDF, topK})
	// 5c. Explicit ctx key bypasses the override like every adaptive guard.
	ParallelSegmentSearchMaxConcurrentOverride.Store(1)
	explicit, err := newDisjunctionSearcherForTest(t, ir, []string{"alpha", "beta"})
	if err != nil {
		t.Fatalf("newDisjunctionSearcherForTest explicit: %v", err)
	}
	defer func() { _ = explicit.Close() }()
	explicit.ctx = context.WithValue(context.Background(), search.ParallelSegmentSearchKey, 10)
	explicit.parallelRoot = true
	step("concurrency override bypassed by ctx key", explicit, true, snap{eval: 1, ran: 1},
		&event{"ran", numSegs, 0, topK})
	ParallelSegmentSearchMaxConcurrentOverride.Store(0)
	parallelSearchesActive.Store(0)

	// 6. Unmarked DSS → declined_nested, before any adaptive guard.
	nested, err := newDisjunctionSearcherForTest(t, ir, []string{"alpha", "beta"})
	if err != nil {
		t.Fatalf("newDisjunctionSearcherForTest nested: %v", err)
	}
	defer func() { _ = nested.Close() }()
	step("unmarked DSS", nested, false, snap{eval: 1, nested: 1},
		&event{"declined_nested", 0, 0, topK})

	// 7. Flag off → declined_disabled alone (Evaluated only counts
	//    evaluations with the feature enabled), and the hook stays quiet.
	EnableParallelSegmentSearch.Store(false)
	step("flag off", srs, false, snap{disabled: 1}, nil)
	EnableParallelSegmentSearch.Store(true)

	// 8. Clearing the overrides (0) restores the compiled defaults.
	ParallelSegmentSearchMinSegsOverride.Store(0)
	ParallelSegmentSearchMinDFPerSegOverride.Store(0)
	step("overrides cleared", srs, false, snap{eval: 1, segs: 1},
		&event{"declined_segs", numSegs, 0, topK})
}
