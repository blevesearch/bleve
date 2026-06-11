// Copyright (c) 2024 Couchbase, Inc.
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

// Integration tests for §7 parallel segment search (search_parallel_segment.go).
//
// TestParallelSegmentSearchUnadornedConjunction is the regression test for the
// nil-postings panic in IndexSnapshotTermFieldReader.ShardView.
//
// Before the fix in index/scorch/snapshot_index_tfr.go, the following sequence
// panicked:
//   1. NewConjunctionSearcher with Score="none" fired the conjunction:unadorned
//      bitmap push-down (OptimizeTFRConjunctionUnadorned.Finish), producing a
//      TermSearcher whose TFR has nil postings.
//   2. NewDisjunctionSearcher (with disjunction:unadorned disabled) created a
//      DisjunctionSliceSearcher containing that TermSearcher.
//   3. On the first Next() call with EnableParallelSegmentSearch=true,
//      runParallelSegmentSearch called ForSegmentRange → ShardView on the
//      nil-postings TFR, which panicked trying to slice a nil slice.

import (
	"context"
	"os"
	"regexp"
	"sort"
	"testing"

	"github.com/blevesearch/bleve/v2/analysis"
	regexpTokenizer "github.com/blevesearch/bleve/v2/analysis/tokenizer/regexp"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// buildMultiBatchScorchIndex creates a Scorch index at dir and indexes docs in
// multiple batches so the engine produces at least two on-disk segments.
// Corpus:
//
//	d1: f="alpha beta"   → intersection(alpha,beta) = {d1,d5}
//	d2: f="alpha gamma"  → gamma = {d2,d3}
//	d3: f="beta gamma"
//	d4: f="delta"        (control doc, no overlap with test terms)
//	d5: f="alpha beta"   → intersection(alpha,beta) = {d1,d5}
//	d6: f="delta"        (control)
func buildMultiBatchScorchIndex(t *testing.T, dir string) index.Index {
	t.Helper()
	analyzer := &analysis.DefaultAnalyzer{
		Tokenizer: regexpTokenizer.NewRegexpTokenizer(regexp.MustCompile(`\w+`)),
	}
	aq := index.NewAnalysisQueue(1)
	idx, err := scorch.NewScorch(scorch.Name, map[string]interface{}{"path": dir}, aq)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Open(); err != nil {
		t.Fatal(err)
	}

	type docDef struct{ id, terms string }
	batches := [][]docDef{
		{{"d1", "alpha beta"}, {"d2", "alpha gamma"}},
		{{"d3", "beta gamma"}, {"d4", "delta"}},
		{{"d5", "alpha beta"}, {"d6", "delta"}},
	}
	for _, batch := range batches {
		b := index.NewBatch()
		for _, d := range batch {
			doc := document.NewDocument(d.id)
			// IndexField only — no term vectors, enabling 1-hit encoding for
			// single-occurrence terms.
			doc.AddField(document.NewTextFieldCustom("f", nil, []byte(d.terms),
				index.IndexField, analyzer))
			b.Update(doc)
		}
		if err := idx.Batch(b); err != nil {
			t.Fatal(err)
		}
	}
	return idx
}

// collectMatches drains a Searcher and returns sorted external IDs.
func collectMatches(t *testing.T, searcher search.Searcher, reader index.IndexReader) []string {
	t.Helper()
	ctx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(searcher.DocumentMatchPoolSize()+10, 0),
	}
	var ids []string
	for {
		m, err := searcher.Next(ctx)
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
		ids = append(ids, ext)
		ctx.DocumentMatchPool.Put(m)
	}
	sort.Strings(ids)
	return ids
}

// TestParallelSegmentSearchUnadornedConjunction is the §7 regression test for
// the nil-postings ShardView panic. Query: (alpha AND beta) OR gamma with
// Score="none" and EnableParallelSegmentSearch=true.
//
// Before the fix: ShardView panicked slicing a nil postings slice when the
// conjunction:unadorned optimization had already built the AND'd bitmap and
// left the TFR with postings==nil.
func TestParallelSegmentSearchUnadornedConjunction(t *testing.T) {
	dir, err := os.MkdirTemp("", "parallel-seg-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	idx := buildMultiBatchScorchIndex(t, dir)
	defer idx.Close()

	// Disable the disjunction:unadorned push-down so that the outer disjunction
	// falls through to DisjunctionSliceSearcher rather than being replaced by a
	// single TermSearcher. This is the precondition that exposes §7 + nil-postings.
	origDisjOpt := scorch.OptimizeDisjunctionUnadorned
	scorch.OptimizeDisjunctionUnadorned = false
	defer func() { scorch.OptimizeDisjunctionUnadorned = origDisjOpt }()

	// Enable §7 parallel segment search and lower the minimum segment threshold
	// so it fires on our 3-segment test index.
	origParallel := EnableParallelSegmentSearch
	origMinSegs := ParallelSegmentSearchMinSegs
	EnableParallelSegmentSearch = true
	ParallelSegmentSearchMinSegs = 2
	defer func() {
		EnableParallelSegmentSearch = origParallel
		ParallelSegmentSearchMinSegs = origMinSegs
	}()

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	noneOpts := search.SearcherOptions{Score: "none"}

	// Inner conjunction: alpha AND beta → {d1, d5}.
	// With Score="none", OptimizeTFRConjunctionUnadorned fires and the result is
	// a TermSearcher wrapping a nil-postings (unadorned) TFR.
	alphaTS, err := NewTermSearcher(context.TODO(), reader, "alpha", "f", 1.0, noneOpts)
	if err != nil {
		t.Fatal(err)
	}
	betaTS, err := NewTermSearcher(context.TODO(), reader, "beta", "f", 1.0, noneOpts)
	if err != nil {
		t.Fatal(err)
	}
	conjTS, err := NewConjunctionSearcher(context.TODO(), reader, []search.Searcher{alphaTS, betaTS}, noneOpts)
	if err != nil {
		t.Fatal(err)
	}

	// Outer term for the disjunction: gamma → {d2, d3}.
	gammaTS, err := NewTermSearcher(context.TODO(), reader, "gamma", "f", 1.0, noneOpts)
	if err != nil {
		t.Fatal(err)
	}

	// Outer disjunction: (alpha AND beta) OR gamma → {d1, d2, d3, d5}.
	// With disjunction:unadorned disabled this creates a DisjunctionSliceSearcher.
	// On the first Next() call, shouldRunParallel returns true (≥2 segments,
	// GOMAXPROCS≥2, both children are *TermSearcher with non-nil term).
	// runParallelSegmentSearch then calls ForSegmentRange → ShardView on the
	// nil-postings conjunction-unadorned TFR.  Before the fix this panicked.
	disjSearcher, err := NewDisjunctionSearcher(context.TODO(), reader,
		[]search.Searcher{conjTS, gammaTS}, 0, noneOpts)
	if err != nil {
		t.Fatal(err)
	}
	defer disjSearcher.Close()

	got := collectMatches(t, disjSearcher, reader)

	want := []string{"d1", "d2", "d3", "d5"}
	if !strSlicesEqual(got, want) {
		t.Errorf("(alpha AND beta) OR gamma: got %v, want %v", got, want)
	}
}

// TestParallelSegmentSearchCorrectness verifies that several Score="none"
// disjunction queries return the correct document sets when
// EnableParallelSegmentSearch=true, including cases where one branch is an
// unadorned conjunction TermSearcher with nil postings.
func TestParallelSegmentSearchCorrectness(t *testing.T) {
	dir, err := os.MkdirTemp("", "parallel-seg-correct-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	idx := buildMultiBatchScorchIndex(t, dir)
	defer idx.Close()

	origDisjOpt := scorch.OptimizeDisjunctionUnadorned
	scorch.OptimizeDisjunctionUnadorned = false
	defer func() { scorch.OptimizeDisjunctionUnadorned = origDisjOpt }()

	origParallel := EnableParallelSegmentSearch
	origMinSegs := ParallelSegmentSearchMinSegs
	EnableParallelSegmentSearch = true
	ParallelSegmentSearchMinSegs = 2
	defer func() {
		EnableParallelSegmentSearch = origParallel
		ParallelSegmentSearchMinSegs = origMinSegs
	}()

	cases := []struct {
		name         string
		buildSearcher func(reader index.IndexReader, opts search.SearcherOptions) (search.Searcher, error)
		want         []string
	}{
		{
			name: "simple alpha OR beta",
			buildSearcher: func(r index.IndexReader, opts search.SearcherOptions) (search.Searcher, error) {
				a, err := NewTermSearcher(context.TODO(), r, "alpha", "f", 1.0, opts)
				if err != nil {
					return nil, err
				}
				b, err := NewTermSearcher(context.TODO(), r, "beta", "f", 1.0, opts)
				if err != nil {
					a.Close()
					return nil, err
				}
				return NewDisjunctionSearcher(context.TODO(), r, []search.Searcher{a, b}, 0, opts)
			},
			want: []string{"d1", "d2", "d3", "d5"},
		},
		{
			name: "(alpha AND beta) OR gamma — nil-postings shard path",
			buildSearcher: func(r index.IndexReader, opts search.SearcherOptions) (search.Searcher, error) {
				a, err := NewTermSearcher(context.TODO(), r, "alpha", "f", 1.0, opts)
				if err != nil {
					return nil, err
				}
				b, err := NewTermSearcher(context.TODO(), r, "beta", "f", 1.0, opts)
				if err != nil {
					a.Close()
					return nil, err
				}
				conj, err := NewConjunctionSearcher(context.TODO(), r, []search.Searcher{a, b}, opts)
				if err != nil {
					return nil, err
				}
				g, err := NewTermSearcher(context.TODO(), r, "gamma", "f", 1.0, opts)
				if err != nil {
					conj.Close()
					return nil, err
				}
				return NewDisjunctionSearcher(context.TODO(), r, []search.Searcher{conj, g}, 0, opts)
			},
			want: []string{"d1", "d2", "d3", "d5"},
		},
		{
			name: "beta OR delta",
			buildSearcher: func(r index.IndexReader, opts search.SearcherOptions) (search.Searcher, error) {
				a, err := NewTermSearcher(context.TODO(), r, "beta", "f", 1.0, opts)
				if err != nil {
					return nil, err
				}
				b, err := NewTermSearcher(context.TODO(), r, "delta", "f", 1.0, opts)
				if err != nil {
					a.Close()
					return nil, err
				}
				return NewDisjunctionSearcher(context.TODO(), r, []search.Searcher{a, b}, 0, opts)
			},
			want: []string{"d1", "d3", "d4", "d5", "d6"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := idx.Reader()
			if err != nil {
				t.Fatal(err)
			}
			defer reader.Close()

			opts := search.SearcherOptions{Score: "none"}
			s, err := tc.buildSearcher(reader, opts)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			got := collectMatches(t, s, reader)
			if !strSlicesEqual(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func strSlicesEqual(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
