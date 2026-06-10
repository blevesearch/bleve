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

package scorch

// Tests for the roaring-bitmap push-down optimizations in optimize.go and
// the ShardView fix for nil-postings (unadorned) TFRs.
//
// Coverage:
//   OptimizeTFRConjunction              — scored conjunction bitmap AND
//   OptimizeTFRConjunctionUnadorned     — no-score conjunction bitmap AND
//   OptimizeTFRDisjunctionUnadorned     — no-score disjunction bitmap OR
//   1-hit fast path                     — single-hit terms in unadorned conjunction
//   ShardView on unadorned TFR          — nil-postings edge case (§7 fix)
//   freshIteratorForShard               — unit tests for the shard iterator helper

import (
	"context"
	"sort"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/blevesearch/bleve/v2/document"
	index "github.com/blevesearch/bleve_index_api"
)

// Corpus:
//   d1: "alpha beta"       → alpha∩beta = {d1}
//   d2: "alpha gamma"      → alpha∪gamma = {d1,d2,d3}
//   d3: "beta gamma"
//   d4: "delta"            → delta appears twice, uses general encoding
//   d5: "delta"
//   d6: "epsilon"          → epsilon appears once → 1-hit FST encoding
//   d7: "zeta"             → zeta appears once   → 1-hit FST encoding
//
// Term vectors are disabled so that 1-hit encoding fires for single-occurrence
// terms (epsilon, zeta).

func buildOptimizeTestIndex(t *testing.T) (index.Index, func()) {
	t.Helper()
	cfg := CreateConfig("TestOptimize")
	if err := InitTest(cfg); err != nil {
		t.Fatal(err)
	}
	aq := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, aq)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Open(); err != nil {
		t.Fatal(err)
	}

	docs := []struct{ id, terms string }{
		{"d1", "alpha beta"},
		{"d2", "alpha gamma"},
		{"d3", "beta gamma"},
		{"d4", "delta"},
		{"d5", "delta"},
		{"d6", "epsilon"},
		{"d7", "zeta"},
	}
	// Index in two separate batches to create at least two segments (helps
	// ShardView partial-shard tests).
	batch1 := index.NewBatch()
	batch2 := index.NewBatch()
	for k, d := range docs {
		doc := document.NewDocument(d.id)
		// IndexField only — no term vectors, enabling 1-hit encoding.
		doc.AddField(document.NewTextFieldCustom("f", nil, []byte(d.terms),
			index.IndexField, testAnalyzer))
		if k < 4 {
			batch1.Update(doc)
		} else {
			batch2.Update(doc)
		}
	}
	if err := idx.Batch(batch1); err != nil {
		t.Fatal(err)
	}
	if err := idx.Batch(batch2); err != nil {
		t.Fatal(err)
	}
	return idx, func() {
		_ = idx.Close()
		_ = DestroyTest(cfg)
	}
}

// openReaderAndTFRs opens a snapshot reader and TermFieldReaders for the
// requested terms on field "f". scored=true requests freq+norm data.
func openReaderAndTFRs(t *testing.T, idx index.Index, scored bool, terms ...string) (
	index.IndexReader, []index.TermFieldReader,
) {
	t.Helper()
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	var tfrs []index.TermFieldReader
	for _, term := range terms {
		tfr, err := reader.TermFieldReader(context.TODO(), []byte(term), "f",
			scored, scored, false)
		if err != nil {
			t.Fatal(err)
		}
		tfrs = append(tfrs, tfr)
	}
	return reader, tfrs
}

// collectDocIDs drains a TermFieldReader and returns sorted external IDs.
func collectDocIDs(t *testing.T, tfr index.TermFieldReader, reader index.IndexReader) []string {
	t.Helper()
	var ids []string
	for {
		hit, err := tfr.Next(nil)
		if err != nil {
			t.Fatal(err)
		}
		if hit == nil {
			break
		}
		extID, err := reader.ExternalID(hit.ID)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, extID)
	}
	sort.Strings(ids)
	return ids
}

// runConjunctionUnadornedOpt triggers OptimizeTFRConjunctionUnadorned via the
// Optimize/Finish API and returns the resulting oTFR, or nil if the
// optimization declined.
func runConjunctionUnadornedOpt(t *testing.T, tfrs []index.TermFieldReader) index.TermFieldReader {
	t.Helper()
	var octx index.OptimizableContext
	for _, tfr := range tfrs {
		o, ok := tfr.(index.Optimizable)
		if !ok {
			return nil
		}
		var err error
		octx, err = o.Optimize("conjunction:unadorned", octx)
		if err != nil || octx == nil {
			return nil
		}
	}
	optimized, err := octx.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if optimized == nil {
		return nil
	}
	oTFR, ok := optimized.(index.TermFieldReader)
	if !ok {
		t.Fatal("Finish did not return a TermFieldReader")
	}
	return oTFR
}

// runDisjunctionUnadornedOpt triggers OptimizeTFRDisjunctionUnadorned.
func runDisjunctionUnadornedOpt(t *testing.T, tfrs []index.TermFieldReader) index.TermFieldReader {
	t.Helper()
	var octx index.OptimizableContext
	for _, tfr := range tfrs {
		o, ok := tfr.(index.Optimizable)
		if !ok {
			return nil
		}
		var err error
		octx, err = o.Optimize("disjunction:unadorned", octx)
		if err != nil || octx == nil {
			return nil
		}
	}
	optimized, err := octx.Finish()
	if err != nil {
		t.Fatalf("Finish: %v", err)
	}
	if optimized == nil {
		return nil
	}
	oTFR, ok := optimized.(index.TermFieldReader)
	if !ok {
		t.Fatal("Finish did not return a TermFieldReader")
	}
	return oTFR
}

// ----------------------------------------------------------------
// 1. Scored conjunction bitmap AND (OptimizeTFRConjunction)
// ----------------------------------------------------------------

// TestOptimizeConjunction verifies that the scored conjunction bitmap AND
// push-down yields the intersection document set.  The optimization modifies
// iterators in-place (Finish returns nil); we iterate the first TFR to collect
// results because all TFRs share the AND'd bitmap after the push-down.
func TestOptimizeConjunction(t *testing.T) {
	tests := []struct {
		terms []string
		want  []string
	}{
		{[]string{"alpha", "beta"}, []string{"d1"}},
		{[]string{"alpha", "gamma"}, []string{"d2"}},
		{[]string{"beta", "gamma"}, []string{"d3"}},
		{[]string{"alpha", "delta"}, nil}, // no intersection
	}

	for _, tc := range tests {
		idx, cleanup := buildOptimizeTestIndex(t)
		reader, tfrs := openReaderAndTFRs(t, idx, true, tc.terms...)

		var octx index.OptimizableContext
		opted := true
		for _, tfr := range tfrs {
			o, ok := tfr.(index.Optimizable)
			if !ok {
				opted = false
				break
			}
			var err error
			octx, err = o.Optimize("conjunction", octx)
			if err != nil || octx == nil {
				opted = false
				break
			}
		}
		if opted && octx != nil {
			if _, err := octx.Finish(); err != nil {
				t.Fatalf("terms %v: Finish: %v", tc.terms, err)
			}
		}

		// After in-place AND, tfrs[0].iterators hold the AND'd bitmaps.
		got := collectDocIDs(t, tfrs[0], reader)
		for _, tfr := range tfrs {
			tfr.Close()
		}
		reader.Close()
		cleanup()

		if !slicesEqual(got, tc.want) {
			t.Errorf("conjunction %v: got %v, want %v", tc.terms, got, tc.want)
		}
	}
}

// ----------------------------------------------------------------
// 2. Unadorned conjunction push-down (OptimizeTFRConjunctionUnadorned)
// ----------------------------------------------------------------

func TestOptimizeConjunctionUnadorned(t *testing.T) {
	tests := []struct {
		terms []string
		want  []string
	}{
		{[]string{"alpha", "beta"}, []string{"d1"}},
		{[]string{"alpha", "gamma"}, []string{"d2"}},
		{[]string{"beta", "gamma"}, []string{"d3"}},
		{[]string{"alpha", "delta"}, nil},
		{[]string{"alpha", "beta", "gamma"}, nil}, // d1 has alpha+beta but not gamma
	}

	idx, cleanup := buildOptimizeTestIndex(t)
	defer cleanup()

	for _, tc := range tests {
		reader, tfrs := openReaderAndTFRs(t, idx, false, tc.terms...)

		oTFR := runConjunctionUnadornedOpt(t, tfrs)
		for _, tfr := range tfrs {
			tfr.Close()
		}

		var got []string
		if oTFR != nil {
			got = collectDocIDs(t, oTFR, reader)
			oTFR.Close()
		}
		reader.Close()

		if !slicesEqual(got, tc.want) {
			t.Errorf("conjunction:unadorned %v: got %v, want %v", tc.terms, got, tc.want)
		}
	}
}

// TestOptimizeConjunctionUnadornedDisabled checks that turning off the
// global flag causes the optimization to decline (octx becomes nil).
func TestOptimizeConjunctionUnadornedDisabled(t *testing.T) {
	OptimizeConjunctionUnadorned = false
	defer func() { OptimizeConjunctionUnadorned = true }()

	idx, cleanup := buildOptimizeTestIndex(t)
	defer cleanup()
	reader, tfrs := openReaderAndTFRs(t, idx, false, "alpha", "beta")
	defer reader.Close()
	defer func() {
		for _, tfr := range tfrs {
			tfr.Close()
		}
	}()

	oTFR := runConjunctionUnadornedOpt(t, tfrs)
	if oTFR != nil {
		oTFR.Close()
		t.Error("expected optimization to be disabled, but got a result")
	}
}

// ----------------------------------------------------------------
// 3. Unadorned disjunction push-down (OptimizeTFRDisjunctionUnadorned)
// ----------------------------------------------------------------

func TestOptimizeDisjunctionUnadorned(t *testing.T) {
	tests := []struct {
		terms []string
		want  []string
	}{
		{[]string{"alpha", "beta"}, []string{"d1", "d2", "d3"}},
		{[]string{"delta", "epsilon"}, []string{"d4", "d5", "d6"}},
		{[]string{"epsilon", "zeta"}, []string{"d6", "d7"}},
	}

	idx, cleanup := buildOptimizeTestIndex(t)
	defer cleanup()

	for _, tc := range tests {
		reader, tfrs := openReaderAndTFRs(t, idx, false, tc.terms...)

		oTFR := runDisjunctionUnadornedOpt(t, tfrs)
		for _, tfr := range tfrs {
			tfr.Close()
		}

		var got []string
		if oTFR != nil {
			got = collectDocIDs(t, oTFR, reader)
			oTFR.Close()
		}
		reader.Close()

		if !slicesEqual(got, tc.want) {
			t.Errorf("disjunction:unadorned %v: got %v, want %v", tc.terms, got, tc.want)
		}
	}
}

// TestOptimizeDisjunctionUnadornedDisabled checks the disabled path.
func TestOptimizeDisjunctionUnadornedDisabled(t *testing.T) {
	OptimizeDisjunctionUnadorned = false
	defer func() { OptimizeDisjunctionUnadorned = true }()

	idx, cleanup := buildOptimizeTestIndex(t)
	defer cleanup()
	reader, tfrs := openReaderAndTFRs(t, idx, false, "alpha", "beta")
	defer reader.Close()
	defer func() {
		for _, tfr := range tfrs {
			tfr.Close()
		}
	}()

	oTFR := runDisjunctionUnadornedOpt(t, tfrs)
	if oTFR != nil {
		oTFR.Close()
		t.Error("expected optimization to be disabled, but got a result")
	}
}

// ----------------------------------------------------------------
// 4. 1-hit fast path in unadorned conjunction
// ----------------------------------------------------------------

// TestOptimize1HitConjunction verifies the DocNum1Hit path inside
// OptimizeTFRConjunctionUnadorned.Finish. "epsilon" and "zeta" each appear in
// exactly one document (no term vectors → 1-hit FST encoding).
// Conjuncting "epsilon" (only d6) and "zeta" (only d7) must yield nothing.
// Conjuncting "alpha" (d1,d2) with "epsilon" (d6) must also yield nothing.
func TestOptimize1HitConjunction(t *testing.T) {
	tests := []struct {
		terms []string
		want  []string
	}{
		{[]string{"epsilon", "zeta"}, nil},  // disjoint 1-hit terms
		{[]string{"alpha", "epsilon"}, nil}, // alpha is general, epsilon is 1-hit, no overlap
		{[]string{"alpha", "beta"}, []string{"d1"}}, // control: no 1-hit involved
	}

	idx, cleanup := buildOptimizeTestIndex(t)
	defer cleanup()

	for _, tc := range tests {
		reader, tfrs := openReaderAndTFRs(t, idx, false, tc.terms...)

		oTFR := runConjunctionUnadornedOpt(t, tfrs)
		for _, tfr := range tfrs {
			tfr.Close()
		}

		var got []string
		if oTFR != nil {
			got = collectDocIDs(t, oTFR, reader)
			oTFR.Close()
		}
		reader.Close()

		if !slicesEqual(got, tc.want) {
			t.Errorf("1-hit conjunction %v: got %v, want %v", tc.terms, got, tc.want)
		}
	}
}

// TestOptimize1HitConjunctionMatch verifies the case where two 1-hit terms
// appear in the SAME document. We index a new doc with both epsilon and zeta.
func TestOptimize1HitConjunctionMatch(t *testing.T) {
	idx, cleanup := buildOptimizeTestIndex(t)
	defer cleanup()

	// Add a doc where both epsilon and zeta appear (each only once total still
	// after this batch, though now epsilon appears in d6 AND d8 → no longer 1-hit.
	// Use unique terms "kappa" and "lambda" instead.)
	batch := index.NewBatch()
	doc := document.NewDocument("d8")
	doc.AddField(document.NewTextFieldCustom("f", nil, []byte("kappa lambda"),
		index.IndexField, testAnalyzer))
	batch.Update(doc)
	if err := idx.Batch(batch); err != nil {
		t.Fatal(err)
	}

	reader, tfrs := openReaderAndTFRs(t, idx, false, "kappa", "lambda")
	defer reader.Close()
	defer func() {
		for _, tfr := range tfrs {
			tfr.Close()
		}
	}()

	oTFR := runConjunctionUnadornedOpt(t, tfrs)
	if oTFR == nil {
		t.Skip("optimization declined")
	}
	defer oTFR.Close()

	got := collectDocIDs(t, oTFR, reader)
	if !slicesEqual(got, []string{"d8"}) {
		t.Errorf("matching 1-hit conjunction: got %v, want [d8]", got)
	}
}

// ----------------------------------------------------------------
// 5. ShardView on unadorned TFR (the nil-postings fix)
// ----------------------------------------------------------------

// TestShardViewUnadornedTFRNoPanic is the regression test for the §7 nil-postings
// panic. Before the fix, ShardView panicked on any unadorned TFR because
// i.postings[startSeg:endSeg] on a nil slice panics in Go for endSeg > 0.
func TestShardViewUnadornedTFRNoPanic(t *testing.T) {
	idx, cleanup := buildOptimizeTestIndex(t)
	defer cleanup()

	reader, tfrs := openReaderAndTFRs(t, idx, false, "alpha", "beta")
	defer reader.Close()
	defer func() {
		for _, tfr := range tfrs {
			tfr.Close()
		}
	}()

	oTFR := runConjunctionUnadornedOpt(t, tfrs)
	if oTFR == nil {
		t.Skip("optimization declined")
	}
	defer oTFR.Close()

	isTFR, ok := oTFR.(*IndexSnapshotTermFieldReader)
	if !ok {
		t.Skip("result not *IndexSnapshotTermFieldReader")
	}
	if isTFR.postings != nil {
		t.Fatal("expected nil postings on unadorned TFR — test precondition failed")
	}

	numSegs := len(isTFR.iterators)
	if numSegs == 0 {
		t.Skip("no segments")
	}

	// Full-span shard [0, numSegs) must not panic and must return correct docs.
	shardFull, err := isTFR.ShardView(0, numSegs)
	if err != nil {
		t.Fatalf("ShardView(0,%d): %v", numSegs, err)
	}
	defer shardFull.Close()
	got := collectDocIDs(t, shardFull, reader)
	if !slicesEqual(got, []string{"d1"}) {
		t.Errorf("ShardView full span: got %v, want [d1]", got)
	}
}

// TestShardViewUnadornedTFREmpty verifies the empty shard [0,0) case.
func TestShardViewUnadornedTFREmpty(t *testing.T) {
	idx, cleanup := buildOptimizeTestIndex(t)
	defer cleanup()

	reader, tfrs := openReaderAndTFRs(t, idx, false, "alpha", "beta")
	defer reader.Close()
	defer func() {
		for _, tfr := range tfrs {
			tfr.Close()
		}
	}()

	oTFR := runConjunctionUnadornedOpt(t, tfrs)
	if oTFR == nil {
		t.Skip("optimization declined")
	}

	isTFR := oTFR.(*IndexSnapshotTermFieldReader)
	defer isTFR.Close()

	empty, err := isTFR.ShardView(0, 0)
	if err != nil {
		t.Fatalf("ShardView(0,0): %v", err)
	}
	defer empty.Close()
	got := collectDocIDs(t, empty, reader)
	if len(got) != 0 {
		t.Errorf("empty shard: got %v, want []", got)
	}
}

// TestShardViewUnadornedTFRPartialShards verifies that multiple non-overlapping
// partial shards, when combined, return the same total document set as a
// full-span ShardView.
func TestShardViewUnadornedTFRPartialShards(t *testing.T) {
	idx, cleanup := buildOptimizeTestIndex(t)
	defer cleanup()

	reader, tfrs := openReaderAndTFRs(t, idx, false, "alpha", "beta")
	defer reader.Close()
	defer func() {
		for _, tfr := range tfrs {
			tfr.Close()
		}
	}()

	oTFR := runConjunctionUnadornedOpt(t, tfrs)
	if oTFR == nil {
		t.Skip("optimization declined")
	}

	isTFR := oTFR.(*IndexSnapshotTermFieldReader)
	defer isTFR.Close()

	numSegs := len(isTFR.iterators)
	if numSegs < 2 {
		t.Skip("need at least 2 segments for partial-shard test")
	}

	// Split into two non-overlapping shards; union their doc sets.
	mid := numSegs / 2
	shard1, err := isTFR.ShardView(0, mid)
	if err != nil {
		t.Fatalf("ShardView(0,%d): %v", mid, err)
	}
	defer shard1.Close()
	shard2, err := isTFR.ShardView(mid, numSegs)
	if err != nil {
		t.Fatalf("ShardView(%d,%d): %v", mid, numSegs, err)
	}
	defer shard2.Close()

	got1 := collectDocIDs(t, shard1, reader)
	got2 := collectDocIDs(t, shard2, reader)
	combined := append(got1, got2...)
	sort.Strings(combined)

	// Full span for comparison.
	full, err := isTFR.ShardView(0, numSegs)
	if err != nil {
		t.Fatalf("ShardView(0,%d): %v", numSegs, err)
	}
	defer full.Close()
	wantFull := collectDocIDs(t, full, reader)

	if !slicesEqual(combined, wantFull) {
		t.Errorf("partial shards combined: %v, full span: %v", combined, wantFull)
	}
}

// ----------------------------------------------------------------
// 6. freshIteratorForShard unit tests
// ----------------------------------------------------------------

func TestFreshIteratorForShardBitmap(t *testing.T) {
	bm := roaring.New()
	bm.AddMany([]uint32{1, 3, 5})
	src := newUnadornedPostingsIteratorFromBitmap(bm)

	// Partially consume the source so its position is advanced.
	src.Next()
	src.Next()

	// Fresh iterator must restart from docNum 1.
	fresh := freshIteratorForShard(src)
	var got []uint64
	for {
		p, err := fresh.Next()
		if err != nil || p == nil {
			break
		}
		got = append(got, p.Number())
	}
	want := []uint64{1, 3, 5}
	if !uint64SlicesEqual(got, want) {
		t.Errorf("fresh bitmap: got %v, want %v", got, want)
	}

	// Source iterator position must be independent of the fresh iterator.
	p, _ := src.Next()
	if p == nil || p.Number() != 5 {
		t.Errorf("source not independent: remaining Next() = %v", p)
	}
}

func TestFreshIteratorForShard1Hit(t *testing.T) {
	src := newUnadornedPostingsIteratorFrom1Hit(42)

	// Consume the single hit from the source.
	src.Next()
	exhausted, _ := src.Next()
	if exhausted != nil {
		t.Fatal("source should be exhausted after one Next()")
	}

	// Fresh iterator must return the 1-hit doc.
	fresh := freshIteratorForShard(src)
	p, err := fresh.Next()
	if err != nil || p == nil || p.Number() != 42 {
		t.Errorf("fresh 1-hit: got %v err=%v, want docNum=42", p, err)
	}
	// Should be exhausted after one hit.
	p2, _ := fresh.Next()
	if p2 != nil {
		t.Errorf("fresh 1-hit: expected exhausted, got %v", p2)
	}
}

func TestFreshIteratorForShardNil(t *testing.T) {
	fresh := freshIteratorForShard(nil)
	p, err := fresh.Next()
	if err != nil || p != nil {
		t.Errorf("nil src: expected empty, got p=%v err=%v", p, err)
	}
}

func TestFreshIteratorForShardEmptyIterator(t *testing.T) {
	fresh := freshIteratorForShard(anEmptyPostingsIterator)
	p, err := fresh.Next()
	if err != nil || p != nil {
		t.Errorf("empty src: expected empty, got p=%v err=%v", p, err)
	}
}

func TestFreshIteratorForShardEmptyBitmap(t *testing.T) {
	// unadornedPostingsIteratorBitmap with nil actualBM → should return empty.
	src := &unadornedPostingsIteratorBitmap{actualBM: nil, actual: nil}
	fresh := freshIteratorForShard(src)
	p, err := fresh.Next()
	if err != nil || p != nil {
		t.Errorf("nil-bitmap src: expected empty, got p=%v err=%v", p, err)
	}
}

// ----------------------------------------------------------------
// helpers
// ----------------------------------------------------------------

func slicesEqual(a, b []string) bool {
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

func uint64SlicesEqual(a, b []uint64) bool {
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
