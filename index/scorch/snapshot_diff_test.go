//  Copyright (c) 2024 Couchbase, Inc.
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

package scorch

import (
	"encoding/json"
	"os"
	"sort"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
	bolt "go.etcd.io/bbolt"
)

func assertStringSlicesEqual(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: length mismatch got=%v want=%v", label, got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: at index %d got=%q want=%q (full: got=%v want=%v)",
				label, i, got[i], want[i], got, want)
		}
	}
}

func assertDiff(t *testing.T, diff *SnapshotDiff, epoch uint64,
	live, deleted, updated, inserted []string,
) {
	t.Helper()
	if diff.Epoch != epoch {
		t.Fatalf("epoch: got=%d want=%d", diff.Epoch, epoch)
	}
	assertStringSlicesEqual(t, "live", diff.Live, live)
	assertStringSlicesEqual(t, "deleted", diff.Deleted, deleted)
	assertStringSlicesEqual(t, "updated", diff.Updated, updated)
	assertStringSlicesEqual(t, "inserted", diff.Inserted, inserted)
}

// ---- buildDiffFromBatch tests ----

func TestBuildDiffFromBatch_FirstSnapshot(t *testing.T) {
	diff := buildDiffFromBatch(nil, 0, []string{"a", "b"}, nil, nil)
	assertDiff(t, diff, 0,
		[]string{"a", "b"}, // live
		nil,                // deleted
		nil,                // updated
		[]string{"a", "b"}, // inserted
	)
}

func TestBuildDiffFromBatch_InsertsOnly(t *testing.T) {
	diff := buildDiffFromBatch([]string{"a", "b"}, 1,
		[]string{"c", "d"}, nil, nil)
	assertDiff(t, diff, 1,
		[]string{"a", "b", "c", "d"}, // live: old + inserted
		nil, // deleted
		nil, // updated
		[]string{"c", "d"}, // inserted
	)
}

func TestBuildDiffFromBatch_DeletesOnly(t *testing.T) {
	diff := buildDiffFromBatch([]string{"a", "b", "c"}, 2,
		nil, nil, []string{"b"})
	assertDiff(t, diff, 2,
		[]string{"a", "c"}, // live: old - deleted
		[]string{"b"},      // deleted
		nil,                // updated
		nil,                // inserted
	)
}

func TestBuildDiffFromBatch_Mixed(t *testing.T) {
	// old: a,b,c,d ; insert e ; update b ; delete c
	diff := buildDiffFromBatch([]string{"a", "b", "c", "d"}, 3,
		[]string{"e"}, []string{"b"}, []string{"c"})
	assertDiff(t, diff, 3,
		[]string{"a", "b", "d", "e"}, // live: old - deleted + inserted
		[]string{"c"},                // deleted
		[]string{"b"},                // updated
		[]string{"e"},                // inserted
	)
}

func TestBuildDiffFromBatch_AllDeleted(t *testing.T) {
	diff := buildDiffFromBatch([]string{"a", "b"}, 4,
		nil, nil, []string{"a", "b"})
	assertDiff(t, diff, 4,
		nil,                // live: empty
		[]string{"a", "b"}, // deleted
		nil,                // updated
		nil,                // inserted
	)
}

func TestBuildDiffFromBatch_SortedOutput(t *testing.T) {
	diff := buildDiffFromBatch([]string{"z", "a", "m"}, 5,
		[]string{"b"}, []string{"z"}, []string{"a"})
	assertDiff(t, diff, 5,
		[]string{"b", "m", "z"}, // live: sorted
		[]string{"a"},           // deleted: sorted
		[]string{"z"},           // updated: sorted
		[]string{"b"},           // inserted: sorted
	)
}

// ---- classifyBatchIDs tests ----

// makeSegment creates a real in-memory segment with the given docIDs.
func makeSegment(t *testing.T, docIDs []string) segment.Segment {
	t.Helper()
	cfg := CreateConfig("TestSnapshotDiff")
	_ = os.RemoveAll(cfg["path"].(string))
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	docs := make([]index.Document, 0, len(docIDs))
	for _, id := range docIDs {
		doc := document.NewDocument(id)
		doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
		doc.AddIDField()
		doc.VisitFields(func(field index.Field) {
			field.Analyze()
		})
		docs = append(docs, doc)
	}
	seg, _, err := idx.(*Scorch).segPlugin.New(docs)
	if err != nil {
		idx.Close()
		t.Fatal(err)
	}
	idx.Close()
	_ = os.RemoveAll(cfg["path"].(string))
	return seg
}

func TestClassifyBatchIDs_PureDeletes(t *testing.T) {
	inserted, updated, deleted := classifyBatchIDs(
		[]string{"a", "b"}, []string{"a", "b", "c"}, nil)
	assertStringSlicesEqual(t, "inserted", inserted, nil)
	assertStringSlicesEqual(t, "updated", updated, nil)
	assertStringSlicesEqual(t, "deleted", deleted, []string{"a", "b"})
}

func TestClassifyBatchIDs_AllInserts(t *testing.T) {
	seg := makeSegment(t, []string{"x", "y"})
	defer seg.Close()

	inserted, updated, deleted := classifyBatchIDs(
		[]string{"x", "y"}, []string{"a", "b"}, seg)
	assertStringSlicesEqual(t, "inserted", inserted, []string{"x", "y"})
	assertStringSlicesEqual(t, "updated", updated, nil)
	assertStringSlicesEqual(t, "deleted", deleted, nil)
}

func TestClassifyBatchIDs_AllUpdates(t *testing.T) {
	seg := makeSegment(t, []string{"a", "b"})
	defer seg.Close()

	inserted, updated, deleted := classifyBatchIDs(
		[]string{"a", "b"}, []string{"a", "b", "c"}, seg)
	assertStringSlicesEqual(t, "inserted", inserted, nil)
	assertStringSlicesEqual(t, "updated", updated, []string{"a", "b"})
	assertStringSlicesEqual(t, "deleted", deleted, nil)
}

func TestClassifyBatchIDs_MixedAllThree(t *testing.T) {
	seg := makeSegment(t, []string{"a", "c"})
	defer seg.Close()

	// batch: a, c, d  ; old: a, b
	// a -> update (in old, in new)
	// c -> insert (not in old, in new)
	// d -> ????? d is in the ids list but NOT in the segment... would be delete since not in new segment
	inserted, updated, deleted := classifyBatchIDs(
		[]string{"a", "c", "d"}, []string{"a", "b"}, seg)
	assertStringSlicesEqual(t, "inserted", inserted, []string{"c"})
	assertStringSlicesEqual(t, "updated", updated, []string{"a"})
	assertStringSlicesEqual(t, "deleted", deleted, []string{"d"})
}

func TestClassifyBatchIDs_InsertAndDelete(t *testing.T) {
	seg := makeSegment(t, []string{"c"})
	defer seg.Close()

	// batch: b, c ; old: a, b
	// b -> NOT in new segment (DocNumbers returns empty) but IS in old live -> deleted (default case: not inNew)
	// c -> insert (not in old, in new)
	inserted, updated, deleted := classifyBatchIDs(
		[]string{"b", "c"}, []string{"a", "b"}, seg)
	assertStringSlicesEqual(t, "inserted", inserted, []string{"c"})
	assertStringSlicesEqual(t, "updated", updated, nil)
	assertStringSlicesEqual(t, "deleted", deleted, []string{"b"})
}

func TestClassifyBatchIDs_EmptyOldLive(t *testing.T) {
	seg := makeSegment(t, []string{"a"})
	defer seg.Close()

	inserted, updated, deleted := classifyBatchIDs(
		[]string{"a", "b"}, nil, seg)
	assertStringSlicesEqual(t, "inserted", inserted, []string{"a"})
	assertStringSlicesEqual(t, "updated", updated, nil)
	// b is in ids but not in new segment -> delete
	assertStringSlicesEqual(t, "deleted", deleted, []string{"b"})
}

func TestClassifyBatchIDs_EmptyBatch(t *testing.T) {
	seg := makeSegment(t, []string{"a"})
	defer seg.Close()

	inserted, updated, deleted := classifyBatchIDs(
		nil, []string{"a", "b"}, seg)
	if len(inserted) != 0 || len(updated) != 0 || len(deleted) != 0 {
		t.Fatalf("expected all empty, got i=%v u=%v d=%v", inserted, updated, deleted)
	}
}

// ---- computeSnapshotDiff integration tests (uses real Scorch) ----

func TestComputeSnapshotDiff_NilPrev(t *testing.T) {
	seg := makeSegment(t, []string{"a", "b"})
	defer seg.Close()

	snap := &IndexSnapshot{
		segment: []*SegmentSnapshot{
			{id: 1, segment: seg, cachedDocs: &cachedDocs{cache: nil}},
		},
		offsets:  []uint64{0},
		internal: map[string][]byte{},
		epoch:    42,
		refs:     1,
	}

	diff, err := computeSnapshotDiff(nil, snap)
	if err != nil {
		t.Fatal(err)
	}
	assertDiff(t, diff, 42,
		[]string{"a", "b"}, // live
		nil,                // deleted
		nil,                // updated
		[]string{"a", "b"}, // inserted = all live
	)
}

func TestComputeSnapshotDiff_DocAdded(t *testing.T) {
	oldSeg := makeSegment(t, []string{"a"})
	defer oldSeg.Close()
	newSeg := makeSegment(t, []string{"a", "b"})
	defer newSeg.Close()

	oldSnap := &IndexSnapshot{
		segment:  []*SegmentSnapshot{{id: 1, segment: oldSeg, cachedDocs: &cachedDocs{cache: nil}}},
		offsets:  []uint64{0},
		internal: map[string][]byte{},
		epoch:    1,
		refs:     1,
	}
	newSnap := &IndexSnapshot{
		segment:  []*SegmentSnapshot{{id: 2, segment: newSeg, cachedDocs: &cachedDocs{cache: nil}}},
		offsets:  []uint64{0},
		internal: map[string][]byte{},
		epoch:    2,
		refs:     1,
	}

	diff, err := computeSnapshotDiff(oldSnap, newSnap)
	if err != nil {
		t.Fatal(err)
	}
	assertDiff(t, diff, 2,
		[]string{"a", "b"}, // live
		nil,                // deleted
		nil,                // updated
		[]string{"b"},      // inserted
	)
}

func TestComputeSnapshotDiff_DocDeleted(t *testing.T) {
	oldSeg := makeSegment(t, []string{"a", "b"})
	defer oldSeg.Close()
	newSeg := makeSegment(t, []string{"b"})
	defer newSeg.Close()

	oldSnap := &IndexSnapshot{
		segment:  []*SegmentSnapshot{{id: 1, segment: oldSeg, cachedDocs: &cachedDocs{cache: nil}}},
		offsets:  []uint64{0},
		internal: map[string][]byte{},
		epoch:    1,
		refs:     1,
	}
	newSnap := &IndexSnapshot{
		segment:  []*SegmentSnapshot{{id: 2, segment: newSeg, cachedDocs: &cachedDocs{cache: nil}}},
		offsets:  []uint64{0},
		internal: map[string][]byte{},
		epoch:    2,
		refs:     1,
	}

	diff, err := computeSnapshotDiff(oldSnap, newSnap)
	if err != nil {
		t.Fatal(err)
	}
	assertDiff(t, diff, 2,
		[]string{"b"},   // live
		[]string{"a"},   // deleted
		nil,             // updated
		nil,             // inserted
	)
}

// ---- Persistence tests (bolt round-trip) ----

// writeDiffSync writes a SnapshotDiff to bolt synchronously (test helper).
func writeDiffSync(t *testing.T, s *Scorch, diff *SnapshotDiff) {
	t.Helper()
	s.diffLock.Lock()
	s.pendingDiffs = append(s.pendingDiffs, diff)
	s.diffLock.Unlock()

	err := s.rootBolt.Update(func(tx *bolt.Tx) error {
		return s.flushPendingDiffs(tx)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPersistAndGetSnapshotDiff(t *testing.T) {
	cfg := CreateConfig("TestPersistAndGetSnapshotDiff")
	_ = os.RemoveAll(cfg["path"].(string))
	defer os.RemoveAll(cfg["path"].(string))

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	s := idx.(*Scorch)

	diff := &SnapshotDiff{
		Epoch:    7,
		Live:     []string{"a", "b", "c"},
		Deleted:  []string{"d"},
		Updated:  []string{"b"},
		Inserted: []string{"c"},
	}
	writeDiffSync(t, s, diff)

	got, err := s.GetSnapshotDiff(7)
	if err != nil {
		t.Fatal(err)
	}
	assertDiff(t, got, 7,
		[]string{"a", "b", "c"},
		[]string{"d"},
		[]string{"b"},
		[]string{"c"},
	)
}

func TestGetAllSnapshotDiffs(t *testing.T) {
	cfg := CreateConfig("TestGetAllSnapshotDiffs")
	_ = os.RemoveAll(cfg["path"].(string))
	defer os.RemoveAll(cfg["path"].(string))

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	s := idx.(*Scorch)

	writeDiffSync(t, s, &SnapshotDiff{Epoch: 1, Live: []string{"a"}, Inserted: []string{"a"}})
	writeDiffSync(t, s, &SnapshotDiff{Epoch: 2, Live: []string{"a", "b"}, Inserted: []string{"b"}})
	writeDiffSync(t, s, &SnapshotDiff{Epoch: 3, Live: []string{"b"}, Deleted: []string{"a"}})

	diffs, err := s.GetAllSnapshotDiffs()
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 3 {
		t.Fatalf("expected 3 diffs, got %d", len(diffs))
	}
	assertStringSlicesEqual(t, "diff[0].live", diffs[0].Live, []string{"a"})
	assertStringSlicesEqual(t, "diff[1].live", diffs[1].Live, []string{"a", "b"})
	assertStringSlicesEqual(t, "diff[2].live", diffs[2].Live, []string{"b"})
}

// ---- Full integration: index, update, delete via Batch + verify diffs ----

func TestSnapshotDiffFullLifecycle(t *testing.T) {
	cfg := CreateConfig("TestSnapshotDiffFullLifecycle")
	_ = os.RemoveAll(cfg["path"].(string))
	defer os.RemoveAll(cfg["path"].(string))

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatal(err)
	}

	// --- snapshot 1: insert d1,d2,d3,d4,d5 ---
	batch := index.NewBatch()
	for _, id := range []string{"d1", "d2", "d3", "d4", "d5"} {
		doc := document.NewDocument(id)
		doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
		batch.Update(doc)
	}
	err = idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	// --- snapshot 2: update d2, delete d1, insert d6 ---
	batch = index.NewBatch()
	doc := document.NewDocument("d2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("updated")))
	batch.Update(doc)
	batch.Delete("d1")
	doc6 := document.NewDocument("d6")
	doc6.AddField(document.NewTextField("name", []uint64{}, []byte("new")))
	batch.Update(doc6)
	err = idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	// Close to drain async goroutines (including persistDiff), then reopen
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err = NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	s := idx.(*Scorch)
	diffs, err := s.GetAllSnapshotDiffs()
	if err != nil {
		t.Fatal(err)
	}

	// Dump diffs for debug
	for _, d := range diffs {
		data, _ := json.Marshal(d)
		t.Logf("epoch=%d %s", d.Epoch, string(data))
	}

	if len(diffs) < 2 {
		t.Fatalf("expected at least 2 diffs, got %d", len(diffs))
	}

	// Find the first batch diff (all inserts) and second (mixed)
	var firstDiff, secondDiff *SnapshotDiff
	for _, d := range diffs {
		if len(d.Inserted) >= 5 {
			firstDiff = d
		}
		if len(d.Deleted) > 0 || len(d.Updated) > 0 {
			secondDiff = d
		}
	}

	if firstDiff == nil {
		t.Fatal("could not find first batch diff with 5 inserts")
	}

	// Verify first batch: d1..d5 are live and inserted
	for _, id := range []string{"d1", "d2", "d3", "d4", "d5"} {
		foundLive := false
		foundIns := false
		for _, live := range firstDiff.Live {
			if live == id {
				foundLive = true
			}
		}
		for _, ins := range firstDiff.Inserted {
			if ins == id {
				foundIns = true
			}
		}
		if !foundLive {
			t.Errorf("first diff: expected %s in live", id)
		}
		if !foundIns {
			t.Errorf("first diff: expected %s in inserted", id)
		}
	}

	if secondDiff == nil {
		t.Fatal("could not find second batch diff with delete/update")
	}

	// Verify second batch: d1 deleted, d2 updated, d6 inserted
	if secondDiff.Deleted != nil {
		has := false
		for _, id := range secondDiff.Deleted {
			if id == "d1" {
				has = true
			}
		}
		if !has {
			t.Errorf("second diff: expected d1 in deleted, got %v", secondDiff.Deleted)
		}
	}

	if secondDiff.Updated != nil {
		has := false
		for _, id := range secondDiff.Updated {
			if id == "d2" {
				has = true
			}
		}
		if !has {
			t.Errorf("second diff: expected d2 in updated, got %v", secondDiff.Updated)
		}
	}

	if secondDiff.Inserted != nil {
		has := false
		for _, id := range secondDiff.Inserted {
			if id == "d6" {
				has = true
			}
		}
		if !has {
			t.Errorf("second diff: expected d6 in inserted, got %v", secondDiff.Inserted)
		}
	}

	// live set for last diff: d2,d3,d4,d5,d6
	sort.Strings(secondDiff.Live)
	assertStringSlicesEqual(t, "final live", secondDiff.Live,
		[]string{"d2", "d3", "d4", "d5", "d6"})
}

// ---- Bolt persistence edge cases ----

func TestGetSnapshotDiff_NotFound(t *testing.T) {
	cfg := CreateConfig("TestGetSnapshotDiff_NotFound")
	_ = os.RemoveAll(cfg["path"].(string))
	defer os.RemoveAll(cfg["path"].(string))

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	s := idx.(*Scorch)
	_, err = s.GetSnapshotDiff(99999)
	if err == nil {
		t.Fatal("expected error for non-existent diff")
	}
}

func TestGetAllSnapshotDiffs_Empty(t *testing.T) {
	cfg := CreateConfig("TestGetAllSnapshotDiffs_Empty")
	_ = os.RemoveAll(cfg["path"].(string))
	defer os.RemoveAll(cfg["path"].(string))

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	s := idx.(*Scorch)
	diffs, err := s.GetAllSnapshotDiffs()
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 0 {
		t.Fatalf("expected 0 diffs, got %d", len(diffs))
	}
}

func TestSnapshotDiffJSONRoundTrip(t *testing.T) {
	cfg := CreateConfig("TestSnapshotDiffJSONRoundTrip")
	_ = os.RemoveAll(cfg["path"].(string))
	defer os.RemoveAll(cfg["path"].(string))

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	s := idx.(*Scorch)

	// Use nil slices to test JSON marshalling of empty vs nil
	diff := &SnapshotDiff{
		Epoch:    10,
		Live:     []string{},
		Deleted:  nil,
		Updated:  []string{},
		Inserted: nil,
	}
	writeDiffSync(t, s, diff)

	got, err := s.GetSnapshotDiff(10)
	if err != nil {
		t.Fatal(err)
	}
	if got.Epoch != 10 {
		t.Fatalf("epoch mismatch: %d", got.Epoch)
	}
}

func TestGetAllSnapshotDiffs_BoltDirect(t *testing.T) {
	cfg := CreateConfig("TestGetAllSnapshotDiffs_BoltDirect")
	_ = os.RemoveAll(cfg["path"].(string))
	defer os.RemoveAll(cfg["path"].(string))

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer idx.Close()

	s := idx.(*Scorch)

	// Write some diffs directly to bolt
	s.rootBolt.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists(boltSnapshotDiffBucket)
		for epoch := uint64(1); epoch <= 5; epoch++ {
			d := &SnapshotDiff{
				Epoch:    epoch,
				Live:     []string{"a"},
				Inserted: []string{"a"},
			}
			data, _ := json.Marshal(d)
			key := encodeUvarintAscending(nil, epoch)
			b.Put(key, data)
		}
		return nil
	})

	diffs, err := s.GetAllSnapshotDiffs()
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 5 {
		t.Fatalf("expected 5 diffs, got %d", len(diffs))
	}
	for i, d := range diffs {
		if d.Epoch != uint64(i+1) {
			t.Errorf("diff[%d] epoch: got=%d want=%d", i, d.Epoch, i+1)
		}
	}
}

// ---- collectLiveDocIDs tests ----

func TestCollectLiveDocIDs_NilSnapshot(t *testing.T) {
	result, err := collectLiveDocIDs(nil)
	if err != nil {
		t.Fatal(err)
	}
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestCollectLiveDocIDs_EmptySnapshot(t *testing.T) {
	snap := &IndexSnapshot{
		segment:  []*SegmentSnapshot{},
		internal: map[string][]byte{},
		refs:     1,
	}
	result, err := collectLiveDocIDs(snap)
	if err != nil {
		t.Fatal(err)
	}
	if len(result) != 0 {
		t.Fatalf("expected empty, got %v", result)
	}
}

func TestCollectLiveDocIDs_MultipleSegments(t *testing.T) {
	seg1 := makeSegment(t, []string{"a", "b"})
	defer seg1.Close()
	seg2 := makeSegment(t, []string{"c"})
	defer seg2.Close()
	seg3 := makeSegment(t, []string{"d", "a"}) // "a" appears in both seg1 and seg3
	defer seg3.Close()

	snap := &IndexSnapshot{
		segment: []*SegmentSnapshot{
			{id: 1, segment: seg1, cachedDocs: &cachedDocs{cache: nil}},
			{id: 2, segment: seg2, cachedDocs: &cachedDocs{cache: nil}},
			{id: 3, segment: seg3, cachedDocs: &cachedDocs{cache: nil}},
		},
		offsets:  []uint64{0, 2, 3},
		internal: map[string][]byte{},
		refs:     1,
	}

	result, err := collectLiveDocIDs(snap)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(result)
	assertStringSlicesEqual(t, "collectLiveDocIDs", result, []string{"a", "b", "c", "d"})
}

// ---- Edge case: GetSnapshotDiff with nil rootBolt ----

func TestGetSnapshotDiff_NilRootBolt(t *testing.T) {
	s := &Scorch{rootBolt: nil}
	_, err := s.GetSnapshotDiff(1)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetAllSnapshotDiffs_NilRootBolt(t *testing.T) {
	s := &Scorch{rootBolt: nil}
	_, err := s.GetAllSnapshotDiffs()
	if err == nil {
		t.Fatal("expected error")
	}
}
