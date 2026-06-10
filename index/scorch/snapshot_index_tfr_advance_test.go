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

// TestAdvanceBackwardSeekNoRaceWithRecycling verifies that the backward-seek
// path in Advance() is safe when TFR recycling is enabled.
//
// Root cause (MB-64604): the original code called i.Close() before *i = *i2.
// Close() with a non-zero fieldTFRCacheThreshold donates i to the recycle pool
// while the caller's pointer to i is still live. Another goroutine can
// immediately retrieve i from the pool and begin writing to its fields;
// simultaneously the Advance() path writes *i = *i2. Two goroutines share the
// same *IndexSnapshotTermFieldReader with no synchronisation — a data race that
// manifests as nil-pointer dereferences, divide-by-zero errors, and
// index-out-of-range panics in the posting list and chunk decoder (MB-64604).
//
// The fix: skip recycleTermFieldReader(i) inside Advance(). Report i's IO stats
// and TotTermSearchersFinished inline (replicating the non-recycle parts of
// Close()), then overwrite i in-place from i2. The caller's pointer stays valid
// throughout; i is never donated to the pool while in use.
//
// Run with -race to confirm no data race is reported.

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	index "github.com/blevesearch/bleve_index_api"
)

func TestAdvanceBackwardSeekNoRaceWithRecycling(t *testing.T) {
	cfg := CreateConfig("TestAdvanceBackwardSeekNoRaceWithRecycling")
	// High threshold reproduces MB-64604: the pool returns i almost immediately
	// after i.Close(), so the next TermFieldReader call in another goroutine gets
	// the same pointer. With the old code the race detector fires here.
	cfg["fieldTFRCacheThreshold"] = 100
	if err := InitTest(cfg); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := DestroyTest(cfg); err != nil {
			t.Log(err)
		}
	}()

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	if err = idx.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = idx.Close() }()

	// Index enough documents so that the term appears at multiple docIDs,
	// giving Advance() non-trivial work after a backward seek.
	const numDocs = 50
	for i := 0; i < numDocs; i++ {
		doc := document.NewDocument(fmt.Sprintf("%d", i))
		doc.AddField(document.NewTextFieldWithAnalyzer("body", []uint64{},
			[]byte("hotel lisbon"), testAnalyzer))
		if err := idx.Update(doc); err != nil {
			t.Fatal(err)
		}
	}

	// Get a snapshot to search against.
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()

	is, ok := reader.(*IndexSnapshot)
	if !ok {
		t.Skip("reader is not an *IndexSnapshot")
	}

	// Concurrently run goroutines that each trigger the backward-seek path.
	// The race: goroutine A calls Advance(firstHit) — currID == firstHit so
	// Compare returns 0 (>= 0) — triggering the restart. With the old code,
	// i.Close() puts i in the pool; goroutine B immediately calls
	// TermFieldReader and gets i back from the pool; then *i = *i2 overwrites
	// the struct that goroutine B is already writing — detected by -race.
	const goroutines = 8
	const iters = 200

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < iters; n++ {
				tfr, err := is.TermFieldReader(context.Background(),
					[]byte("hotel"), "body", true, true, false)
				if err != nil {
					t.Errorf("TermFieldReader: %v", err)
					return
				}

				// Advance to the first hit.
				first, err := tfr.Next(nil)
				if err != nil {
					_ = tfr.Close()
					t.Errorf("Next: %v", err)
					return
				}
				if first == nil {
					_ = tfr.Close()
					continue // no hits — nothing to test
				}
				firstID := make(index.IndexInternalID, len(first.ID))
				copy(firstID, first.ID)

				// Advance to the same ID: currID.Compare(firstID) == 0 >= 0,
				// so the backward-seek branch fires and triggers the pool race.
				_, err = tfr.Advance(firstID, nil)
				if err != nil {
					_ = tfr.Close()
					t.Errorf("Advance: %v", err)
					return
				}

				_ = tfr.Close()
			}
		}()
	}
	wg.Wait()
}
