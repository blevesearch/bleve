//  Copyright (c) 2020 Couchbase, Inc.
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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/blevesearch/bleve/v2/document"
	index "github.com/blevesearch/bleve_index_api"
)

// TestDeleteDuringFileMergeIntroduction is a variant of
// TestObsoleteSegmentMergeIntroduction that uses nested documents and
// verifies that after a close+reopen, the live document count is correct.
//
// This test targets the bug in introduceMerge / persistSnapshotMaybeMerge where
// concurrent deletions that arrive during a file-segment merge introduction are
// correctly tracked in newSegmentDeleted[i] (via the deletedSince logic in
// introduceMerge), but then the persisted BoltDB snapshot drops those deletions
// because persistSnapshotMaybeMerge builds its equiv SegmentSnapshot with
// TestDeleteDuringMemMergeIntroduction reproduces the bug in Scorch where
// concurrent deletions (including cascaded nested deletions) are lost
// when merging in-memory segments.
// The bug is in persister.go where persistSnapshotMaybeMerge constructs an
// equivalent snapshot but sets deleted: nil, losing any concurrent deletions
// picked up by introduceMerge.
func TestDeleteDuringMemMergeIntroduction(t *testing.T) {
	testConfig := CreateConfig("TestDeleteDuringMemMergeIntroduction")
	err := InitTest(testConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := DestroyTest(testConfig)
		if err != nil {
			t.Fatal(err)
		}
	}()

	persisterReachedPause := make(chan struct{}, 1)
	mergeIntroStart := make(chan struct{}, 1)
	mergeIntroComplete := make(chan struct{}, 1)
	batchIntroComplete := make(chan struct{}, 1)
	var pausePersister atomic.Value
	pausePersister.Store(make(chan struct{}))

	var idx index.Index
	var signalReachedPause, signalIntroStart, signalIntroComplete sync.Once
	RegistryEventCallbacks["testMemNested"] = func(e Event) bool {
		if name, ok := e.Scorch.config["eventCallbackName"].(string); !ok || name != "testMemNested" {
			return true
		}
		switch e.Kind {
		case EventKindPurgerCheck:
			signalReachedPause.Do(func() {
				persisterReachedPause <- struct{}{}
			})
			<-pausePersister.Load().(chan struct{})
		case EventKindBatchMemoryApplied:
			select {
			case batchIntroComplete <- struct{}{}:
			default:
			}
		case EventKindMemMergeIntroductionStart:
			signalIntroStart.Do(func() {
				mergeIntroStart <- struct{}{}
			})
			<-pausePersister.Load().(chan struct{})
		case EventKindMemMergeIntroductionComplete:
			signalIntroComplete.Do(func() {
				mergeIntroComplete <- struct{}{}
			})
		}
		return true
	}

	ourConfig := make(map[string]interface{}, len(testConfig))
	for k, v := range testConfig {
		ourConfig[k] = v
	}
	ourConfig["eventCallbackName"] = "testMemNested"

	// Ensure in-memory merge is triggered for 2 segments
	originalMinSegments := DefaultMinSegmentsForInMemoryMerge
	originalNapTime := DefaultPersisterNapTimeMSec
	DefaultMinSegmentsForInMemoryMerge = 2
	// Increase nap time to avoid tight loop and give us time to pause
	DefaultPersisterNapTimeMSec = 100
	defer func() {
		DefaultMinSegmentsForInMemoryMerge = originalMinSegments
		DefaultPersisterNapTimeMSec = originalNapTime
		delete(RegistryEventCallbacks, "testMemNested")
	}()

	analysisQueue := index.NewAnalysisQueue(1)
	defer analysisQueue.Close()

	idx, err = NewScorch(Name, ourConfig, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Open()
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}

	<-persisterReachedPause

	// 1. Introduce two segments.
	batch := index.NewBatch()
	parent1 := document.NewDocument("1")
	parent1.AddField(document.NewTextField("name", []uint64{}, []byte("parent")))
	child1 := document.NewDocument("1/c1")
	child1.AddField(document.NewTextField("name", []uint64{}, []byte("child1")))
	child2 := document.NewDocument("1/c2")
	child2.AddField(document.NewTextField("name", []uint64{}, []byte("child2")))
	parent1.AddNestedDocument(child1)
	parent1.AddNestedDocument(child2)
	batch.Update(parent1)
	go func() {
		err := idx.Batch(batch)
		if err != nil {
		}
	}()
	<-batchIntroComplete

	batch.Reset()
	doc2 := document.NewDocument("2")
	doc2.AddField(document.NewTextField("name", []uint64{}, []byte("test2")))
	batch.Update(doc2)
	go func() {
		err := idx.Batch(batch)
		if err != nil {
		}
	}()
	<-batchIntroComplete

	// 2. Unpause the persister so it sees both in-memory segments and starts a merge
	close(pausePersister.Load().(chan struct{}))
	pausePersister.Store(make(chan struct{})) // Reset for next pause

	// 3. Wait for the persister to start merge and pause
	<-mergeIntroStart

	// 4. Issue a concurrent deletion while the merge is in-flight
	batch.Reset()
	batch.Delete("1")
	go func() {
		err := idx.Batch(batch)
		if err != nil {
		}
	}()
	<-batchIntroComplete

	close(pausePersister.Load().(chan struct{}))

	// 5. Wait for the merge introduction to complete
	<-mergeIntroComplete

	// 6. Close and reopen the index to see if the deletion was persisted
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	// Reopen
	idx, err = NewScorch(Name, ourConfig, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	idxr, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := idxr.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docCount, err := idxr.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	// WITHOUT THE FIX: docCount will be 2 because doc "1" was resurrected
	// WITH THE FIX: docCount will be 1
	if docCount != 1 {
		t.Errorf("Expected doc count 1, but got %d. Doc '1' might have leaked.", docCount)
	}
}


func TestObsoleteSegmentMergeIntroduction(t *testing.T) {
	testConfig := CreateConfig("TestObsoleteSegmentMergeIntroduction")
	err := InitTest(testConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := DestroyTest(testConfig)
		if err != nil {
			t.Fatal(err)
		}
	}()

	var introComplete, mergeIntroStart, mergeIntroComplete sync.WaitGroup
	introComplete.Add(1)
	mergeIntroStart.Add(1)
	mergeIntroComplete.Add(1)
	var segIntroCompleted int
	RegistryEventCallbacks["test"] = func(e Event) bool {
		switch e.Kind {
		case EventKindBatchIntroduction:
			segIntroCompleted++
			if segIntroCompleted == 3 {
				// all 3 segments introduced
				introComplete.Done()
			}
		case EventKindMergeTaskIntroductionStart:
			// signal the start of merge task introduction so that
			// we can introduce a new batch which obsoletes the
			// merged segment's contents.
			mergeIntroStart.Done()
			// hold the merge task introduction until the merged segment contents
			// are obsoleted with the next batch/segment introduction.
			introComplete.Wait()
		case EventKindMergeTaskIntroduction:
			// signal the completion of the merge task introduction.
			mergeIntroComplete.Done()

		}

		return true
	}

	ourConfig := make(map[string]interface{}, len(testConfig))
	for k, v := range testConfig {
		ourConfig[k] = v
	}
	ourConfig["eventCallbackName"] = "test"

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, ourConfig, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Open()
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// first introduce two documents over two batches.
	batch := index.NewBatch()
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test3")))
	batch.Update(doc)
	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	batch.Reset()
	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test2updated")))
	batch.Update(doc)
	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	// wait until the merger trying to introduce the new merged segment.
	mergeIntroStart.Wait()

	// execute another batch which obsoletes the contents of the new merged
	// segment awaiting introduction.
	batch.Reset()
	batch.Delete("1")
	batch.Delete("2")
	doc = document.NewDocument("3")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test3updated")))
	batch.Update(doc)
	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	// wait until the merge task introduction complete.
	mergeIntroComplete.Wait()

	idxr, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}

	numSegments := len(idxr.(*IndexSnapshot).segment)
	if numSegments != 1 {
		t.Errorf("expected one segment at the root, got: %d", numSegments)
	}

	skipIntroCount := atomic.LoadUint64(&idxr.(*IndexSnapshot).parent.stats.TotFileMergeIntroductionsObsoleted)
	if skipIntroCount != 1 {
		t.Errorf("expected one obsolete merge segment skipping the introduction, got: %d", skipIntroCount)
	}

	docCount, err := idxr.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if docCount != 1 {
		t.Errorf("Expected document count to be %d got %d", 1, docCount)
	}

	err = idxr.Close()
	if err != nil {
		t.Fatal(err)
	}
}
