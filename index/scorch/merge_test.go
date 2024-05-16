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
		if e.Kind == EventKindBatchIntroduction {
			segIntroCompleted++
			if segIntroCompleted == 3 {
				// all 3 segments introduced
				introComplete.Done()
			}
		} else if e.Kind == EventKindMergeTaskIntroductionStart {
			// signal the start of merge task introduction so that
			// we can introduce a new batch which obsoletes the
			// merged segment's contents.
			mergeIntroStart.Done()
			// hold the merge task introduction until the merged segment contents
			// are obsoleted with the next batch/segment introduction.
			introComplete.Wait()
		} else if e.Kind == EventKindMergeTaskIntroduction {
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
