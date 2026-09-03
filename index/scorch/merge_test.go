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
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/scorch/mergeplan"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
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

func TestParseMergePlannerOptionsNumMergerWorkers(t *testing.T) {
	tests := []struct {
		name        string
		config      map[string]interface{}
		wantWorkers int
		wantErr     bool
	}{
		{
			name:        "unconfigured",
			config:      map[string]interface{}{},
			wantWorkers: mergeplan.DefaultNumMergerWorkers,
		},
		{
			name: "configured with fast merge",
			config: map[string]interface{}{
				IndexTrainedWithFastMerge: true,
				"scorchMergePlanOptions": map[string]interface{}{
					"NumMergerWorkers": 4,
				},
			},
			wantWorkers: 4,
		},
		{
			// without fast merge a merge task rebuilds its vector index out of
			// the vectors being merged, which is too heavy to overlap.
			name: "configured without fast merge stays serial",
			config: map[string]interface{}{
				"scorchMergePlanOptions": map[string]interface{}{
					"NumMergerWorkers": 4,
				},
			},
			wantWorkers: 1,
		},
		{
			name: "fast merge turned off explicitly stays serial",
			config: map[string]interface{}{
				IndexTrainedWithFastMerge: false,
				"scorchMergePlanOptions": map[string]interface{}{
					"NumMergerWorkers": 4,
				},
			},
			wantWorkers: 1,
		},
		{
			// the form the docs present the config in
			name: "configured with a lower camel case key",
			config: map[string]interface{}{
				IndexTrainedWithFastMerge: true,
				"scorchMergePlanOptions": map[string]interface{}{
					"numMergerWorkers": 4,
				},
			},
			wantWorkers: 4,
		},
		{
			name: "no workers is rejected",
			config: map[string]interface{}{
				IndexTrainedWithFastMerge: true,
				"scorchMergePlanOptions": map[string]interface{}{
					"NumMergerWorkers": 0,
				},
			},
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := &Scorch{config: test.config}
			mpo, err := s.parseMergePlannerOptions(&persisterOptions{})
			if test.wantErr {
				if err == nil {
					t.Fatal("expected an error, got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("parseMergePlannerOptions: %v", err)
			}
			if mpo.NumMergerWorkers != test.wantWorkers {
				t.Fatalf("expected %d merger workers, got %d",
					test.wantWorkers, mpo.NumMergerWorkers)
			}
		})
	}
}

// mergeBarrierPlugin wraps a segment plugin so that a test can observe how many
// of a plan's file merges are in flight at once. Every merge announces itself on
// arrivedCh and then waits for releaseCh before doing any real work.
type mergeBarrierPlugin struct {
	SegmentPlugin
	arrivedCh chan struct{}
	releaseCh chan struct{}
	// when set, every merge fails with this instead of merging
	mergeErr error
}

func (p *mergeBarrierPlugin) MergeUsing(segments []segment.Segment, drops []*roaring.Bitmap,
	path string, closeCh chan struct{}, sr segment.StatsReporter,
	config map[string]interface{}) ([][]uint64, uint64, error) {
	p.arrivedCh <- struct{}{}
	<-p.releaseCh
	if p.mergeErr != nil {
		return nil, 0, p.mergeErr
	}
	return p.SegmentPlugin.MergeUsing(segments, drops, path, closeCh, sr, config)
}

// newMergeTestScorch builds the minimum Scorch that performFileMerges needs: a
// directory to write the merged segments into, a plugin and a worker count.
func newMergeTestScorch(t *testing.T, plugin SegmentPlugin, numWorkers int) *Scorch {
	t.Helper()
	return &Scorch{
		path:                t.TempDir(),
		segPlugin:           plugin,
		mergePlannerOptions: &mergeplan.MergePlanOptions{NumMergerWorkers: numWorkers},
	}
}

// newMergeTestBatches builds numBatches merge batches, each holding a single
// one-document segment to be merged into a file of its own.
func newMergeTestBatches(t *testing.T, s *Scorch, numBatches int) []*mergeBatch {
	t.Helper()
	batches := make([]*mergeBatch, numBatches)
	for i := range batches {
		seg, _, err := defaultSegmentPlugin.New([]index.Document{genDoc(t)})
		if err != nil {
			t.Fatalf("failed to create segment: %v", err)
		}
		t.Cleanup(func() {
			if cerr := seg.Close(); cerr != nil {
				t.Errorf("closing input segment: %v", cerr)
			}
		})

		newSegmentID := atomic.AddUint64(&s.nextSegmentID, 1)
		batches[i] = &mergeBatch{
			segments:    []segment.Segment{seg},
			drops:       []*roaring.Bitmap{nil},
			newID:       newSegmentID,
			newFilename: zapFileName(newSegmentID),
		}
	}
	return batches
}

// newMergeTestCloseChWrapper returns a wrapper that never cancels, so that the
// merges under test are only paced by the barrier plugin.
func newMergeTestCloseChWrapper(t *testing.T) *closeChWrapper {
	t.Helper()
	cw := newCloseChWrapper(nil, context.Background())
	t.Cleanup(cw.close)
	go cw.listen()
	return cw
}

func TestPerformFileMergesRunsBatchesConcurrently(t *testing.T) {
	const numBatches = 4
	plugin := &mergeBarrierPlugin{
		SegmentPlugin: defaultSegmentPlugin,
		arrivedCh:     make(chan struct{}, numBatches),
		releaseCh:     make(chan struct{}),
	}
	s := newMergeTestScorch(t, plugin, numBatches)
	batches := newMergeTestBatches(t, s, numBatches)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.performFileMerges(batches, newMergeTestCloseChWrapper(t))
	}()

	// every batch has to reach its merge before any of them is allowed to
	// finish, which can only happen if the batches run at the same time.
	for i := 0; i < numBatches; i++ {
		select {
		case <-plugin.arrivedCh:
		case <-time.After(30 * time.Second):
			close(plugin.releaseCh)
			t.Fatalf("only %d of %d batches reached the merge", i, numBatches)
		}
	}
	close(plugin.releaseCh)

	if err := <-errCh; err != nil {
		t.Fatalf("performFileMerges: %v", err)
	}
	for i, batch := range batches {
		if batch.new == nil {
			t.Fatalf("batch %d was not merged into a new segment", i)
		}
		if err := batch.new.Close(); err != nil {
			t.Fatalf("closing merged segment %d: %v", i, err)
		}
	}
}

func TestPerformFileMergesHonoursWorkerLimit(t *testing.T) {
	const numBatches = 3
	plugin := &mergeBarrierPlugin{
		SegmentPlugin: defaultSegmentPlugin,
		arrivedCh:     make(chan struct{}, numBatches),
		releaseCh:     make(chan struct{}),
	}
	s := newMergeTestScorch(t, plugin, 1)
	batches := newMergeTestBatches(t, s, numBatches)

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.performFileMerges(batches, newMergeTestCloseChWrapper(t))
	}()

	// the lone worker lets exactly one batch into its merge...
	select {
	case <-plugin.arrivedCh:
	case <-time.After(30 * time.Second):
		close(plugin.releaseCh)
		t.Fatal("the first batch never reached the merge")
	}
	// ...and the rest wait for it instead of piling in behind it.
	select {
	case <-plugin.arrivedCh:
		close(plugin.releaseCh)
		t.Fatal("a second batch reached the merge while the only worker was busy")
	case <-time.After(250 * time.Millisecond):
	}
	close(plugin.releaseCh)

	if err := <-errCh; err != nil {
		t.Fatalf("performFileMerges: %v", err)
	}
	for i, batch := range batches {
		if batch.new == nil {
			t.Fatalf("batch %d was not merged into a new segment", i)
		}
		if err := batch.new.Close(); err != nil {
			t.Fatalf("closing merged segment %d: %v", i, err)
		}
	}
}

func TestPerformFileMergesReportsErrClosedUnwrapped(t *testing.T) {
	const numBatches = 3
	plugin := &mergeBarrierPlugin{
		SegmentPlugin: defaultSegmentPlugin,
		arrivedCh:     make(chan struct{}, numBatches),
		releaseCh:     make(chan struct{}),
		mergeErr:      segment.ErrClosed,
	}
	// nothing to choreograph here, let every batch run straight into its failure
	close(plugin.releaseCh)

	s := newMergeTestScorch(t, plugin, numBatches)
	batches := newMergeTestBatches(t, s, numBatches)

	err := s.performFileMerges(batches, newMergeTestCloseChWrapper(t))
	// the merger loop compares the error against ErrClosed to tell that the
	// index is going away, so a plan that failed only for that reason has to
	// report it as itself rather than as a join of copies of it.
	if err != segment.ErrClosed {
		t.Fatalf("expected %v, got %v", segment.ErrClosed, err)
	}
}
