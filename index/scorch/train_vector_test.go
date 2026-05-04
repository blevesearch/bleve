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

//go:build vectors
// +build vectors

package scorch

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
)

// newMinimalScorchForTrainer returns a *Scorch with just the fields required by
// vectorTrainer tests that do not touch BoltDB or the segment plugin.
func newMinimalScorchForTrainer(t *testing.T) *Scorch {
	t.Helper()
	return &Scorch{
		closeCh:       make(chan struct{}),
		segmentConfig: make(map[string]interface{}),
		config:        make(map[string]interface{}),
		path:          t.TempDir(),
	}
}

// waitGroupDone blocks until wg.Wait() returns or d elapses, returning true
// iff the wait completed in time.
func waitGroupDone(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return true
	case <-time.After(d):
		return false
	}
}

// -----------------------------------------------------------------------
// initTrainer
// -----------------------------------------------------------------------

func TestInitTrainerFlagAbsent(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	if tr := initTrainer(s, s.config); tr != nil {
		t.Fatal("expected nil trainer when flag is absent, got non-nil")
	}
}

func TestInitTrainerFlagFalse(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	cfg := map[string]interface{}{IndexTrainedWithFastMerge: false}
	if tr := initTrainer(s, cfg); tr != nil {
		t.Fatal("expected nil trainer when flag is false, got non-nil")
	}
}

func TestInitTrainerReturnsTrainer(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	cfg := map[string]interface{}{IndexTrainedWithFastMerge: true}
	if tr := initTrainer(s, cfg); tr == nil {
		t.Fatal("expected non-nil trainer when flag is true")
	}
}

func TestInitTrainerClonesConfig(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	s.config["pre-existing"] = "value"
	cfg := map[string]interface{}{IndexTrainedWithFastMerge: true}
	tr := initTrainer(s, cfg)
	if tr == nil {
		t.Fatal("expected non-nil trainer")
	}
	// Mutations to the trainer's config must not affect the parent's config.
	tr.config["canary"] = true
	if _, leaked := s.config["canary"]; leaked {
		t.Error("trainer config is the same map as parent config — expected a clone")
	}
}

// -----------------------------------------------------------------------
// getInternal
// -----------------------------------------------------------------------

func TestGetInternalDefaultFalse(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	tr := &vectorTrainer{parent: s, config: map[string]interface{}{}}

	got, err := tr.getInternal(util.BoltTrainCompleteKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != "false" {
		t.Errorf("expected 'false', got %q", got)
	}
}

func TestGetInternalTrueAfterSet(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	tr := &vectorTrainer{parent: s, config: map[string]interface{}{}}
	tr.trainingComplete.Store(true)

	got, err := tr.getInternal(util.BoltTrainCompleteKey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != "true" {
		t.Errorf("expected 'true', got %q", got)
	}
}

func TestGetInternalUnknownKey(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	tr := &vectorTrainer{parent: s, config: map[string]interface{}{}}

	got, err := tr.getInternal([]byte("no-such-key"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil for unknown key, got %v", got)
	}
}

// -----------------------------------------------------------------------
// getTrainedIndex
// -----------------------------------------------------------------------

func TestGetTrainedIndexNil(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	tr := &vectorTrainer{parent: s, config: map[string]interface{}{}}

	got, err := tr.getTrainedIndex("vec")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestGetTrainedIndexNotTrainedSegment(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	tr := &vectorTrainer{
		parent: s,
		config: map[string]interface{}{},
		// mockSegmentBase does NOT implement segment.TrainedSegment.
		trainedIndex: &SegmentSnapshot{segment: &mockSegmentBase{}},
	}

	_, err := tr.getTrainedIndex("vec")
	if err == nil {
		t.Fatal("expected error for segment that does not implement TrainedSegment")
	}
}

// mockTrainedSeg embeds mockSegmentBase and additionally satisfies
// segment.TrainedSegment by implementing GetCoarseQuantizer.
type mockTrainedSeg struct {
	mockSegmentBase
	coarseQuantizerFn func(string) (interface{}, error)
}

func (m *mockTrainedSeg) GetCoarseQuantizer(field string) (interface{}, error) {
	if m.coarseQuantizerFn != nil {
		return m.coarseQuantizerFn(field)
	}
	return nil, nil
}

func TestGetTrainedIndexReturnsCQ(t *testing.T) {
	sentinel := struct{ label string }{"centroid"}
	s := newMinimalScorchForTrainer(t)
	tr := &vectorTrainer{
		parent: s,
		config: map[string]interface{}{},
		trainedIndex: &SegmentSnapshot{
			segment: &mockTrainedSeg{
				coarseQuantizerFn: func(string) (interface{}, error) {
					return sentinel, nil
				},
			},
		},
	}

	got, err := tr.getTrainedIndex("vec")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != sentinel {
		t.Errorf("expected sentinel coarse quantizer, got %v", got)
	}
}

func TestGetTrainedIndexPropagatesError(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	tr := &vectorTrainer{
		parent: s,
		config: map[string]interface{}{},
		trainedIndex: &SegmentSnapshot{
			segment: &mockTrainedSeg{
				coarseQuantizerFn: func(string) (interface{}, error) {
					return nil, fmt.Errorf("coarse quantizer unavailable")
				},
			},
		},
	}

	_, err := tr.getTrainedIndex("vec")
	if err == nil {
		t.Fatal("expected error from GetCoarseQuantizer, got nil")
	}
}

// -----------------------------------------------------------------------
// trainLoop — exit paths that do not require BoltDB
// -----------------------------------------------------------------------

// TestTrainLoopExitsWhenComplete verifies that the loop returns
// immediately (without touching any channel) when trainingComplete is already
// set, and that it flushes the sample count into parent stats.
func TestTrainLoopExitsWhenComplete(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	tr := &vectorTrainer{
		parent:  s,
		config:  map[string]interface{}{},
		trainCh: make(chan *trainRequest, 1),
	}
	tr.trainedSamples = 42
	tr.trainingComplete.Store(true)

	s.asyncTasks.Add(1)
	go tr.trainLoop()

	if !waitGroupDone(&s.asyncTasks, 2*time.Second) {
		t.Fatal("trainLoop did not exit within deadline")
	}
	if got := atomic.LoadUint64(&s.stats.TotTrainedSamples); got != 42 {
		t.Errorf("expected TotTrainedSamples=42, got %d", got)
	}
}

// TestTrainLoopCloseChNoRequest verifies that closing the
// parent's closeCh causes the loop to exit cleanly when there is no buffered
// train request.
func TestTrainLoopCloseChNoRequest(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	tr := &vectorTrainer{
		parent:  s,
		config:  map[string]interface{}{},
		trainCh: make(chan *trainRequest, 1),
	}

	s.asyncTasks.Add(1)
	go tr.trainLoop()

	close(s.closeCh)
	if !waitGroupDone(&s.asyncTasks, 2*time.Second) {
		t.Fatal("trainLoop did not exit within deadline")
	}
}

// TestTrainLoopCloseChAcksRequest verifies that the goroutine
// exits after closeCh is closed, and that any ackCh buffered in trainCh is
// handled before the goroutine returns (no leaked ackCh).
//
// Due to Go's non-deterministic select, the loop may either:
//
//	(a) react to closeCh first → drain the request and ack it with an error, OR
//	(b) consume the request first (nil-sample, non-final → close ackCh) and
//	    then exit via closeCh on the next iteration.
//
// Either outcome is correct. The invariant under test is that ackCh is
// drained/closed before the goroutine exits, preventing a caller goroutine leak.
func TestTrainLoopCloseChAcksRequest(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	tr := &vectorTrainer{
		parent:  s,
		config:  map[string]interface{}{},
		trainCh: make(chan *trainRequest, 1),
	}

	// Buffered so that the loop's non-blocking write (if it takes closeCh path)
	// does not deadlock.
	ackCh := make(chan error, 1)
	tr.trainCh <- &trainRequest{finalSample: false, sample: nil, ackCh: ackCh}

	s.asyncTasks.Add(1)
	go tr.trainLoop()

	close(s.closeCh)
	if !waitGroupDone(&s.asyncTasks, 2*time.Second) {
		t.Fatal("trainLoop did not exit within deadline")
	}

	// ackCh must be drainable without blocking regardless of which select path
	// was taken.
	select {
	case <-ackCh:
		// received error (closeCh drain path) or nil (closed by normal processing)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("ackCh was not handled after trainLoop exited — possible goroutine leak")
	}
}

// TestTrainLoopNilSampleAcks verifies that a nil-sample,
// non-final request is acknowledged without error, and that the loop continues
// running afterwards (cleaned up by closing closeCh).
func TestTrainLoopNilSampleAcks(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	tr := &vectorTrainer{
		parent:  s,
		config:  map[string]interface{}{},
		trainCh: make(chan *trainRequest, 1),
	}

	ackCh := make(chan error, 1)
	tr.trainCh <- &trainRequest{finalSample: false, sample: nil, ackCh: ackCh}

	s.asyncTasks.Add(1)
	go tr.trainLoop()

	// The loop should close ackCh with no value written to it.
	select {
	case err := <-ackCh:
		if err != nil {
			t.Errorf("expected nil error for nil-sample non-final request, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ackCh to be closed")
	}

	// Stop the loop cleanly.
	close(s.closeCh)
	if !waitGroupDone(&s.asyncTasks, 2*time.Second) {
		t.Fatal("trainLoop did not exit within deadline after closeCh")
	}
}

// mockUnpersistedSeg satisfies both segment.Segment (via mockSegmentBase) and
// segment.UnpersistedSegment.  persistFn controls the return value of Persist.
type mockUnpersistedSeg struct {
	mockSegmentBase
	persistFn func(path string) error
}

func (m *mockUnpersistedSeg) Persist(path string) error {
	if m.persistFn != nil {
		return m.persistFn(path)
	}
	return nil
}

// TestTrainLoopPersistErrorAcks verifies that
// when persist-to-directory fails for a non-nil UnpersistedSegment, the loop:
//  1. sends an error on ackCh and closes it, and
//  2. continues running (does not return), so a subsequent closeCh can stop it.
func TestTrainLoopPersistErrorAcks(t *testing.T) {
	s := newMinimalScorchForTrainer(t)
	tr := &vectorTrainer{
		parent:  s,
		config:  map[string]interface{}{},
		trainCh: make(chan *trainRequest, 1),
	}

	ackCh := make(chan error, 1)
	tr.trainCh <- &trainRequest{
		finalSample: false,
		sample: &mockUnpersistedSeg{
			persistFn: func(string) error { return fmt.Errorf("disk full") },
		},
		ackCh: ackCh,
	}

	s.asyncTasks.Add(1)
	go tr.trainLoop()

	// The loop should write an error and close ackCh before continuing.
	select {
	case err, ok := <-ackCh:
		if !ok {
			t.Fatal("ackCh was closed before the error value could be read")
		}
		if err == nil {
			t.Fatal("expected non-nil error from failed persist, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for error on ackCh")
	}

	// Loop must still be running; stop it cleanly.
	close(s.closeCh)
	if !waitGroupDone(&s.asyncTasks, 2*time.Second) {
		t.Fatal("trainLoop did not exit within deadline after closeCh")
	}
}

func TestTrainerSampleIngestion(t *testing.T) {
	cfg := CreateConfig("TestTrainerSampleIngestion")
	cfg[IndexTrainedWithFastMerge] = true
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	analysisQueue := index.NewAnalysisQueue(1)
	s, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatalf("failed to create Scorch: %v", err)
	}
	defer s.Close()

	sc, ok := s.(*Scorch)
	if !ok {
		t.Fatal("expected Scorch instance")
	}

	batch := index.NewBatch()
	for i := 0; i < 100; i++ {
		doc := document.NewDocument(fmt.Sprintf("doc-%d", i))
		stubVal := float32(i)
		doc.AddField(document.NewVectorField("vec", nil, []float32{stubVal, stubVal + 1, stubVal + 2}, 3, "cosine", index.IndexOptimizedForRecall))
		batch.Update(doc)
	}

	tr, ok := sc.trainer.(*vectorTrainer)
	if !ok {
		t.Fatal("expected vectorTrainer instance")
	}
	if sc.rootBolt == nil {
		err := sc.openBolt()
		if err != nil {
			t.Fatalf("opening bolt failed %v", err)
		}
	}
	sc.asyncTasks.Add(1)
	go tr.trainLoop()

	err = sc.Train(batch)
	if err != nil {
		t.Fatalf("training failed: %v", err)
	}

	val, err := tr.getInternal(util.BoltTrainCompleteKey)
	if err != nil {
		t.Fatalf("failed to get internal value: %v", err)
	}

	if string(val) != "false" {
		t.Errorf("expected 'false' for training complete key, got %q", val)
	}

	if tr.trainedSamples != 100 {
		t.Errorf("expected 100 trained samples, got %d", tr.trainedSamples)
	}

	b := index.NewBatch()
	doc := document.NewDocument("doc-998")
	doc.AddField(document.NewVectorField("vec", nil, []float32{998, 998, 998}, 3, "cosine", index.IndexOptimizedForRecall))
	b.Update(doc)
	err = sc.Train(b)
	if err != nil {
		t.Fatalf("training failed: %v", err)
	}
	val, err = tr.getInternal(util.BoltTrainCompleteKey)
	if err != nil {
		t.Fatalf("failed to get internal value: %v", err)
	}

	if string(val) != "false" {
		t.Errorf("expected 'false' for training complete key, got %q", val)
	}

	if tr.trainedSamples != 101 {
		t.Errorf("expected 101 trained samples, got %d", tr.trainedSamples)
	}

	b.Reset()
	b.SetInternal(util.BoltTrainCompleteKey, []byte("true"))
	sc.Train(b)
	if err != nil {
		t.Fatalf("training failed: %v", err)
	}

	val, err = tr.getInternal(util.BoltTrainCompleteKey)
	if err != nil {
		t.Fatalf("failed to get internal value: %v", err)
	}

	if string(val) != "true" {
		t.Errorf("expected 'true' for training complete key, got %q", val)
	}

	if tr.trainedSamples != 101 {
		t.Errorf("expected 101 trained samples (no increment after training complete), got %d", tr.trainedSamples)
	}
}

func TestTrainerBlocksDataIngestion(t *testing.T) {
	cfg := CreateConfig("TestTrainerBlocksDataIngestion")
	cfg[IndexTrainedWithFastMerge] = true
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	analysisQueue := index.NewAnalysisQueue(1)
	s, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatalf("failed to create Scorch: %v", err)
	}
	defer s.Close()

	sc, ok := s.(*Scorch)
	if !ok {
		t.Fatal("expected Scorch instance")
	}

	batch := index.NewBatch()
	for i := 0; i < 100; i++ {
		doc := document.NewDocument(fmt.Sprintf("doc-%d", i))
		stubVal := float32(i)
		doc.AddField(document.NewVectorField("vec", nil, []float32{stubVal, stubVal + 1, stubVal + 2}, 3, "cosine", index.IndexOptimizedForRecall))
		batch.Update(doc)
	}

	tr, ok := sc.trainer.(*vectorTrainer)
	if !ok {
		t.Fatal("expected vectorTrainer instance")
	}
	if sc.rootBolt == nil {
		err := sc.openBolt()
		if err != nil {
			t.Fatalf("opening bolt failed %v", err)
		}
	}

	// spawn all the routines to check the data ingest as well
	err = sc.Open()
	if err != nil {
		t.Fatalf("failed to open Scorch: %v", err)
	}

	err = sc.Train(batch)
	if err != nil {
		t.Fatalf("training failed: %v", err)
	}

	val, err := tr.getInternal(util.BoltTrainCompleteKey)
	if err != nil {
		t.Fatalf("failed to get internal value: %v", err)
	}

	if string(val) != "false" {
		t.Errorf("expected 'false' for training complete key, got %q", val)
	}

	if tr.trainedSamples != 100 {
		t.Errorf("expected 100 trained samples, got %d", tr.trainedSamples)
	}

	batch.Reset()
	doc := document.NewDocument("doc-998")
	doc.AddField(document.NewVectorField("vec", nil, []float32{998, 998, 998}, 3, "cosine", index.IndexOptimizedForRecall))
	batch.Update(doc)
	err = sc.Batch(batch)
	if err == nil {
		t.Fatalf("data ingestion should be blocked during training")
	}

	batch.Reset()
	batch.SetInternal(util.BoltTrainCompleteKey, []byte("true"))
	err = sc.Train(batch)
	if err != nil {
		t.Fatalf("training failed: %v", err)
	}

	val, err = tr.getInternal(util.BoltTrainCompleteKey)
	if err != nil {
		t.Fatalf("failed to get internal value: %v", err)
	}

	if string(val) != "true" {
		t.Errorf("expected 'true' for training complete key, got %q", val)
	}

	batch.Reset()
	doc = document.NewDocument("doc-998")
	doc.AddField(document.NewVectorField("vec", nil, []float32{998, 998, 998}, 3, "cosine", index.IndexOptimizedForRecall))
	batch.Update(doc)
	err = sc.Batch(batch)
	if err != nil {
		t.Fatalf("data ingestion expected to suceed after training is complete")
	}

	err = sc.Train(batch)
	if err == nil {
		t.Fatalf("re-training is not allowed on the index")
	}

}

type dummyFileCopyAPI interface {
	CopyFile(fileName string, dst index.IndexDirectory) error
	SetPathInBolt(key []byte, value []byte) error
}

// trainedIndexWriter implements the writer used when copying the trained index
// file from the source partition into this partition's index directory.
type trainedIndexWriter struct {
	rootpath  string
	destIndex dummyFileCopyAPI
}

func (c *trainedIndexWriter) GetWriter(path string) (io.WriteCloser, error) {
	if !(strings.HasSuffix(path, index.TrainedIndexFileName)) {
		return nil, fmt.Errorf("write not allowed on path %s", path)
	}
	return os.OpenFile(filepath.Join(c.rootpath, filepath.Base(path)), os.O_CREATE|os.O_WRONLY, 0600)
}
func (c *trainedIndexWriter) SetPathInBolt(key []byte, value []byte) error {
	return c.destIndex.SetPathInBolt(key, value)
}

func TestTrainerIndexCopy(t *testing.T) {
	cfg := CreateConfig("TestTrainerIndexCopy")
	cfg[IndexTrainedWithFastMerge] = true
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}

	cfg["path"] = filepath.Clean(cfg["path"].(string))
	analysisQueue := index.NewAnalysisQueue(1)
	s, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatalf("failed to create Scorch: %v", err)
	}
	defer s.Close()

	sc, ok := s.(*Scorch)
	if !ok {
		t.Fatal("expected Scorch instance")
	}

	batch := index.NewBatch()
	for i := 0; i < 100; i++ {
		doc := document.NewDocument(fmt.Sprintf("doc-%d", i))
		stubVal := float32(i)
		doc.AddField(document.NewVectorField("vec", nil, []float32{stubVal, stubVal + 1, stubVal + 2}, 3, "cosine", index.IndexOptimizedForRecall))
		batch.Update(doc)
	}

	tr, ok := sc.trainer.(*vectorTrainer)
	if !ok {
		t.Fatal("expected vectorTrainer instance")
	}
	if sc.rootBolt == nil {
		err := sc.openBolt()
		if err != nil {
			t.Fatalf("opening bolt failed %v", err)
		}
	}
	sc.asyncTasks.Add(1)
	go tr.trainLoop()

	err = sc.Train(batch)
	if err != nil {
		t.Fatalf("training failed: %v", err)
	}

	val, err := tr.getInternal(util.BoltTrainCompleteKey)
	if err != nil {
		t.Fatalf("failed to get internal value: %v", err)
	}

	if string(val) != "false" {
		t.Errorf("expected 'false' for training complete key, got %q", val)
	}

	// -----------------------------------------------------------------------
	// At this point the source trainer has been trained with 100 samples, and the
	// trained index file should be available on disk. Now we will create a new
	// trainer in a new Scorch instance, and copy the trained index file from the
	// source trainer into the destination trainer's index directory, simulating
	// the file copy between partitions to avoid re-training.
	// -----------------------------------------------------------------------
	dstCfg := CreateConfig("TestTrainerIndexCopyDest")
	dstCfg[IndexTrainedWithFastMerge] = true
	err = InitTest(dstCfg)
	if err != nil {
		t.Fatal(err)
	}

	dstCfg["path"] = filepath.Clean(dstCfg["path"].(string))

	dstWriter := &trainedIndexWriter{
		rootpath: dstCfg["path"].(string),
	}

	dstScorch, err := NewScorch(Name, dstCfg, analysisQueue)
	if err != nil {
		t.Fatalf("failed to create new Scorch: %v", err)
	}
	defer dstScorch.Close()

	dst, ok := dstScorch.(*Scorch)
	if !ok {
		t.Fatal("expected Scorch instance")
	}
	dstWriter.destIndex = dstScorch.(dummyFileCopyAPI)

	dstTrainer, ok := dst.trainer.(*vectorTrainer)
	if !ok {
		t.Fatal("expected vectorTrainer instance")
	}
	if dst.rootBolt == nil {
		err := dst.openBolt()
		if err != nil {
			t.Fatalf("opening bolt failed %v", err)
		}
	}
	dst.asyncTasks.Add(1)
	go dstTrainer.trainLoop()

	src, ok := s.(dummyFileCopyAPI)
	if !ok {
		t.Fatal("expected dummyFileCopyAPI instance")
	}

	err = src.CopyFile(index.TrainedIndexFileName, dstWriter)
	if err != nil {
		t.Fatalf("CopyFile failed: %v", err)
	}

	val, err = dstTrainer.getInternal(util.BoltTrainCompleteKey)
	if err != nil {
		t.Fatalf("failed to get internal value: %v", err)
	}

	if string(val) != "false" {
		t.Errorf("expected 'false' for training complete key in destination trainer, got %q", val)
	}

	if trainedIndex, err := dstTrainer.getTrainedIndex("vec"); err != nil || trainedIndex == nil {
		t.Errorf("expected trained index in destination trainer, got nil")
	}

}
