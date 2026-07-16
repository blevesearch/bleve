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
	"encoding/binary"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

type trainRequest struct {
	finalSample          bool
	sampleSize           int
	ackCh                chan error
	sample               segment.Segment
	trainingParams       *index.TrainingParams
	removedFileWriterIDs map[string]struct{}
}

type vectorTrainer struct {
	trainingComplete atomic.Bool
	trainedSamples   uint64
	parent           *Scorch
	config           map[string]interface{}

	m sync.RWMutex
	// not a searchable segment in the sense that it won't return
	// the data vectors, returns trained centroid layout
	trainedIndex *SegmentSnapshot
	trainCh      chan *trainRequest
	// closed by trainLoop when it exits (via close, training completion, or
	// error). Producers select on it so they never block on a request that the
	// trainer will never consume/ack.
	doneCh chan struct{}
}

const IndexTrainedWithFastMerge = "vector_index_fast_merge"

func initTrainer(s *Scorch, config map[string]interface{}) *vectorTrainer {
	if f, ok := config[IndexTrainedWithFastMerge]; ok {
		feature, ok := f.(bool)
		if ok && feature {
			trainer := vectorTrainer{
				parent:  s,
				config:  maps.Clone(s.config),
				trainCh: make(chan *trainRequest, 1),
				doneCh:  make(chan struct{}),
			}
			// update the parent scorch config with the trainer's callback to fetch the trained index
			s.segmentConfig[index.TrainedIndexCallback] = index.TrainedIndexCallbackFn(trainer.getTrainedIndex)
			return &trainer
		}
	}
	return nil
}

func moveFile(sourcePath, destPath string) error {
	// rename is supposed to be atomic on the same filesystem
	err := os.Rename(sourcePath, destPath)
	if err != nil {
		return fmt.Errorf("error renaming file: %v", err)
	}
	return nil
}

func (t *vectorTrainer) persistToBolt(trainReq *trainRequest) error {
	tx, err := t.parent.rootBolt.Begin(true)
	if err != nil {
		return fmt.Errorf("error starting bolt transaction: %v", err)
	}
	defer tx.Rollback()

	snapshotsBucket, err := tx.CreateBucketIfNotExists(util.BoltSnapshotsBucket)
	if err != nil {
		return fmt.Errorf("error creating snapshots bucket: %v", err)
	}

	trainerBucket, err := snapshotsBucket.CreateBucketIfNotExists(util.BoltTrainerKey)
	if err != nil {
		return fmt.Errorf("error creating trained index bucket: %v", err)
	}

	segmentPath := t.parent.path + string(os.PathSeparator) + index.TrainedIndexFileName
	if _, err := os.Stat(segmentPath); !os.IsNotExist(err) {
		err = trainerBucket.Put(util.BoltPathKey, []byte(index.TrainedIndexFileName), nil)
		if err != nil {
			return fmt.Errorf("error updating trained index bucket: %v", err)
		}
	}

	t.trainingComplete.Store(trainReq.finalSample)
	err = trainerBucket.Put(util.BoltTrainCompleteKey, []byte(strconv.FormatBool(trainReq.finalSample)), nil)
	if err != nil {
		return fmt.Errorf("error updating train complete key: %v", err)
	}

	totSamples := atomic.AddUint64(&t.trainedSamples, uint64(trainReq.sampleSize))
	err = trainerBucket.Put(util.BoltTrainedSamplesKey, binary.LittleEndian.AppendUint64(nil, totSamples), nil)
	if err != nil {
		return fmt.Errorf("error updating trained samples key: %v", err)
	}

	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("error committing bolt transaction: %v", err)
	}

	return t.parent.rootBolt.Sync()
}

// trainLoop is not a routine that runs throughout the lifetime of the index.
// Its purpose is only to train the vector index before data ingestion starts.
// It is the sole consumer of trainCh and the sole owner of trainedIndex
// mutations originating from training.
func (t *vectorTrainer) trainLoop() {
	defer t.parent.asyncTasks.Done()
	// signal any blocked producers that no further requests will be serviced.
	defer close(t.doneCh)

	start := time.Now()
	path := filepath.Join(t.parent.path, index.TrainedIndexFileName)
	// config is owned exclusively by this goroutine. t.config is read
	// concurrently by train(), so it must not be mutated here.
	config := maps.Clone(t.config)

	for {
		// exit once the final sample set has been ingested and training is complete.
		if t.trainingComplete.Load() {
			t.recordTrainingStats(start)
			return
		}
		select {
		case <-t.parent.closeCh:
			// the deferred close(doneCh) releases any producer blocked on
			// trainCh or its ackCh, so there is no need to drain here.
			return
		case req := <-t.trainCh:
			shutdown, err := t.handleRequest(req, path, config)
			ackRequest(req, err)
			if shutdown {
				return
			}
		}
	}
}

// recordTrainingStats publishes the final training counters once the loop exits.
func (t *vectorTrainer) recordTrainingStats(start time.Time) {
	atomic.StoreUint64(&t.parent.stats.TotTrainedSamples, atomic.LoadUint64(&t.trainedSamples))
	atomic.StoreUint64(&t.parent.stats.TotTrainTime, uint64(time.Since(start).Milliseconds()))
}

// ackRequest delivers a request's result to the producer waiting in submit and
// then releases it. A non-nil err is sent before the close so submit observes
// it; the send blocks until submit receives, which the shutdown handshake in
// submit relies on.
func ackRequest(req *trainRequest, err error) {
	if err != nil {
		req.ackCh <- err
	}
	close(req.ackCh)
}

// handleRequest processes a single training request. The returned err, if
// non-nil, is already formatted for the producer. shutdown reports whether the
// trainLoop must exit: every failure is fatal except a failed initial persist,
// which leaves the trainer able to retry on the next sample.
func (t *vectorTrainer) handleRequest(req *trainRequest, path string, config map[string]interface{}) (shutdown bool, err error) {
	if req.trainingParams != nil {
		// mutate the goroutine-local config only; t.config is read concurrently
		// by train() and must stay immutable after init.
		config[index.TrainingKey] = req.trainingParams
	}

	// remove any file writer ids that are no longer in use
	if req.removedFileWriterIDs != nil {
		if err := t.removeFileWriterIDs(req.removedFileWriterIDs); err != nil {
			return true, fmt.Errorf("error removing file writer ids: %v", err)
		}
		return false, nil
	}

	// no sample segment: just persist state if this is the final sample.
	if req.sample == nil {
		if req.finalSample {
			if err := t.persistToBolt(req); err != nil {
				return true, fmt.Errorf("error persisting to bolt: %v", err)
			}
		}
		return false, nil
	}

	// snapshot the current trained segment under the read lock; MergeUsing below
	// only reads it, so concurrent getTrainedIndex readers stay safe.
	t.m.RLock()
	prev := t.trainedIndex
	t.m.RUnlock()

	if prev == nil {
		if seg, ok := req.sample.(segment.UnpersistedSegment); ok {
			if err := persistToDirectory(seg, nil, path); err != nil {
				// recoverable: retry on the next sample rather than tear down.
				return false, fmt.Errorf("error persisting segment: %v", err)
			}
		}
	} else {
		// merge the new sample with the existing index into a .tmp file; it is
		// renamed into place by publishTrainedIndex (Os.Open on the live path is
		// unsafe during the merge).
		_, _, mErr := t.parent.segPlugin.MergeUsing([]segment.Segment{prev.segment, req.sample},
			[]*roaring.Bitmap{nil, nil}, path+".tmp", t.parent.closeCh, nil, config)
		if mErr != nil {
			return true, fmt.Errorf("error merging trained index: %v", mErr)
		}
	}

	if err := t.publishTrainedIndex(req, prev, path, config); err != nil {
		return true, err
	}
	return false, nil
}

// publishTrainedIndex makes the freshly written index visible to readers. The
// old segment handle must be closed before moveFile can replace the backing
// file, so the close -> rename -> open -> pointer-swap sequence runs under the
// write lock; otherwise a reader holding RLock could observe a closed segment
// or a missing file. On any error past the close, the pointer is reset to nil
// so readers never see the freed prev segment (callers fall back to naive
// merge). The bolt write acts as a failover-recovery checkpoint: callers
// downstream can rely on the trained index being available once this completes.
// todo: rethink the frequency of bolt writes
func (t *vectorTrainer) publishTrainedIndex(req *trainRequest, prev *SegmentSnapshot, path string, config map[string]interface{}) error {
	t.m.Lock()
	defer t.m.Unlock()

	if prev != nil {
		if err := prev.segment.Close(); err != nil {
			t.trainedIndex = nil
			return fmt.Errorf("error closing previous trained index: %v", err)
		}
		if err := moveFile(path+".tmp", path); err != nil {
			t.trainedIndex = nil
			return fmt.Errorf("error renaming trained index: %v", err)
		}
	}

	if err := t.persistToBolt(req); err != nil {
		if prev != nil {
			t.trainedIndex = nil
		}
		return fmt.Errorf("error persisting to bolt: %v", err)
	}

	trainedIndex, err := t.parent.segPlugin.OpenUsing(path, config)
	if err != nil {
		t.trainedIndex = nil
		return fmt.Errorf("error opening trained index: %v", err)
	}
	t.trainedIndex = &SegmentSnapshot{segment: trainedIndex}
	return nil
}

// loadTrainedData loads the trained-index metadata from boltdb during init. The
// trainedIndex write is guarded because callback-driven readers may already be
// registered by the time this runs.
func (t *vectorTrainer) loadTrainedData(bucket *util.BoltBucketImpl) error {
	if bucket == nil {
		return nil
	}

	reader, err := util.NewFileReader("", nil)
	if err != nil {
		return fmt.Errorf("error creating file reader: %v", err)
	}

	// segmentSnapshot is constructed only if the trained index file path exists. If
	// application layer fails to create one and the underlying path doesn't exist, we'll
	// just skip loading the trained index and fallback to naive merge
	var segmentSnapshot *SegmentSnapshot
	pathBytes, err := bucket.Get(util.BoltPathKey, nil)
	if pathBytes != nil {
		segmentSnapshot, err = t.parent.loadSegment(bucket, reader)
		if err != nil {
			return err
		}
	}

	// get the training status out of bolt
	trainComplete, err := bucket.Get(util.BoltTrainCompleteKey, nil)
	if err != nil {
		return fmt.Errorf("error getting train complete: %v", err)
	}
	trainedSamples, err := bucket.Get(util.BoltTrainedSamplesKey, nil)
	if err != nil {
		return fmt.Errorf("error getting trained samples: %v", err)
	}
	if len(trainedSamples) == 8 {
		atomic.StoreUint64(&t.trainedSamples, binary.LittleEndian.Uint64(trainedSamples))
	}
	if len(trainComplete) > 0 {
		comp, err := strconv.ParseBool(string(trainComplete))
		if err != nil {
			return fmt.Errorf("error parsing train complete: %v", err)
		}
		t.trainingComplete.Store(comp)
	}

	t.m.Lock()
	defer t.m.Unlock()
	t.trainedIndex = segmentSnapshot
	return nil
}

func (t *vectorTrainer) train(batch *index.Batch) error {
	// regulate the Train function
	t.parent.FireIndexEvent()
	if t.trainingComplete.Load() {
		return fmt.Errorf("training is already complete, cannot accept more training data")
	}

	var trainData []index.Document
	for _, doc := range batch.IndexOps {
		if doc != nil {
			// insert _id field
			// no need to track updates/deletes over here since
			// the API is singleton
			doc.AddIDField()
		}
		trainData = append(trainData, doc)
	}

	trainComplete := batch.InternalOps[string(util.BoltTrainCompleteKey)]
	if trainComplete == nil {
		trainComplete = []byte("false")
	}
	fin, err := strconv.ParseBool(string(trainComplete))
	if err != nil {
		return fmt.Errorf("error parsing train complete: %v", err)
	}

	trainReq := &trainRequest{
		finalSample: fin,
		sampleSize:  len(trainData),
		ackCh:       make(chan error),
	}
	// setting the training params using the internal value before the actual
	// training has started
	config := t.config
	if atomic.LoadUint64(&t.trainedSamples) == 0 {
		trainingParamsBytes := batch.InternalOps[index.TrainingKey]
		var trainingParams index.TrainingParams
		if trainingParamsBytes != nil {
			err = util.UnmarshalJSON(trainingParamsBytes, &trainingParams)
			if err != nil {
				return fmt.Errorf("error parsing training params: %v", err)
			}
			trainReq.trainingParams = &trainingParams
			config = maps.Clone(t.config)
			config[index.TrainingKey] = &trainingParams
		}
	}

	// just builds a new vector index out of the train data provided
	// this is not necessarily the final train data since this is submitted
	// as a request to the trainer component to be merged. once the training
	// is complete, the template will be used for other operations down the line
	// like merge and search.
	//
	// note: this might index text data too, how to handle this? s.segmentConfig?
	// todo: updates/deletes -> data drift detection
	if len(trainData) > 0 {
		trainReq.sample, _, err = t.parent.segPlugin.NewUsing(trainData, config)
		if err != nil {
			return err
		}
	}

	if err := t.submit(trainReq); err != nil {
		return fmt.Errorf("train_vector: train() err'd out with: %w", err)
	}

	return nil
}

// submit hands a request to the trainLoop and waits for its ack. It never
// blocks past trainer shutdown: if the trainLoop has already exited (via close,
// training completion, or error) the doneCh releases the caller instead of
// letting it hang on a request that will never be consumed or acked.
func (t *vectorTrainer) submit(req *trainRequest) error {
	select {
	case t.trainCh <- req:
	case <-t.doneCh:
		return fmt.Errorf("trainer is closed")
	}

	select {
	case err := <-req.ackCh:
		return err
	case <-t.doneCh:
		// trainLoop acks every request it consumes before returning, so if our
		// request was processed the ack is already available — prefer it over
		// reporting a shutdown that didn't actually drop this request.
		select {
		case err := <-req.ackCh:
			return err
		default:
			return fmt.Errorf("trainer is closed")
		}
	}
}

func (t *vectorTrainer) getInternal(key []byte) ([]byte, error) {
	switch string(key) {
	case string(util.BoltTrainCompleteKey):
		return []byte(strconv.FormatBool(t.trainingComplete.Load())), nil
	}
	return nil, nil
}

func (t *vectorTrainer) getTrainedIndex(field string) (interface{}, error) {
	// return the coarse quantizer of the trained faiss index belonging to the field
	// if its not available then zap performs naive merge
	t.m.RLock()
	defer t.m.RUnlock()
	if t.trainedIndex != nil {
		trainedSegment, ok := t.trainedIndex.segment.(segment.TrainedSegment)
		if !ok {
			return nil, fmt.Errorf("segment is not a trained index segment")
		}

		coarseQuantizer, err := trainedSegment.GetCoarseQuantizer(field)
		if err != nil {
			return nil, err
		}
		return coarseQuantizer, nil
	}
	return nil, nil
}

func (t *vectorTrainer) copyFileLOCKED(file string, d index.IndexDirectory) error {
	if strings.HasSuffix(file, index.TrainedIndexFileName) {
		// trained index file - this is outside the snapshots domain so the bolt update is different
		err := d.SetPathInBolt(util.BoltTrainerKey, []byte(file))
		if err != nil {
			return fmt.Errorf("error updating dest index bolt: %w", err)
		}

		// update the stats in destination
		err = d.SetPathInBolt(util.BoltTrainedSamplesKey, binary.LittleEndian.AppendUint64(nil, atomic.LoadUint64(&t.trainedSamples)))
		if err != nil {
			return fmt.Errorf("error updating trained samples key: %w", err)
		}
	}

	return nil
}

func (t *vectorTrainer) updateBolt(snapshotsBucket *util.BoltBucketImpl, key []byte, value []byte) error {
	switch string(key) {
	case string(util.BoltTrainerKey):
		trainerBucket, err := snapshotsBucket.CreateBucketIfNotExists(util.BoltTrainerKey)
		if err != nil {
			return err
		}
		if trainerBucket == nil {
			return fmt.Errorf("trainer bucket not found")
		}

		// guard against duplicate updates
		existingValue, err := trainerBucket.Get(util.BoltPathKey, nil)
		if err != nil {
			return fmt.Errorf("error checking existing value: %v", err)
		}
		if existingValue != nil {
			return fmt.Errorf("key already exists %v %v", t.parent.path, string(existingValue))
		}

		err = trainerBucket.Put(util.BoltPathKey, value, nil)
		if err != nil {
			return err
		}

		reader, err := util.NewFileReader("", nil)
		if err != nil {
			return fmt.Errorf("error creating file reader: %v", err)
		}

		// update the centroid index pointer under the write lock; getTrainedIndex
		// and trainLoop may read t.trainedIndex concurrently.
		seg, err := t.parent.loadSegment(trainerBucket, reader)
		if err != nil {
			return err
		}
		t.m.Lock()
		t.trainedIndex = seg
		t.m.Unlock()

	case string(util.BoltTrainedSamplesKey):
		trainedSamples := binary.LittleEndian.Uint64(value)
		atomic.StoreUint64(&t.trainedSamples, trainedSamples)
		trainerBucket, err := snapshotsBucket.CreateBucketIfNotExists(util.BoltTrainerKey)
		if err != nil {
			return err
		}
		err = trainerBucket.Put(util.BoltTrainedSamplesKey, value, nil)
		if err != nil {
			return err
		}
	}

	return nil
}

// dropFileWriterIDs removes the given file writer ids from the trained index.
// This is a lifecycle operation (key rotation) that must work for the entire
// life of the trained index, not just while training is running.
func (t *vectorTrainer) dropFileWriterIDs(ids map[string]struct{}) error {
	if t.trainingComplete.Load() {
		if err := t.removeFileWriterIDs(ids); err != nil {
			return fmt.Errorf("train_vector: dropFileWriterIDs() err'd out with: %w", err)
		}
		return nil
	}

	trainReq := &trainRequest{
		ackCh:                make(chan error),
		removedFileWriterIDs: ids,
	}
	if err := t.submit(trainReq); err != nil {
		return fmt.Errorf("train_vector: dropFileWriterIDs() err'd out with: %w", err)
	}

	return nil
}

func (t *vectorTrainer) removeFileWriterIDs(ids map[string]struct{}) error {
	// trainer bucket in the bolt file doesn't have any sensitive data to be encrypted,
	// so we don't have to update it with new writer ids.
	// invoke the merge API and let the zap side of things handle the update of writer IDs and also
	// the data in the trained index.
	t.m.Lock()
	defer t.m.Unlock()
	trainedIndexPath := filepath.Join(t.parent.path, index.TrainedIndexFileName)
	if t.trainedIndex != nil {
		if encryptedIndex, ok := t.trainedIndex.segment.(segment.SegmentWithCallbacks); ok {
			if _, ok := ids[encryptedIndex.CallbackId()]; ok {
				_, _, err := t.parent.segPlugin.MergeUsing([]segment.Segment{t.trainedIndex.segment},
					[]*roaring.Bitmap{nil}, trainedIndexPath+"temp", t.parent.closeCh, nil, t.config)
				if err != nil {
					if serr := os.Remove(trainedIndexPath + "temp"); serr != nil {
						return fmt.Errorf("error removing temp trained index: %v", serr)
					}
					return err
				}

				err = t.trainedIndex.segment.Close()
				if err != nil {
					if serr := os.Remove(trainedIndexPath + "temp"); serr != nil {
						return fmt.Errorf("error removing temp trained index: %v", serr)
					}
					t.trainedIndex = nil
					return err
				}

				if err := moveFile(trainedIndexPath+"temp", trainedIndexPath); err != nil {
					if serr := os.Remove(trainedIndexPath + "temp"); serr != nil {
						return fmt.Errorf("error removing temp trained index: %v", serr)
					}
					t.trainedIndex = nil
					return err
				}

				trainedIndex, err := t.parent.segPlugin.OpenUsing(trainedIndexPath, t.config)
				if err != nil {
					t.trainedIndex = nil
					return err
				}

				t.trainedIndex = &SegmentSnapshot{segment: trainedIndex}
			}
		}
	}
	return nil
}

func (t *vectorTrainer) fileWriterIDsInUse() (map[string]struct{}, error) {
	t.m.RLock()
	defer t.m.RUnlock()
	writerIDs := make(map[string]struct{})
	if t.trainedIndex != nil {
		if seg, ok := t.trainedIndex.segment.(segment.SegmentWithCallbacks); ok {
			writerIDs[seg.CallbackId()] = struct{}{}
		}
	}

	return writerIDs, nil
}
