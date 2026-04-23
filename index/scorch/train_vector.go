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
	"bytes"
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
	finalSample bool
	sampleSize  int
	ackCh       chan error
	sample      segment.Segment
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
	err = trainerBucket.Put(util.BoltPathKey, []byte(index.TrainedIndexFileName), nil)
	if err != nil {
		return fmt.Errorf("error updating trained index bucket: %v", err)
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

// this is not a routine that will be running throughout the lifetime of the index. It's purpose
// is to only train the vector index before the data ingestion starts.
func (t *vectorTrainer) trainLoop() {
	defer func() {
		t.parent.asyncTasks.Done()
	}()

	trainLoopStartTime := time.Now()
	path := filepath.Join(t.parent.path, index.TrainedIndexFileName)
	for {
		select {
		case <-t.parent.closeCh:
			select {
			case req := <-t.trainCh:
				req.ackCh <- fmt.Errorf("trainer is closed")
				close(req.ackCh)
			default:
			}
			return
		case trainReq := <-t.trainCh:
			sampleSeg := trainReq.sample
			if t.trainedIndex == nil {
				switch seg := sampleSeg.(type) {
				case segment.UnpersistedSegment:
					err := persistToDirectory(seg, nil, path)
					if err != nil {
						trainReq.ackCh <- fmt.Errorf("error persisting segment: %v", err)
						close(trainReq.ackCh)
						continue
					}
				default:
				}
			} else {
				// merge the new segment with the existing one, to create a new
				// .tmp trained index file and then move it to the actual
				// trained index file path (during the merge, Os.Open(trainedIndexPath)
				// won't be safe since its still being used for merge)
				if trainReq.sampleSize > 0 {
					t.config[index.TrainingKey] = true
					_, _, err := t.parent.segPlugin.MergeUsing([]segment.Segment{t.trainedIndex.segment, sampleSeg},
						[]*roaring.Bitmap{nil, nil}, path+".tmp", t.parent.closeCh, nil, t.config)
					if err != nil {
						trainReq.ackCh <- fmt.Errorf("error merging trained index: %v", err)
						close(trainReq.ackCh)
						return
					}
					// reset the training flag once completed
					t.config[index.TrainingKey] = false

					// close the existing trained segment - it's supposed to be gc'd at this point
					t.trainedIndex.segment.Close()
					err = moveFile(path+".tmp", path)
					if err != nil {
						trainReq.ackCh <- fmt.Errorf("error renaming trained index: %v", err)
						close(trainReq.ackCh)
						return
					}
				}
			}

			// a bolt transaction is necessary for failover-recovery scenario and also serves as a checkpoint
			// where we can be sure that the trained index is available for the indexing operations downstream
			//
			// note: when the scale increases massively especially with real world dimensions of 1536+, this API
			// will have to be refactored to persist in a more resource efficient way. so having this bolt related
			// code will help in tracking the progress a lot better and avoid any redudant data streaming operations.
			//
			// todo: rethink the frequency of bolt writes
			err := t.persistToBolt(trainReq)
			if err != nil {
				trainReq.ackCh <- fmt.Errorf("error persisting to bolt: %v", err)
				close(trainReq.ackCh)
				return
			}

			// update the trained index pointer
			trainedIndex, err := t.parent.segPlugin.OpenUsing(path, t.parent.segmentConfig)
			if err != nil {
				trainReq.ackCh <- fmt.Errorf("error opening trained index: %v", err)
				close(trainReq.ackCh)
				return
			}

			t.m.Lock()
			t.trainedIndex = &SegmentSnapshot{
				segment: trainedIndex,
			}
			t.m.Unlock()
			close(trainReq.ackCh)

			// exit the trainer loop we've ingested the final sample set and training
			// is assumed to be complete.
			if t.trainingComplete.Load() {
				atomic.StoreUint64(&t.parent.stats.TotTrainedSamples, t.trainedSamples)
				atomic.StoreUint64(&t.parent.stats.TotTrainTime, uint64(time.Since(trainLoopStartTime).Milliseconds()))
				return
			}
		}
	}
}

// loads the metadata specific to the trained index from boltdb, happens during init
// no lock needed
func (t *vectorTrainer) loadTrainedData(bucket *util.BoltBucketImpl) error {
	if bucket == nil {
		return nil
	}
	writerID, err := bucket.Get(util.BoltMetaDataFileWriterIDKey, nil)
	if err != nil {
		return fmt.Errorf("error getting writer id: %v", err)
	}
	reader, err := util.NewFileReader(string(writerID), nil)
	if err != nil {
		return fmt.Errorf("error creating file reader: %v", err)
	}

	segmentSnapshot, err := t.parent.loadSegment(bucket, reader)
	if err != nil {
		return err
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
	atomic.StoreUint64(&t.trainedSamples, binary.LittleEndian.Uint64(trainedSamples))
	comp, err := strconv.ParseBool(string(trainComplete))
	if err != nil {
		return fmt.Errorf("error parsing train complete: %v", err)
	}
	t.trainingComplete.Store(comp)

	t.m.Lock()
	defer t.m.Unlock()
	t.trainedIndex = segmentSnapshot
	return nil
}

func (t *vectorTrainer) train(batch *index.Batch) error {
	// regulate the Train function
	t.parent.FireIndexEvent()

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

	// just builds a new vector index out of the train data provided
	// this is not necessarily the final train data since this is submitted
	// as a request to the trainer component to be merged. once the training
	// is complete, the template will be used for other operations down the line
	// like merge and search.
	//
	// note: this might index text data too, how to handle this? s.segmentConfig?
	// todo: updates/deletes -> data drift detection
	seg, _, err := t.parent.segPlugin.NewUsing(trainData, t.parent.segmentConfig)
	if err != nil {
		return err
	}

	trainReq := &trainRequest{
		finalSample: fin,
		sampleSize:  len(trainData),
		ackCh:       make(chan error),
		sample:      seg,
	}

	t.trainCh <- trainReq
	err = <-trainReq.ackCh
	if err != nil {
		return fmt.Errorf("train_vector: train() err'd out with: %w", err)
	}

	return err
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
	}

	return nil
}

func (t *vectorTrainer) updateBolt(snapshotsBucket *util.BoltBucketImpl, key []byte, value []byte) error {
	if bytes.Equal(key, util.BoltTrainerKey) {
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

		writerID, err := trainerBucket.Get(util.BoltMetaDataFileWriterIDKey, nil)
		if err != nil {
			return fmt.Errorf("error getting writer id: %v", err)
		}
		reader, err := util.NewFileReader(string(writerID), nil)
		if err != nil {
			return fmt.Errorf("error creating file reader: %v", err)
		}

		// update the centroid index pointer
		t.trainedIndex, err = t.parent.loadSegment(trainerBucket, reader)
		if err != nil {
			return err
		}
	}

	return nil
}
