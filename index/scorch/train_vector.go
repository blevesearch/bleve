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
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/RoaringBitmap/roaring/v2"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/go-faiss"
	segment "github.com/blevesearch/scorch_segment_api/v2"
	bolt "go.etcd.io/bbolt"
)

type trainRequest struct {
	sample   segment.Segment
	vecCount int
	ackCh    chan error
}

func initTrainer(s *Scorch) *vectorTrainer {
	return &vectorTrainer{
		parent:  s,
		trainCh: make(chan *trainRequest),
	}
}

type vectorTrainer struct {
	parent *Scorch

	m sync.Mutex
	// not a searchable segment in the sense that it won't return
	// the data vectors. can return centroid vectors
	centroidIndex *SegmentSnapshot
	trainCh       chan *trainRequest
}

func moveFile(sourcePath, destPath string) error {
	// rename is supposed to be atomic on the same filesystem
	err := os.Rename(sourcePath, destPath)
	if err != nil {
		return fmt.Errorf("error renaming file: %v", err)
	}
	return nil
}

// this is not a routine that will be running throughout the lifetime of the index. It's purpose
// is to only train the vector index before the data ingestion starts.
func (t *vectorTrainer) trainLoop() {
	defer func() {
		t.parent.asyncTasks.Done()
	}()
	// initialize stuff
	t.parent.segmentConfig[index.CentroidIndexCallback] = t.getCentroidIndex
	path := filepath.Join(t.parent.path, index.CentroidIndexFileName)
	for {
		select {
		case <-t.parent.closeCh:
			return
		case trainReq := <-t.trainCh:
			sampleSeg := trainReq.sample
			if t.centroidIndex == nil {
				switch seg := sampleSeg.(type) {
				case segment.UnpersistedSegment:
					err := persistToDirectory(seg, nil, path)
					if err != nil {
						trainReq.ackCh <- fmt.Errorf("error persisting segment: %v", err)
						close(trainReq.ackCh)
						return
					}
				default:
				}
			} else {
				// merge the new segment with the existing one, no need to persist?
				// persist in a tmp file and then rename - is that a fair strategy?
				t.parent.segmentConfig[index.TrainingKey] = true
				_, _, err := t.parent.segPlugin.MergeUsing([]segment.Segment{t.centroidIndex.segment, sampleSeg},
					[]*roaring.Bitmap{nil, nil}, path+".tmp", t.parent.closeCh, nil, t.parent.segmentConfig)
				if err != nil {
					trainReq.ackCh <- fmt.Errorf("error merging centroid index: %v", err)
					close(trainReq.ackCh)
				}
				// reset the training flag once completed
				t.parent.segmentConfig[index.TrainingKey] = false

				// close the existing centroid segment - it's supposed to be gc'd at this point
				t.centroidIndex.segment.Close()
				err = moveFile(path+".tmp", path)
				if err != nil {
					trainReq.ackCh <- fmt.Errorf("error renaming centroid index: %v", err)
					close(trainReq.ackCh)
				}
			}
			// a bolt transaction is necessary for failover-recovery scenario and also serves as a checkpoint
			// where we can be sure that the centroid index is available for the indexing operations downstream
			//
			// note: when the scale increases massively especially with real world dimensions of 1536+, this API
			// will have to be refactored to persist in a more resource efficient way. so having this bolt related
			// code will help in tracking the progress a lot better and avoid any redudant data streaming operations.
			//
			// todo: rethink the frequency of bolt writes
			tx, err := t.parent.rootBolt.Begin(true)
			if err != nil {
				trainReq.ackCh <- fmt.Errorf("error starting bolt transaction: %v", err)
				close(trainReq.ackCh)
				return
			}
			defer func() {
				if err != nil {
					_ = tx.Rollback()
				}
			}()

			snapshotsBucket, err := tx.CreateBucketIfNotExists(util.BoltSnapshotsBucket)
			if err != nil {
				trainReq.ackCh <- fmt.Errorf("error creating snapshots bucket: %v", err)
				close(trainReq.ackCh)
				return
			}

			trainerBucket, err := snapshotsBucket.CreateBucketIfNotExists(util.BoltTrainerKey)
			if err != nil {
				trainReq.ackCh <- fmt.Errorf("error creating centroid bucket: %v", err)
				close(trainReq.ackCh)
				return
			}

			err = trainerBucket.Put(util.BoltPathKey, []byte(index.CentroidIndexFileName))
			if err != nil {
				trainReq.ackCh <- fmt.Errorf("error updating centroid bucket: %v", err)
				close(trainReq.ackCh)
				return
			}

			err = tx.Commit()
			if err != nil {
				trainReq.ackCh <- fmt.Errorf("error committing bolt transaction: %v", err)
				close(trainReq.ackCh)
				return
			}

			err = t.parent.rootBolt.Sync()
			if err != nil {
				trainReq.ackCh <- fmt.Errorf("error on bolt sync: %v", err)
				close(trainReq.ackCh)
				return
			}

			// update the centroid index pointer
			centroidIndex, err := t.parent.segPlugin.OpenUsing(path, t.parent.segmentConfig)
			if err != nil {
				trainReq.ackCh <- fmt.Errorf("error opening centroid index: %v", err)
				close(trainReq.ackCh)
				return
			}
			t.m.Lock()
			t.centroidIndex = &SegmentSnapshot{
				segment: centroidIndex,
			}
			t.m.Unlock()
			close(trainReq.ackCh)
		}
	}
}

// loads the metadata specific to the centroid index from boltdb
func (t *vectorTrainer) loadTrainedData(bucket *bolt.Bucket) error {
	if bucket == nil {
		return nil
	}
	segmentSnapshot, err := t.parent.loadSegment(bucket)
	if err != nil {
		return err
	}
	t.m.Lock()
	defer t.m.Unlock()
	t.centroidIndex = segmentSnapshot
	return nil
}

func (t *vectorTrainer) train(batch *index.Batch) error {
	// regulate the Train function
	t.parent.FireIndexEvent()

	var trainData []index.Document
	for key, doc := range batch.IndexOps {
		if doc != nil {
			// insert _id field
			// no need to track updates/deletes over here since
			// the API is singleton
			doc.AddIDField()
		}
		if strings.HasPrefix(key, index.TrainDataPrefix) {
			trainData = append(trainData, doc)
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
	seg, _, err := t.parent.segPlugin.NewUsing(trainData, t.parent.segmentConfig)
	if err != nil {
		return err
	}

	trainReq := &trainRequest{
		sample:   seg,
		vecCount: len(trainData), // todo: multivector support
		ackCh:    make(chan error),
	}

	t.trainCh <- trainReq
	err = <-trainReq.ackCh
	if err != nil {
		return fmt.Errorf("train_vector: train() err'd out with: %w", err)
	}

	return err
}

func (t *vectorTrainer) getInternal(key []byte) ([]byte, error) {
	// todo: return the total number of vectors that have been processed so far in training
	// in cbft use that as a checkpoint to resume training for n-x samples.
	switch string(key) {
	case string(util.BoltTrainCompleteKey):
		return []byte(fmt.Sprintf("%t", t.centroidIndex != nil)), nil
	}
	return nil, nil
}

func (t *vectorTrainer) getCentroidIndex(field string) (*faiss.IndexImpl, error) {
	// return the coarse quantizer of the centroid index belonging to the field
	centroidIndexSegment, ok := t.centroidIndex.segment.(segment.CentroidIndexSegment)
	if !ok {
		return nil, fmt.Errorf("segment is not a centroid index segment")
	}

	coarseQuantizer, err := centroidIndexSegment.GetCoarseQuantizer(field)
	if err != nil {
		return nil, err
	}
	return coarseQuantizer, nil
}
