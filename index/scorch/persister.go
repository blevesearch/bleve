//  Copyright (c) 2017 Couchbase, Inc.
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
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/index/scorch/segment"
	"github.com/blevesearch/bleve/index/scorch/segment/mem"
	"github.com/blevesearch/bleve/index/scorch/segment/zap"
	"github.com/boltdb/bolt"
)

type notificationChan chan struct{}

func (s *Scorch) persisterLoop() {
	var notify notificationChan
	var lastPersistedEpoch uint64
OUTER:
	for {
		select {
		case <-s.closeCh:
			break OUTER
		case notify = <-s.persisterNotifier:

		default:
			// check to see if there is a new snapshot to persist
			s.rootLock.RLock()
			ourSnapshot := s.root
			s.rootLock.RUnlock()

			//for ourSnapshot.epoch != lastPersistedEpoch {
			if ourSnapshot.epoch != lastPersistedEpoch {
				// lets get started
				err := s.persistSnapshot(ourSnapshot)
				if err != nil {
					log.Printf("got err persisting snapshot: %v", err)
					continue OUTER
				}
				lastPersistedEpoch = ourSnapshot.epoch
				if notify != nil {
					close(notify)
					notify = nil
				}
			}

			// tell the introducer we're waiting for changes
			// first make a notification chan
			notifyUs := make(notificationChan)

			// give it to the introducer
			select {
			case <-s.closeCh:
				break OUTER
			case s.introducerNotifier <- notifyUs:
			}

			// check again
			s.rootLock.RLock()
			ourSnapshot = s.root
			s.rootLock.RUnlock()
			if ourSnapshot.epoch != lastPersistedEpoch {

				// lets get started
				err := s.persistSnapshot(ourSnapshot)
				if err != nil {
					log.Printf("got err persisting snapshot: %v", err)
					continue OUTER
				}
				lastPersistedEpoch = ourSnapshot.epoch
				if notify != nil {
					close(notify)
					notify = nil
				}
			}

			// now wait for it (but also detect close)
			select {
			case <-s.closeCh:
				break OUTER
			case <-notifyUs:
				// woken up, next loop should pick up work
			}
		}
	}
	s.asyncTasks.Done()
}

func (s *Scorch) persistSnapshot(snapshot *IndexSnapshot) error {
	// start a write transaction
	tx, err := s.rootBolt.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}()

	snapshotsBucket, err := tx.CreateBucketIfNotExists(boltSnapshotsBucket)
	if err != nil {
		return err
	}
	newSnapshotKey := segment.EncodeUvarintAscending(nil, snapshot.epoch)
	snapshotBucket, err := snapshotsBucket.CreateBucketIfNotExists(newSnapshotKey)
	if err != nil {
		return err
	}

	// persist internal values
	internalBucket, err := snapshotBucket.CreateBucketIfNotExists(boltInternalKey)
	if err != nil {
		return err
	}
	// TODO optimize writing these in order?
	for k, v := range snapshot.internal {
		err = internalBucket.Put([]byte(k), v)
		if err != nil {
			return err
		}
	}

	newSegmentPaths := make(map[uint64]string)

	// first ensure that each segment in this snapshot has been persisted
	for i, segmentSnapshot := range snapshot.segment {
		snapshotSegmentKey := segment.EncodeUvarintAscending(nil, uint64(i))
		snapshotSegmentBucket, err2 := snapshotBucket.CreateBucketIfNotExists(snapshotSegmentKey)
		if err2 != nil {
			return err2
		}
		switch seg := segmentSnapshot.segment.(type) {
		case *mem.Segment:
			// need to persist this to disk
			filename := fmt.Sprintf("%x.zap", segmentSnapshot.id)
			path := s.path + string(os.PathSeparator) + filename
			err2 := zap.PersistSegment(seg, path, 1024)
			if err2 != nil {
				return fmt.Errorf("error persisting segment: %v", err2)
			}
			newSegmentPaths[segmentSnapshot.id] = path
			err = snapshotSegmentBucket.Put(boltPathKey, []byte(filename))
			if err != nil {
				return err
			}
		case *zap.Segment:
			path := seg.Path()
			filename := strings.TrimPrefix(path, s.path+string(os.PathSeparator))
			err = snapshotSegmentBucket.Put(boltPathKey, []byte(filename))
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown segment type: %T", seg)
		}
		// store current deleted bits
		var roaringBuf bytes.Buffer
		if segmentSnapshot.deleted != nil {
			_, err = segmentSnapshot.deleted.WriteTo(&roaringBuf)
			if err != nil {
				return fmt.Errorf("error persisting roaring bytes: %v", err)
			}
			err = snapshotSegmentBucket.Put(boltDeletedKey, roaringBuf.Bytes())
			if err != nil {
				return err
			}
		}
	}

	// now try to open all the new snapshots
	newSegments := make(map[uint64]segment.Segment)
	for segmentID, path := range newSegmentPaths {
		newSegments[segmentID], err = zap.Open(path)
		if err != nil {
			return fmt.Errorf("error opening new segment at %s, %v", path, err)
		}
	}

	// get write lock and update the current snapshot with disk-based versions
	var notifications []chan error

	s.rootLock.Lock()
	newIndexSnapshot := &IndexSnapshot{
		epoch:    s.root.epoch,
		segment:  make([]*SegmentSnapshot, len(s.root.segment)),
		offsets:  make([]uint64, len(s.root.offsets)),
		internal: make(map[string][]byte, len(s.root.internal)),
	}
	for i, segmentSnapshot := range s.root.segment {
		// see if this segment has been replaced
		if replacement, ok := newSegments[segmentSnapshot.id]; ok {
			newSegmentSnapshot := &SegmentSnapshot{
				segment: replacement,
				deleted: segmentSnapshot.deleted,
				id:      segmentSnapshot.id,
			}
			newIndexSnapshot.segment[i] = newSegmentSnapshot
			// add the old segment snapshots notifications to the list
			for _, notification := range segmentSnapshot.notify {
				notifications = append(notifications, notification)
			}
		} else {
			newIndexSnapshot.segment[i] = s.root.segment[i]
		}
		newIndexSnapshot.offsets[i] = s.root.offsets[i]
	}
	for k, v := range s.root.internal {
		newIndexSnapshot.internal[k] = v
	}
	s.root = newIndexSnapshot
	s.rootLock.Unlock()

	// now that we've given up the lock, notify everyone that we've safely
	// persisted their data
	for _, notification := range notifications {
		close(notification)
	}

	return nil
}

// bolt snapshot code

var boltSnapshotsBucket = []byte{'s'}
var boltPathKey = []byte{'p'}
var boltDeletedKey = []byte{'d'}
var boltInternalKey = []byte{'i'}

func (s *Scorch) loadFromBolt() error {
	return s.rootBolt.View(func(tx *bolt.Tx) error {
		snapshots := tx.Bucket(boltSnapshotsBucket)
		if snapshots == nil {
			return nil
		}
		c := snapshots.Cursor()
		for k, _ := c.Last(); k != nil; k, _ = c.Prev() {
			_, snapshotEpoch, err := segment.DecodeUvarintAscending(k)
			if err != nil {
				log.Printf("unable to parse segment epoch % x, contiuing", k)
				continue
			}
			snapshot := snapshots.Bucket(k)
			if snapshot == nil {
				log.Printf("snapshot key, but bucket missing % x, continuing", k)
				continue
			}
			indexSnapshot, err := s.loadSnapshot(snapshot)
			if err != nil {
				log.Printf("unable to load snapshot, %v continuing", err)
				continue
			}
			indexSnapshot.epoch = snapshotEpoch
			// set the nextSegmentID
			for _, segment := range indexSnapshot.segment {
				if segment.id > s.nextSegmentID {
					s.nextSegmentID = segment.id
				}
			}
			s.nextSegmentID++
			s.nextSnapshotEpoch = snapshotEpoch + 1
			s.root = indexSnapshot
			break
		}
		return nil
	})
}

func (s *Scorch) loadSnapshot(snapshot *bolt.Bucket) (*IndexSnapshot, error) {

	rv := &IndexSnapshot{
		internal: make(map[string][]byte),
	}
	var running uint64
	c := snapshot.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if k[0] == boltInternalKey[0] {
			internalBucket := snapshot.Bucket(k)
			err := internalBucket.ForEach(func(key []byte, val []byte) error {
				copiedVal := append([]byte(nil), val...)
				rv.internal[string(key)] = copiedVal
				return nil
			})
			if err != nil {
				return nil, err
			}
		} else {
			segmentBucket := snapshot.Bucket(k)
			if segmentBucket == nil {
				return nil, fmt.Errorf("segment key, but bucket missing % x", k)
			}
			segmentSnapshot, err := s.loadSegment(segmentBucket)
			if err != nil {
				return nil, fmt.Errorf("failed to load segment: %v", err)
			}
			_, segmentSnapshot.id, err = segment.DecodeUvarintAscending(k)
			if err != nil {
				return nil, fmt.Errorf("failed to decode segment id: %v", err)
			}
			rv.segment = append(rv.segment, segmentSnapshot)
			rv.offsets = append(rv.offsets, running)
			running += segmentSnapshot.segment.Count()
		}
	}
	return rv, nil
}

func (s *Scorch) loadSegment(segmentBucket *bolt.Bucket) (*SegmentSnapshot, error) {
	pathBytes := segmentBucket.Get(boltPathKey)
	if pathBytes == nil {
		return nil, fmt.Errorf("segment path missing")
	}
	segmentPath := s.path + string(os.PathSeparator) + string(pathBytes)
	segment, err := zap.Open(segmentPath)
	if err != nil {
		return nil, fmt.Errorf("error opening bolt segment: %v", err)
	}

	rv := &SegmentSnapshot{
		segment: segment,
	}
	deletedBytes := segmentBucket.Get(boltDeletedKey)
	if deletedBytes != nil {
		deletedBitmap := roaring.NewBitmap()
		r := bytes.NewReader(deletedBytes)
		_, err := deletedBitmap.ReadFrom(r)
		if err != nil {
			return nil, fmt.Errorf("error reading deleted bytes: %v", err)
		}
		rv.deleted = deletedBitmap
	}

	return rv, nil
}
