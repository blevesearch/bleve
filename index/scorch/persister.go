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
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/index/scorch/segment"
	"github.com/blevesearch/bleve/index/scorch/segment/mem"
	"github.com/blevesearch/bleve/index/scorch/segment/zap"
	"github.com/boltdb/bolt"
)

type notificationChan chan struct{}

func (s *Scorch) persisterLoop() {
	s.removeOldData(true)

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
			ourSnapshot.AddRef()
			s.rootLock.RUnlock()

			//for ourSnapshot.epoch != lastPersistedEpoch {
			if ourSnapshot.epoch != lastPersistedEpoch {
				// lets get started
				err := s.persistSnapshot(ourSnapshot)
				if err != nil {
					log.Printf("got err persisting snapshot: %v", err)
					_ = ourSnapshot.DecRef()
					continue OUTER
				}
				lastPersistedEpoch = ourSnapshot.epoch
				if notify != nil {
					close(notify)
					notify = nil
				}
			}
			_ = ourSnapshot.DecRef()

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
			ourSnapshot.AddRef()
			s.rootLock.RUnlock()

			if ourSnapshot.epoch != lastPersistedEpoch {
				// lets get started
				err := s.persistSnapshot(ourSnapshot)
				if err != nil {
					log.Printf("got err persisting snapshot: %v", err)
					_ = ourSnapshot.DecRef()
					continue OUTER
				}
				lastPersistedEpoch = ourSnapshot.epoch
				if notify != nil {
					close(notify)
					notify = nil
				}
			}
			_ = ourSnapshot.DecRef()

			// now wait for it (but also detect close)
			select {
			case <-s.closeCh:
				break OUTER
			case <-notifyUs:
				// woken up, next loop should pick up work
			}
		}
		s.removeOldData(false)
	}
	s.asyncTasks.Done()
}

func (s *Scorch) persistSnapshot(snapshot *IndexSnapshot) error {
	// start a write transaction
	tx, err := s.rootBolt.Begin(true)
	if err != nil {
		return err
	}
	// defer fsync of the rootbolt
	defer func() {
		if err == nil {
			err = s.rootBolt.Sync()
		}
	}()
	// defer commit/rollback transaction
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
			for _, s := range newSegments {
				_ = s.Close() // cleanup segments that were successfully opened
			}
			return fmt.Errorf("error opening new segment at %s, %v", path, err)
		}
	}

	// get write lock and update the current snapshot with disk-based versions
	var notifications []chan error

	s.rootLock.Lock()
	newIndexSnapshot := &IndexSnapshot{
		parent:   s,
		epoch:    s.root.epoch,
		segment:  make([]*SegmentSnapshot, len(s.root.segment)),
		offsets:  make([]uint64, len(s.root.offsets)),
		internal: make(map[string][]byte, len(s.root.internal)),
		refs:     1,
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
			newIndexSnapshot.segment[i].segment.AddRef()
		}
		newIndexSnapshot.offsets[i] = s.root.offsets[i]
	}
	for k, v := range s.root.internal {
		newIndexSnapshot.internal[k] = v
	}
	rootPrev := s.root
	s.root = newIndexSnapshot
	s.rootLock.Unlock()

	if rootPrev != nil {
		_ = rootPrev.DecRef()
	}

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
		foundRoot := false
		c := snapshots.Cursor()
		for k, _ := c.Last(); k != nil; k, _ = c.Prev() {
			_, snapshotEpoch, err := segment.DecodeUvarintAscending(k)
			if err != nil {
				log.Printf("unable to parse segment epoch %x, continuing", k)
				continue
			}
			if foundRoot {
				s.eligibleForRemoval = append(s.eligibleForRemoval, snapshotEpoch)
				continue
			}
			snapshot := snapshots.Bucket(k)
			if snapshot == nil {
				log.Printf("snapshot key, but bucket missing %x, continuing", k)
				s.eligibleForRemoval = append(s.eligibleForRemoval, snapshotEpoch)
				continue
			}
			indexSnapshot, err := s.loadSnapshot(snapshot)
			if err != nil {
				log.Printf("unable to load snapshot, %v, continuing", err)
				s.eligibleForRemoval = append(s.eligibleForRemoval, snapshotEpoch)
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
			if s.root != nil {
				_ = s.root.DecRef()
			}
			s.root = indexSnapshot
			foundRoot = true
		}
		return nil
	})
}

func (s *Scorch) loadSnapshot(snapshot *bolt.Bucket) (*IndexSnapshot, error) {

	rv := &IndexSnapshot{
		parent:   s,
		internal: make(map[string][]byte),
		refs:     1,
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
				_ = rv.DecRef()
				return nil, err
			}
		} else {
			segmentBucket := snapshot.Bucket(k)
			if segmentBucket == nil {
				_ = rv.DecRef()
				return nil, fmt.Errorf("segment key, but bucket missing % x", k)
			}
			segmentSnapshot, err := s.loadSegment(segmentBucket)
			if err != nil {
				_ = rv.DecRef()
				return nil, fmt.Errorf("failed to load segment: %v", err)
			}
			_, segmentSnapshot.id, err = segment.DecodeUvarintAscending(k)
			if err != nil {
				_ = rv.DecRef()
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
			_ = segment.Close()
			return nil, fmt.Errorf("error reading deleted bytes: %v", err)
		}
		rv.deleted = deletedBitmap
	}

	return rv, nil
}

type uint64Descending []uint64

func (p uint64Descending) Len() int           { return len(p) }
func (p uint64Descending) Less(i, j int) bool { return p[i] > p[j] }
func (p uint64Descending) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (s *Scorch) removeOldData(force bool) {
	removed, err := s.removeOldBoltSnapshots()
	if err != nil {
		log.Printf("got err removing old bolt snapshots: %v", err)
	}

	if force || removed > 0 {
		err = s.removeOldZapFiles()
		if err != nil {
			log.Printf("go err removing old zap files: %v", err)
		}
	}
}

// NumSnapshotsToKeep represents how many recent, old snapshots to
// keep around per Scorch instance.  Useful for apps that require
// rollback'ability.
var NumSnapshotsToKeep int

// Removes enough snapshots from the rootBolt so that the
// s.eligibleForRemoval stays under the NumSnapshotsToKeep policy.
func (s *Scorch) removeOldBoltSnapshots() (numRemoved int, err error) {
	var epochsToRemove []uint64

	s.rootLock.Lock()
	if len(s.eligibleForRemoval) > NumSnapshotsToKeep {
		sort.Sort(uint64Descending(s.eligibleForRemoval))
		epochsToRemove = append([]uint64(nil), s.eligibleForRemoval[NumSnapshotsToKeep:]...) // Copy.
		s.eligibleForRemoval = s.eligibleForRemoval[0:NumSnapshotsToKeep]
	}
	s.rootLock.Unlock()

	if len(epochsToRemove) <= 0 {
		return 0, nil
	}

	tx, err := s.rootBolt.Begin(true)
	if err != nil {
		return 0, err
	}
	defer func() {
		if err == nil {
			err = s.rootBolt.Sync()
		}
	}()
	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
	}()

	for _, epochToRemove := range epochsToRemove {
		k := segment.EncodeUvarintAscending(nil, epochToRemove)
		err = tx.DeleteBucket(k)
		if err == bolt.ErrBucketNotFound {
			err = nil
		}
		if err == nil {
			numRemoved++
		}
	}

	return numRemoved, err
}

// Removes any *.zap files which aren't listed in the rootBolt.
func (s *Scorch) removeOldZapFiles() error {
	liveFileNames, err := s.loadZapFileNames()
	if err != nil {
		return err
	}

	currFileInfos, err := ioutil.ReadDir(s.path)
	if err != nil {
		return err
	}

	for _, finfo := range currFileInfos {
		fname := finfo.Name()
		if filepath.Ext(fname) == ".zap" {
			if _, exists := liveFileNames[fname]; !exists {
				err := os.Remove(s.path + string(os.PathSeparator) + fname)
				if err != nil {
					log.Printf("got err removing file: %s, err: %v", fname, err)
				}
			}
		}
	}

	return nil
}

// Returns the *.zap file names that are listed in the rootBolt.
func (s *Scorch) loadZapFileNames() (map[string]struct{}, error) {
	rv := map[string]struct{}{}
	err := s.rootBolt.View(func(tx *bolt.Tx) error {
		snapshots := tx.Bucket(boltSnapshotsBucket)
		if snapshots == nil {
			return nil
		}
		sc := snapshots.Cursor()
		for sk, _ := sc.First(); sk != nil; sk, _ = sc.Next() {
			snapshot := snapshots.Bucket(sk)
			if snapshot == nil {
				continue
			}
			segc := snapshot.Cursor()
			for segk, _ := segc.First(); segk != nil; segk, _ = segc.Next() {
				if segk[0] == boltInternalKey[0] {
					continue
				}
				segmentBucket := snapshot.Bucket(segk)
				if segmentBucket == nil {
					continue
				}
				pathBytes := segmentBucket.Get(boltPathKey)
				if pathBytes == nil {
					continue
				}
				rv[string(pathBytes)] = struct{}{}
			}
		}
		return nil
	})

	return rv, err
}
