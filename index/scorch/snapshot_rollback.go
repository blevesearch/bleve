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
	"fmt"
	"log"

	"github.com/blevesearch/bleve/index/scorch/segment"
)

type RollbackPoint struct {
	epoch uint64
	meta  map[string][]byte
}

func (r *RollbackPoint) GetInternal(key []byte) []byte {
	return r.meta[string(key)]
}

// RollbackPoints returns an array of rollback points available
// for the application to make a decision on where to rollback
// to. A nil return value indicates that there are no available
// rollback points.
func (s *Scorch) RollbackPoints() ([]*RollbackPoint, error) {
	if s.rootBolt == nil {
		return nil, fmt.Errorf("RollbackPoints: root is nil")
	}

	// start a read-only bolt transaction
	tx, err := s.rootBolt.Begin(false)
	if err != nil {
		return nil, fmt.Errorf("RollbackPoints: failed to start" +
			" read-only transaction")
	}

	// read-only bolt transactions to be rolled back
	defer func() {
		_ = tx.Rollback()
	}()

	snapshots := tx.Bucket(boltSnapshotsBucket)
	if snapshots == nil {
		return nil, fmt.Errorf("RollbackPoints: no snapshots available")
	}

	rollbackPoints := []*RollbackPoint{}

	c1 := snapshots.Cursor()
	for k, _ := c1.Last(); k != nil; k, _ = c1.Prev() {
		_, snapshotEpoch, err := segment.DecodeUvarintAscending(k)
		if err != nil {
			log.Printf("RollbackPoints:"+
				" unable to parse segment epoch %x, continuing", k)
			continue
		}

		snapshot := snapshots.Bucket(k)
		if snapshot == nil {
			log.Printf("RollbackPoints:"+
				" snapshot key, but bucket missing %x, continuing", k)
			continue
		}

		meta := map[string][]byte{}
		c2 := snapshot.Cursor()
		for j, _ := c2.First(); j != nil; j, _ = c2.Next() {
			if j[0] == boltInternalKey[0] {
				internalBucket := snapshot.Bucket(j)
				err = internalBucket.ForEach(func(key []byte, val []byte) error {
					copiedVal := append([]byte(nil), val...)
					meta[string(key)] = copiedVal
					return nil
				})
				if err != nil {
					break
				}
			}
		}

		if err != nil {
			log.Printf("RollbackPoints:"+
				" failed in fetching interal data: %v", err)
			continue
		}

		rollbackPoints = append(rollbackPoints, &RollbackPoint{
			epoch: snapshotEpoch,
			meta:  meta,
		})
	}

	return rollbackPoints, nil
}

// Rollback atomically and durably (if unsafeBatch is unset) brings
// the store back to the point in time as represented by the
// RollbackPoint. Rollbac() should only be passed a RollbackPoint
// that came from the same store using the RollbackPoints() API.
func (s *Scorch) Rollback(to *RollbackPoint) error {
	if to == nil {
		return fmt.Errorf("Rollback: RollbackPoint is nil")
	}

	if s.rootBolt == nil {
		return fmt.Errorf("Rollback: root is nil")
	}

	// start a read-only bolt transaction
	tx, err := s.rootBolt.Begin(false)
	if err != nil {
		return fmt.Errorf("Rollback: failed to start read-only transaction")
	}

	snapshots := tx.Bucket(boltSnapshotsBucket)
	if snapshots == nil {
		return fmt.Errorf("Rollback: no snapshots available")
	}

	pos := segment.EncodeUvarintAscending(nil, to.epoch)

	snapshot := snapshots.Bucket(pos)
	if snapshot == nil {
		return fmt.Errorf("Rollback: snapshot not found")
	}

	revertTo, err := s.loadSnapshot(snapshot)
	if err != nil {
		return fmt.Errorf("Rollback: unable to load snapshot: %v", err)
	}

	// add segments referenced by loaded index snapshot to the
	// ineligibleForRemoval map
	for _, segSnap := range revertTo.segment {
		filename := zapFileName(segSnap.id)
		s.markIneligibleForRemoval(filename)
	}

	// read-only bolt transactions to be rolled back
	_ = tx.Rollback()

	revert := &snapshotReversion{
		snapshot: revertTo,
		applied:  make(chan error),
	}

	if !s.unsafeBatch {
		revert.persisted = make(chan error)
	}

	s.revertToSnapshots <- revert

	// block until this snapshot is applied
	err = <-revert.applied
	if err != nil {
		return fmt.Errorf("Rollback: failed with err: %v", err)
	}

	if revert.persisted != nil {
		err = <-revert.persisted
	}

	return err
}
