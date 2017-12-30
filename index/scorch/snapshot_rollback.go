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
	"log"

	"github.com/blevesearch/bleve/index/scorch/segment"
)

// PreviousPersistedSnapshot returns the next older, previous
// IndexSnapshot based on the provided IndexSnapshot. If the provided
// argument is nil, the most recently persisted IndexSnapshot is returned.
// This API allows the application to walk backwards into the history
// of a store to previous points in time. A nil return value indicates
// that no previous snapshots are available.
func (s *Scorch) PreviousPersistedSnapshot(is *IndexSnapshot) (*IndexSnapshot, error) {
	if s.rootBolt == nil {
		return nil, nil
	}

	// start a read-only transaction
	tx, err := s.rootBolt.Begin(false)
	if err != nil {
		return nil, err
	}

	// Read-only bolt transactions to be rolled back.
	defer func() {
		_ = tx.Rollback()
	}()

	snapshots := tx.Bucket(boltSnapshotsBucket)
	if snapshots == nil {
		return nil, nil
	}

	pos := []byte(nil)

	if is != nil {
		pos = segment.EncodeUvarintAscending(nil, is.epoch)
	}

	c := snapshots.Cursor()
	for k, _ := c.Last(); k != nil; k, _ = c.Prev() {
		if pos == nil || bytes.Compare(k, pos) < 0 {
			_, snapshotEpoch, err := segment.DecodeUvarintAscending(k)
			if err != nil {
				log.Printf("PreviousPersistedSnapshot:"+
					" unable to parse segment epoch %x, continuing", k)
				continue
			}

			snapshot := snapshots.Bucket(k)
			if snapshot == nil {
				log.Printf("PreviousPersistedSnapshot:"+
					" snapshot key, but bucket missing %x, continuing", k)
				continue
			}

			indexSnapshot, err := s.loadSnapshot(snapshot)
			if err != nil {
				log.Printf("PreviousPersistedSnapshot:"+
					" unable to load snapshot, %v, continuing", err)
				continue
			}

			indexSnapshot.epoch = snapshotEpoch
			// Mark segments that are referenced by this indexSnapshot
			// as ineligible for removal.
			for _, segSnapshot := range indexSnapshot.segment {
				filename := zapFileName(segSnapshot.id)
				s.markIneligibleForRemoval(filename)
			}
			return indexSnapshot, nil
		}
	}

	return nil, nil
}

// SnapshotRevert atomically brings the store back to the point in time
// as represented by the revertTo IndexSnapshot. SnapshotRevert() should
// only be passed an IndexSnapshot that came from the same store.
func (s *Scorch) SnapshotRevert(revertTo *IndexSnapshot) error {
	revert := &snapshotReversion{
		snapshot: revertTo,
		applied:  make(chan error),
	}

	if !s.unsafeBatch {
		revert.persisted = make(chan error)
	}

	s.revertToSnapshots <- revert

	// block until this IndexSnapshot is applied
	err := <-revert.applied
	if err != nil {
		return err
	}

	if revert.persisted != nil {
		err = <-revert.persisted
	}

	return err
}
