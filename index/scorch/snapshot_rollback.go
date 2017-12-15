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

type indexSnapErr struct {
	i *IndexSnapshot
	e error
}

// PreviousPersistedSnapshot returns the next older, previous
// IndexSnapshot based on the provided IndexSnapshot. If the provided
// argument were nil, the last persisted indexsnapshot is returned.
// This API allows the application to walk backwards into the history
// of a store to previous points in time. A nil return value indicates
// that no previous snapshots are available.
func (s *Scorch) PreviousPersistedSnapshot(is *IndexSnapshot) (*IndexSnapshot, error) {
	if s.rootBolt == nil {
		return nil, nil
	}

	ch := make(chan *indexSnapErr)

	go func() {
		// start a read-only transaction
		tx, err := s.rootBolt.Begin(false)
		if err != nil {
			ch <- &indexSnapErr{nil, err}
			return
		}

		snapshots := tx.Bucket(boltSnapshotsBucket)
		if snapshots == nil {
			ch <- &indexSnapErr{nil, nil}
			return
		}

		pos := []byte(nil)
		returnNextSnapshot := true

		if is != nil {
			pos = segment.EncodeUvarintAscending(nil, is.epoch)
			returnNextSnapshot = false
		}

		c := snapshots.Cursor()
		for k, _ := c.Last(); k != nil; k, _ = c.Prev() {
			if !returnNextSnapshot {
				comp := bytes.Compare(k, pos)
				if comp > 0 {
					// Entry not found yet.
					continue
				} else if comp == 0 {
					// Found the entry that is being searched for,
					// return a valid IndexSnapshot found in the next iteration.
					returnNextSnapshot = true
					continue
				} else { // comp < 0
					// Found an entry that is smaller than the one being searched for,
					// return a valid IndexSnapshot found right away.
					returnNextSnapshot = true
				}
			}

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
			ch <- &indexSnapErr{indexSnapshot, nil}
			return
		}

		ch <- &indexSnapErr{nil, nil}
	}()

	rv := <-ch

	return rv.i, rv.e
}

// SnapshotRevert atomically brings the store back to the point in time
// as represented by the revertTo IndexSnapshot. SnapshotRevert() should
// only be passed an IndexSnapshot that came from the same store.
func (s *Scorch) SnapshotRevert(revertTo *IndexSnapshot) error {
	revert := &snapshotReversion{
		snapshot: revertTo,
		applied:  make(chan error),
	}

	s.revertToSnapshots <- revert

	// block until this IndexSnapshot is applied
	err := <-revert.applied
	return err
}
