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

	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/index/scorch/segment"
)

type segmentIntroduction struct {
	id        uint64
	data      segment.Segment
	obsoletes map[uint64]*roaring.Bitmap
	ids       []string
	internal  map[string][]byte

	applied   chan error
	persisted chan error
}

func (s *Scorch) mainLoop() {
	var notify notificationChan
OUTER:
	for {
		select {
		case <-s.closeCh:
			break OUTER

		case notify = <-s.introducerNotifier:
			continue

		case nextMerge := <-s.merges:
			s.introduceMerge(nextMerge)

		case next := <-s.introductions:
			err := s.introduceSegment(next)
			if err != nil {
				continue OUTER
			}
		}
		// notify persister
		if notify != nil {
			close(notify)
			notify = nil
		}
	}

	s.asyncTasks.Done()
}

func (s *Scorch) introduceSegment(next *segmentIntroduction) error {
	// acquire lock
	s.rootLock.Lock()

	nsegs := len(s.root.segment)

	// prepare new index snapshot
	newSnapshot := &IndexSnapshot{
		parent:   s,
		segment:  make([]*SegmentSnapshot, nsegs, nsegs+1),
		offsets:  make([]uint64, nsegs, nsegs+1),
		internal: make(map[string][]byte, len(s.root.internal)),
		epoch:    s.nextSnapshotEpoch,
		refs:     1,
	}
	s.nextSnapshotEpoch++

	// iterate through current segments
	var running uint64
	for i := range s.root.segment {
		// see if optimistic work included this segment
		delta, ok := next.obsoletes[s.root.segment[i].id]
		if !ok {
			var err error
			delta, err = s.root.segment[i].segment.DocNumbers(next.ids)
			if err != nil {
				next.applied <- fmt.Errorf("error computing doc numbers: %v", err)
				close(next.applied)
				_ = newSnapshot.DecRef()
				return err
			}
		}
		newSnapshot.segment[i] = &SegmentSnapshot{
			id:         s.root.segment[i].id,
			segment:    s.root.segment[i].segment,
			persisted:  s.root.segment[i].persisted,
			cachedDocs: s.root.segment[i].cachedDocs,
		}
		s.root.segment[i].segment.AddRef()
		// apply new obsoletions
		if s.root.segment[i].deleted == nil {
			newSnapshot.segment[i].deleted = delta
		} else {
			newSnapshot.segment[i].deleted = roaring.Or(s.root.segment[i].deleted, delta)
		}

		newSnapshot.offsets[i] = running
		running += s.root.segment[i].Count()

	}
	// append new segment, if any, to end of the new index snapshot
	if next.data != nil {
		newSnapshot.segment = append(newSnapshot.segment, &SegmentSnapshot{
			id:         next.id,
			segment:    next.data, // take ownership of next.data's ref-count
			cachedDocs: &cachedDocs{cache: nil},
		})
		newSnapshot.offsets = append(newSnapshot.offsets, running)
		if next.persisted != nil {
			newSnapshot.segment[nsegs].persisted =
				append(newSnapshot.segment[nsegs].persisted, next.persisted)
		}
	} else { // new segment might be nil when it's an internal data update only
		if next.persisted != nil {
			newSnapshot.persisted = append(newSnapshot.persisted, next.persisted)
		}
	}
	// copy old values
	for key, oldVal := range s.root.internal {
		newSnapshot.internal[key] = oldVal
	}
	// set new values and apply deletes
	for key, newVal := range next.internal {
		if newVal != nil {
			newSnapshot.internal[key] = newVal
		} else {
			delete(newSnapshot.internal, key)
		}
	}
	// swap in new segment
	rootPrev := s.root
	s.root = newSnapshot
	// release lock
	s.rootLock.Unlock()

	if rootPrev != nil {
		_ = rootPrev.DecRef()
	}

	close(next.applied)

	return nil
}

func (s *Scorch) introduceMerge(nextMerge *segmentMerge) {
	// acquire lock
	s.rootLock.Lock()

	// prepare new index snapshot
	currSize := len(s.root.segment)
	newSize := currSize + 1 - len(nextMerge.old)
	newSnapshot := &IndexSnapshot{
		parent:   s,
		segment:  make([]*SegmentSnapshot, 0, newSize),
		offsets:  make([]uint64, 0, newSize),
		internal: s.root.internal,
		epoch:    s.nextSnapshotEpoch,
		refs:     1,
	}
	s.nextSnapshotEpoch++

	// iterate through current segments
	newSegmentDeleted := roaring.NewBitmap()
	var running uint64
	for i := range s.root.segment {
		segmentID := s.root.segment[i].id
		if segSnapAtMerge, ok := nextMerge.old[segmentID]; ok {
			// this segment is going away, see if anything else was deleted since we started the merge
			if s.root.segment[i].deleted != nil {
				// assume all these deletes are new
				deletedSince := s.root.segment[i].deleted
				// if we already knew about some of them, remove
				if segSnapAtMerge.deleted != nil {
					deletedSince = roaring.AndNot(s.root.segment[i].deleted, segSnapAtMerge.deleted)
				}
				deletedSinceItr := deletedSince.Iterator()
				for deletedSinceItr.HasNext() {
					oldDocNum := deletedSinceItr.Next()
					newDocNum := nextMerge.oldNewDocNums[segmentID][oldDocNum]
					newSegmentDeleted.Add(uint32(newDocNum))
				}
			}
		} else {
			// this segment is staying
			newSnapshot.segment = append(newSnapshot.segment, &SegmentSnapshot{
				id:         s.root.segment[i].id,
				segment:    s.root.segment[i].segment,
				deleted:    s.root.segment[i].deleted,
				persisted:  s.root.segment[i].persisted,
				cachedDocs: s.root.segment[i].cachedDocs,
			})
			s.root.segment[i].segment.AddRef()
			newSnapshot.offsets = append(newSnapshot.offsets, running)
			running += s.root.segment[i].Count()
		}
	}

	// put new segment at end
	newSnapshot.segment = append(newSnapshot.segment, &SegmentSnapshot{
		id:         nextMerge.id,
		segment:    nextMerge.new, // take ownership for nextMerge.new's ref-count
		deleted:    newSegmentDeleted,
		cachedDocs: &cachedDocs{cache: nil},
	})
	newSnapshot.offsets = append(newSnapshot.offsets, running)

	// swap in new segment
	rootPrev := s.root
	s.root = newSnapshot
	// release lock
	s.rootLock.Unlock()

	if rootPrev != nil {
		_ = rootPrev.DecRef()
	}

	// notify merger we incorporated this
	close(nextMerge.notify)
}
