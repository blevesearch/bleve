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

		case next := <-s.introductions:
			// acquire lock
			s.rootLock.Lock()

			// prepare new index snapshot, with curr size + 1
			newSnapshot := &IndexSnapshot{
				segment:  make([]*SegmentSnapshot, len(s.root.segment)+1),
				offsets:  make([]uint64, len(s.root.segment)+1),
				internal: make(map[string][]byte, len(s.root.segment)),
				epoch:    s.nextSnapshotEpoch,
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
						continue OUTER
					}
				}
				newSnapshot.segment[i] = &SegmentSnapshot{
					id:         s.root.segment[i].id,
					segment:    s.root.segment[i].segment,
					notify:     s.root.segment[i].notify,
					cachedDocs: s.root.segment[i].cachedDocs,
				}
				// apply new obsoletions
				if s.root.segment[i].deleted == nil {
					newSnapshot.segment[i].deleted = delta
				} else {
					newSnapshot.segment[i].deleted = roaring.Or(s.root.segment[i].deleted, delta)
				}

				newSnapshot.offsets[i] = running
				running += s.root.segment[i].Count()

			}
			// put new segment at end
			newSnapshot.segment[len(s.root.segment)] = &SegmentSnapshot{
				id:         next.id,
				segment:    next.data,
				cachedDocs: &cachedDocs{cache: nil},
			}
			newSnapshot.offsets[len(s.root.segment)] = running
			if !s.unsafeBatch {
				newSnapshot.segment[len(s.root.segment)].notify = append(
					newSnapshot.segment[len(s.root.segment)].notify,
					next.persisted,
				)
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
			s.root = newSnapshot
			// release lock
			s.rootLock.Unlock()
			close(next.applied)

			if notify != nil {
				close(notify)
				notify = nil
			}
		}
	}

	s.asyncTasks.Done()
}
