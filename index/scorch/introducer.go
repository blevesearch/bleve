package scorch

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/blevesearch/bleve/index/scorch/segment"
)

type segmentIntroduction struct {
	id        uint64
	data      segment.Segment
	obsoletes map[uint64]*roaring.Bitmap
	ids       []string
	internal  map[string][]byte

	applied chan struct{}
}

func (s *Scorch) mainLoop() {
	for {
		select {
		case <-s.closeCh:
			return

		case next := <-s.introductions:

			// acquire lock
			s.rootLock.Lock()

			// prepare new index snapshot, with curr size + 1
			newSnapshot := &IndexSnapshot{
				segment:  make([]*SegmentSnapshot, len(s.root.segment)+1),
				offsets:  make([]uint64, len(s.root.segment)+1),
				internal: make(map[string][]byte, len(s.root.segment)),
			}

			// iterate through current segments
			var running uint64
			for i := range s.root.segment {
				// see if optimistic work included this segment
				delta, ok := next.obsoletes[s.root.segment[i].id]
				if !ok {
					delta = s.root.segment[i].segment.DocNumbers(next.ids)
				}
				newSnapshot.segment[i] = &SegmentSnapshot{
					id:      s.root.segment[i].id,
					segment: s.root.segment[i].segment,
				}
				// apply new obsoletions
				if s.root.segment[i].deleted == nil {
					newSnapshot.segment[i].deleted = delta
				} else {
					newSnapshot.segment[i].deleted = s.root.segment[i].deleted.Clone()
					newSnapshot.segment[i].deleted.Or(delta)
				}

				newSnapshot.offsets[i] = running
				running += s.root.segment[i].Count()

			}
			// put new segment at end
			newSnapshot.segment[len(s.root.segment)] = &SegmentSnapshot{
				id:      next.id,
				segment: next.data,
			}
			newSnapshot.offsets[len(s.root.segment)] = running
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
		}
	}
}
