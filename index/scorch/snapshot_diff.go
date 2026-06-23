//  Copyright (c) 2024 Couchbase, Inc.
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
	"encoding/json"
	"fmt"
	"sort"

	segment "github.com/blevesearch/scorch_segment_api/v2"
	bolt "go.etcd.io/bbolt"
)

// boltSnapshotDiffBucket is the top-level bucket in root.bolt that stores
// per-epoch diffs showing what changed between consecutive index snapshots.
var boltSnapshotDiffBucket = []byte{'x'}

// SnapshotDiff captures the docID-level changes between two consecutive
// IndexSnapshots. All fields are sorted docID slices.
type SnapshotDiff struct {
	Epoch    uint64   `json:"epoch"`
	Live     []string `json:"live"`
	Deleted  []string `json:"deleted"`
	Updated  []string `json:"updated"`
	Inserted []string `json:"inserted"`
}

// collectLiveDocIDs returns the sorted set of all live (non-deleted) docIDs
// in the given IndexSnapshot.
func collectLiveDocIDs(snapshot *IndexSnapshot) ([]string, error) {
	if snapshot == nil {
		return nil, nil
	}

	set := make(map[string]struct{})
	for _, seg := range snapshot.segment {
		liveDocs := seg.DocNumbersLive()
		iter := liveDocs.Iterator()
		for iter.HasNext() {
			localDocNum := iter.Next()
			docIDBytes, err := seg.DocID(uint64(localDocNum))
			if err != nil {
				return nil, fmt.Errorf("error reading docID from segment %d: %v",
					seg.id, err)
			}
			set[string(docIDBytes)] = struct{}{}
		}
	}

	result := make([]string, 0, len(set))
	for id := range set {
		result = append(result, id)
	}
	sort.Strings(result)
	return result, nil
}

// classifyBatchIDs classifies each batch docID as inserted, updated, or
// deleted.  oldLive is the previous snapshot's live docIDs.  newData is
// the new segment (nil means pure deletes).  Uses simple nested loops.
func classifyBatchIDs(ids []string, oldLive []string, newData segment.Segment) (
	inserted, updated, deleted []string,
) {
	for _, id := range ids {
		if newData == nil {
			deleted = append(deleted, id)
			continue
		}

		inNew := false
		bm, err := newData.DocNumbers([]string{id})
		if err == nil && !bm.IsEmpty() {
			inNew = true
		}

		inOld := false
		for _, oid := range oldLive {
			if oid == id {
				inOld = true
				break
			}
		}

		switch {
		case inNew && inOld:
			updated = append(updated, id)
		case inNew && !inOld:
			inserted = append(inserted, id)
		default:
			deleted = append(deleted, id)
		}
	}
	return inserted, updated, deleted
}

// buildDiffFromBatch constructs a SnapshotDiff using the per-batch
// classification and the previous snapshot's live set.
func buildDiffFromBatch(oldLive []string, newEpoch uint64,
	inserted, updated, deleted []string,
) *SnapshotDiff {
	newLiveSet := make(map[string]bool, len(oldLive)+len(inserted))
	for _, id := range oldLive {
		newLiveSet[id] = true
	}
	for _, id := range deleted {
		delete(newLiveSet, id)
	}
	for _, id := range inserted {
		newLiveSet[id] = true
	}

	live := make([]string, 0, len(newLiveSet))
	for id := range newLiveSet {
		live = append(live, id)
	}
	sort.Strings(live)

	sort.Strings(inserted)
	sort.Strings(updated)
	sort.Strings(deleted)

	return &SnapshotDiff{
		Epoch:    newEpoch,
		Live:     live,
		Deleted:  deleted,
		Updated:  updated,
		Inserted: inserted,
	}
}

// computeSnapshotDiff compares the docID sets of two snapshots and returns
// the diff.  Used for merge/persist introductions which have no batch IDs.
func computeSnapshotDiff(prevSnapshot, newSnapshot *IndexSnapshot) (*SnapshotDiff, error) {
	newLive, err := collectLiveDocIDs(newSnapshot)
	if err != nil {
		return nil, err
	}

	diff := &SnapshotDiff{
		Epoch:    newSnapshot.epoch,
		Live:     newLive,
		Deleted:  []string{},
		Updated:  []string{},
		Inserted: []string{},
	}

	if prevSnapshot == nil {
		diff.Inserted = newLive
		return diff, nil
	}

	oldLive, err := collectLiveDocIDs(prevSnapshot)
	if err != nil {
		return nil, err
	}

	oldSet := make(map[string]bool, len(oldLive))
	for _, id := range oldLive {
		oldSet[id] = true
	}

	newSet := make(map[string]bool, len(newLive))
	for _, id := range newLive {
		newSet[id] = true
	}

	var deleted, inserted, updated []string
	for _, id := range oldLive {
		if !newSet[id] {
			deleted = append(deleted, id)
		}
	}
	for _, id := range newLive {
		if !oldSet[id] {
			inserted = append(inserted, id)
		}
	}

	// Update detection is handled by classifyBatchIDs at batch time.
	// Merge/persist introductions are internal reorgs with no semantic
	// doc updates, so we leave updated empty here.

	sort.Strings(deleted)
	sort.Strings(inserted)
	sort.Strings(updated)

	diff.Deleted = deleted
	diff.Inserted = inserted
	diff.Updated = updated

	return diff, nil
}

// recordSnapshotDiff is used for merge/persist introductions which have no
// batch IDs.  It falls back to full snapshot set-comparison.
func (s *Scorch) recordSnapshotDiff(prevSnapshot, newSnapshot *IndexSnapshot) {
	diff, err := computeSnapshotDiff(prevSnapshot, newSnapshot)
	if err != nil {
		s.fireAsyncError(fmt.Errorf("snapshot diff compute error for epoch %d: %v",
			newSnapshot.epoch, err))
		return
	}
	s.diffLock.Lock()
	s.pendingDiffs = append(s.pendingDiffs, diff)
	s.diffLock.Unlock()
}

// recordSnapshotDiffWithBatch is used by introduceSegment.
func (s *Scorch) recordSnapshotDiffWithBatch(oldLive []string, newEpoch uint64,
	inserted, updated, deleted []string,
) {
	diff := buildDiffFromBatch(oldLive, newEpoch, inserted, updated, deleted)
	s.diffLock.Lock()
	s.pendingDiffs = append(s.pendingDiffs, diff)
	s.diffLock.Unlock()
}

// flushPendingDiffs writes all pending snapshot diffs into the given bolt
// transaction.  Called by the persister inside its bolt write tx.
func (s *Scorch) flushPendingDiffs(tx *bolt.Tx) error {
	s.diffLock.Lock()
	defer s.diffLock.Unlock()

	if len(s.pendingDiffs) == 0 {
		return nil
	}

	diffBucket, cerr := tx.CreateBucketIfNotExists(boltSnapshotDiffBucket)
	if cerr != nil {
		return cerr
	}

	for _, diff := range s.pendingDiffs {
		data, err := json.Marshal(diff)
		if err != nil {
			return fmt.Errorf("snapshot diff marshal error for epoch %d: %v",
				diff.Epoch, err)
		}
		key := encodeUvarintAscending(nil, diff.Epoch)
		if err := diffBucket.Put(key, data); err != nil {
			return err
		}
	}

	// Clear the slice in-place so the persister doesn't grab heap.
	clear(s.pendingDiffs)
	s.pendingDiffs = s.pendingDiffs[:0]
	return nil
}

// GetSnapshotDiff retrieves the diff for a specific epoch from rootBolt.
func (s *Scorch) GetSnapshotDiff(epoch uint64) (*SnapshotDiff, error) {
	if s.rootBolt == nil {
		return nil, fmt.Errorf("rootBolt is nil")
	}

	var diff SnapshotDiff
	err := s.rootBolt.View(func(tx *bolt.Tx) error {
		diffBucket := tx.Bucket(boltSnapshotDiffBucket)
		if diffBucket == nil {
			return fmt.Errorf("snapshot diff bucket not found")
		}
		key := encodeUvarintAscending(nil, epoch)
		data := diffBucket.Get(key)
		if data == nil {
			return fmt.Errorf("snapshot diff for epoch %d not found", epoch)
		}
		return json.Unmarshal(data, &diff)
	})
	if err != nil {
		return nil, err
	}
	return &diff, nil
}

// GetAllSnapshotDiffs returns all snapshot diffs from rootBolt, sorted by
// epoch ascending.
func (s *Scorch) GetAllSnapshotDiffs() ([]*SnapshotDiff, error) {
	if s.rootBolt == nil {
		return nil, fmt.Errorf("rootBolt is nil")
	}

	var diffs []*SnapshotDiff
	err := s.rootBolt.View(func(tx *bolt.Tx) error {
		diffBucket := tx.Bucket(boltSnapshotDiffBucket)
		if diffBucket == nil {
			return nil
		}
		c := diffBucket.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			_, epoch, derr := decodeUvarintAscending(k)
			if derr != nil {
				continue
			}
			var diff SnapshotDiff
			if jerr := json.Unmarshal(v, &diff); jerr != nil {
				continue
			}
			diff.Epoch = epoch
			diffs = append(diffs, &diff)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return diffs, nil
}
