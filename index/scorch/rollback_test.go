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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
)

func TestIndexRollback(t *testing.T) {
	cfg := CreateConfig("TestIndexRollback")
	numSnapshotsToKeepOrig := NumSnapshotsToKeep
	NumSnapshotsToKeep = 1000

	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		NumSnapshotsToKeep = numSnapshotsToKeepOrig

		err := DestroyTest(cfg)
		if err != nil {
			t.Log(err)
		}
	}()

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}

	_, ok := idx.(*Scorch)
	if !ok {
		t.Fatalf("Not a scorch index?")
	}

	indexPath, _ := cfg["path"].(string)
	// should have no rollback points initially
	rollbackPoints, err := RollbackPoints(indexPath)
	if err == nil {
		t.Fatalf("expected no err, got: %v, %d", err, len(rollbackPoints))
	}
	if len(rollbackPoints) != 0 {
		t.Fatalf("expected no rollbackPoints, got %d", len(rollbackPoints))
	}

	err = idx.Open()
	if err != nil {
		t.Fatal(err)
	}
	// create a batch, insert 2 new documents
	batch := index.NewBatch()
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test1")))
	batch.Update(doc)
	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test2")))
	batch.Update(doc)

	err = idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	readerSlow, err := idx.Reader() // keep snapshot around so it's not cleaned up
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = readerSlow.Close()
	}()

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	// fetch rollback points after first batch
	rollbackPoints, err = RollbackPoints(indexPath)
	if err != nil {
		t.Fatalf("expected no err, got: %v, %d", err, len(rollbackPoints))
	}
	if len(rollbackPoints) == 0 {
		t.Fatalf("expected some rollbackPoints, got none")
	}

	// set this as a rollback point for the future
	rollbackPoint := rollbackPoints[0]

	err = idx.Open()
	if err != nil {
		t.Fatal(err)
	}
	// create another batch, insert 2 new documents, and delete an existing one
	batch = index.NewBatch()
	doc = document.NewDocument("3")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test3")))
	batch.Update(doc)
	doc = document.NewDocument("4")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test4")))
	batch.Update(doc)
	batch.Delete("1")

	err = idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	rollbackPointsB, err := RollbackPoints(indexPath)
	if err != nil || len(rollbackPointsB) <= len(rollbackPoints) {
		t.Fatalf("expected no err, got: %v, %d", err, len(rollbackPointsB))
	}

	found := false
	for _, p := range rollbackPointsB {
		if rollbackPoint.epoch == p.epoch {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected rollbackPoint epoch to still be available")
	}

	err = idx.Open()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}

	docCount, err := reader.DocCount()
	if err != nil {
		t.Fatal(err)
	}

	// expect docs 2, 3, 4
	if docCount != 3 {
		t.Fatalf("unexpected doc count: %v", docCount)
	}
	ret, err := reader.Document("1")
	if err != nil || ret != nil {
		t.Fatal(ret, err)
	}
	ret, err = reader.Document("2")
	if err != nil || ret == nil {
		t.Fatal(ret, err)
	}
	ret, err = reader.Document("3")
	if err != nil || ret == nil {
		t.Fatal(ret, err)
	}
	ret, err = reader.Document("4")
	if err != nil || ret == nil {
		t.Fatal(ret, err)
	}

	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	// rollback to a non existing rollback point
	err = Rollback(indexPath, &RollbackPoint{epoch: 100})
	if err == nil {
		t.Fatalf("expected err: Rollback: target epoch 100 not found in bolt")
	}

	// rollback to the selected rollback point
	err = Rollback(indexPath, rollbackPoint)
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Open()
	if err != nil {
		t.Fatal(err)
	}

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}

	docCount, err = reader.DocCount()
	if err != nil {
		t.Fatal(err)
	}

	// expect only docs 1, 2
	if docCount != 2 {
		t.Fatalf("unexpected doc count: %v", docCount)
	}
	ret, err = reader.Document("1")
	if err != nil || ret == nil {
		t.Fatal(ret, err)
	}
	ret, err = reader.Document("2")
	if err != nil || ret == nil {
		t.Fatal(ret, err)
	}
	ret, err = reader.Document("3")
	if err != nil || ret != nil {
		t.Fatal(ret, err)
	}
	ret, err = reader.Document("4")
	if err != nil || ret != nil {
		t.Fatal(ret, err)
	}

	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func openTestBolt(t *testing.T, name string) *util.RootBoltImpl {
	t.Helper()
	cfg := CreateConfig(name)
	if err := InitTest(cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := DestroyTest(cfg); err != nil {
			t.Error(err)
		}
	})
	path := cfg["path"].(string)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	rootBolt, err := util.OpenBolt(filepath.Join(path, "root.bolt"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rootBolt.Close(); err != nil {
			t.Error(err)
		}
	})
	return rootBolt
}

func seedBoltSnapshots(t *testing.T, rootBolt *util.RootBoltImpl, snapshotTimes map[uint64]time.Time) {
	t.Helper()
	err := rootBolt.Update(func(tx *util.BoltTxImpl) error {
		snapshots, err := tx.CreateBucketIfNotExists(util.BoltSnapshotsBucket)
		if err != nil {
			return err
		}
		for epoch, timeStamp := range snapshotTimes {
			snapshot, err := snapshots.CreateBucketIfNotExists(
				encodeUvarintAscending(nil, epoch))
			if err != nil {
				return err
			}
			metaBucket, err := snapshot.CreateBucketIfNotExists(util.BoltMetaDataKey)
			if err != nil {
				return err
			}
			timeStampBinary, err := timeStamp.MarshalText()
			if err != nil {
				return err
			}
			err = metaBucket.Put(util.BoltMetaDataTimeStamp, timeStampBinary, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestGetProtectedSnapshots(t *testing.T) {
	rootBolt := openTestBolt(t, "TestGetProtectedSnapshots")
	interval := 10 * time.Minute
	s := &Scorch{
		rootBolt:                 rootBolt,
		rollbackSamplingInterval: interval,
	}
	currentTimeStamp := time.Now()
	tests := []struct {
		title              string
		metaData           []*snapshotMetaData
		numSnapshotsToKeep int
		expCount           int
		expEpochs          []uint64
	}{
		{
			title: "epochs that have exact timestamps as per expectation for protecting",
			metaData: []*snapshotMetaData{
				{epoch: 100, timeStamp: currentTimeStamp},
				{epoch: 99, timeStamp: currentTimeStamp.Add(-(interval / 12))},
				{epoch: 88, timeStamp: currentTimeStamp.Add(-(interval / 6))},
				{epoch: 50, timeStamp: currentTimeStamp.Add(-(interval))},
				{epoch: 35, timeStamp: currentTimeStamp.Add(-(6 * interval / 5))},
				{epoch: 10, timeStamp: currentTimeStamp.Add(-(2 * interval))},
			},
			numSnapshotsToKeep: 3,
			expCount:           3,
			expEpochs:          []uint64{100, 50, 10},
		},
		{
			title: "epochs that have exact timestamps as per expectation for protecting",
			metaData: []*snapshotMetaData{
				{epoch: 100, timeStamp: currentTimeStamp},
				{epoch: 99, timeStamp: currentTimeStamp.Add(-(interval / 12))},
				{epoch: 88, timeStamp: currentTimeStamp.Add(-(interval / 6))},
				{epoch: 50, timeStamp: currentTimeStamp.Add(-(interval))},
			},
			numSnapshotsToKeep: 2,
			expCount:           2,
			expEpochs:          []uint64{100, 50},
		},
		{
			title: "epochs that have timestamps approximated to the expected value, " +
				"always retain the latest one",
			metaData: []*snapshotMetaData{
				{epoch: 100, timeStamp: currentTimeStamp},
				{epoch: 99, timeStamp: currentTimeStamp.Add(-(interval / 12))},
				{epoch: 88, timeStamp: currentTimeStamp.Add(-(interval / 6))},
				{epoch: 50, timeStamp: currentTimeStamp.Add(-(3 * interval / 4))},
				{epoch: 35, timeStamp: currentTimeStamp.Add(-(6 * interval / 5))},
				{epoch: 10, timeStamp: currentTimeStamp.Add(-(2 * interval))},
			},
			numSnapshotsToKeep: 3,
			expCount:           3,
			expEpochs:          []uint64{100, 35, 10},
		},
		{
			title: "protecting epochs when we don't have enough snapshots with rollbackSamplingInterval" +
				" separated timestamps",
			metaData: []*snapshotMetaData{
				{epoch: 100, timeStamp: currentTimeStamp},
				{epoch: 99, timeStamp: currentTimeStamp.Add(-(interval / 12))},
				{epoch: 88, timeStamp: currentTimeStamp.Add(-(interval / 6))},
				{epoch: 50, timeStamp: currentTimeStamp.Add(-(3 * interval / 4))},
				{epoch: 35, timeStamp: currentTimeStamp.Add(-(5 * interval / 6))},
				{epoch: 10, timeStamp: currentTimeStamp.Add(-(7 * interval / 8))},
			},
			numSnapshotsToKeep: 4,
			expCount:           4,
			expEpochs:          []uint64{100, 99, 88, 10},
		},
		{
			title: "epochs of which some are approximated to the expected timestamps, and" +
				" we don't have enough snapshots with rollbackSamplingInterval separated timestamps",
			metaData: []*snapshotMetaData{
				{epoch: 100, timeStamp: currentTimeStamp},
				{epoch: 99, timeStamp: currentTimeStamp.Add(-(interval / 12))},
				{epoch: 88, timeStamp: currentTimeStamp.Add(-(interval / 6))},
				{epoch: 50, timeStamp: currentTimeStamp.Add(-(3 * interval / 4))},
				{epoch: 35, timeStamp: currentTimeStamp.Add(-(8 * interval / 7))},
				{epoch: 10, timeStamp: currentTimeStamp.Add(-(6 * interval / 5))},
			},
			numSnapshotsToKeep: 3,
			expCount:           3,
			expEpochs:          []uint64{100, 50, 10},
		},
	}

	for i, test := range tests {
		s.numSnapshotsToKeep = test.numSnapshotsToKeep
		protectedEpochs := s.getProtectedSnapshots(test.metaData)
		if len(protectedEpochs) != test.expCount {
			t.Errorf("%d test: %s, getProtectedSnapshots expected to return %d "+
				"snapshots, but got: %d", i, test.title, test.expCount, len(protectedEpochs))
		}
		for _, e := range test.expEpochs {
			if _, found := protectedEpochs[e]; !found {
				t.Errorf("%d test: %s, %d epoch expected to be protected, "+
					"but missing from protected list: %v", i, test.title, e, protectedEpochs)
			}
		}
	}
}

func TestLatestSnapshotProtected(t *testing.T) {
	rootBolt := openTestBolt(t, "TestLatestSnapshotProtected")
	s := &Scorch{
		rootBolt:                 rootBolt,
		rollbackSamplingInterval: 10 * time.Minute,
		numSnapshotsToKeep:       3,
	}
	// seed snapshots with the following timestamps:
	// 	tc, tc - d/12, tc - d/6, tc - 3d/4, tc - 5d/6, tc - 6d/5
	// where d is the rollback sampling interval. the latest snapshot is only
	// d/12 newer than the one before it, so it does not fit into the sampled
	// time series anchored at the oldest snapshot, and must be retained by
	// the fallback that always protects the latest snapshot.
	d := s.rollbackSamplingInterval
	tc := time.Now()
	seedBoltSnapshots(t, rootBolt, map[uint64]time.Time{
		100: tc,
		99:  tc.Add(-(d / 12)),
		88:  tc.Add(-(d / 6)),
		50:  tc.Add(-(3 * d / 4)),
		35:  tc.Add(-(5 * d / 6)),
		10:  tc.Add(-(6 * d / 5)),
	})
	liveSnapshots, err := s.getLiveSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(liveSnapshots) != 6 {
		t.Fatalf("expected 6 live snapshots, got %d", len(liveSnapshots))
	}
	for i, epoch := range []uint64{100, 99, 88, 50, 35, 10} {
		if liveSnapshots[i].epoch != epoch {
			t.Fatalf("expected epoch %d at index %d, got %d", epoch, i,
				liveSnapshots[i].epoch)
		}
	}
	protectedSnapshots := s.getProtectedSnapshots(liveSnapshots)
	if len(protectedSnapshots) != s.numSnapshotsToKeep {
		t.Fatalf("expected %d protected snapshots, got %d",
			s.numSnapshotsToKeep, len(protectedSnapshots))
	}
	for _, epoch := range []uint64{100, 50, 10} {
		if _, found := protectedSnapshots[epoch]; !found {
			t.Fatalf("expected epoch %d to be protected, got %v", epoch, protectedSnapshots)
		}
	}
}

func TestBackupRacingWithPurge(t *testing.T) {
	rootBolt := openTestBolt(t, "TestBackupRacingWithPurge")
	s := &Scorch{
		rootBolt:                 rootBolt,
		rollbackSamplingInterval: 10 * time.Minute,
		numSnapshotsToKeep:       3,
	}
	d := s.rollbackSamplingInterval
	tc := time.Now()
	seedBoltSnapshots(t, rootBolt, map[uint64]time.Time{
		100: tc,
		99:  tc.Add(-(d / 12)),
		88:  tc.Add(-(d / 6)),
		50:  tc.Add(-(3 * d / 4)),
		35:  tc.Add(-(5 * d / 6)),
		10:  tc.Add(-(6 * d / 5)),
	})

	// even if every epoch were marked eligible for removal while a backup
	// reads from the latest snapshot, the purge must never delete the latest
	// snapshot from bolt - else the backup's CopyTo would fail
	s.eligibleForRemoval = []uint64{100, 99, 88, 50, 35, 10}

	if _, err := s.removeOldBoltSnapshots(); err != nil {
		t.Fatal(err)
	}

	remaining, err := s.RootBoltSnapshotEpochs()
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) == 0 || remaining[0] != 100 {
		t.Fatalf("expected the latest snapshot (epoch 100) to survive the "+
			"purge, got remaining epochs %v", remaining)
	}
	expected := []uint64{100, 50, 10} // newest -> oldest
	if len(remaining) != len(expected) {
		t.Fatalf("expected snapshot epochs %v to remain in bolt, got %v",
			expected, remaining)
	}
	for i, epoch := range expected {
		if remaining[i] != epoch {
			t.Fatalf("expected snapshot epochs %v to remain in bolt, got %v",
				expected, remaining)
		}
	}
}

func TestSparseMutationCheckpointing(t *testing.T) {
	rootBolt := openTestBolt(t, "TestSparseMutationCheckpointing")
	s := &Scorch{
		rootBolt:                 rootBolt,
		rollbackSamplingInterval: 10 * time.Minute,
		numSnapshotsToKeep:       3,
		rollbackRetentionFactor:  0.5,
	}
	// snapshots persisted at rollbackSamplingInterval (d) intervals, followed
	// by a single new snapshot after a long (3d) mutation gap - so every
	// older snapshot now falls outside the (numSnapshotsToKeep-1) * d
	// expiration window
	d := s.rollbackSamplingInterval
	tc := time.Now()
	seedBoltSnapshots(t, rootBolt, map[uint64]time.Time{
		9: tc,
		7: tc.Add(-(3 * d)),
		5: tc.Add(-(4 * d)),
		3: tc.Add(-(5 * d)),
		1: tc.Add(-(6 * d)),
	})
	// checkpoints computed while epochs 7, 5 and 3 were still protected; the
	// retention factor extends the expiration boundary to the middle
	// checkpoint (epoch 5), so checkpoints newer than it survive the gap
	s.checkPoints = []*snapshotMetaData{
		{epoch: 7, timeStamp: tc.Add(-(3 * d))},
		{epoch: 5, timeStamp: tc.Add(-(4 * d))},
		{epoch: 3, timeStamp: tc.Add(-(5 * d))},
	}
	liveSnapshots, err := s.getLiveSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	expectedLiveEpochs := []uint64{9, 7}
	if len(liveSnapshots) != len(expectedLiveEpochs) {
		t.Fatalf("expected %d live snapshots, got %d", len(expectedLiveEpochs),
			len(liveSnapshots))
	}
	for i, epoch := range expectedLiveEpochs {
		if liveSnapshots[i].epoch != epoch {
			t.Fatalf("expected epoch %d at index %d, got %d", epoch, i,
				liveSnapshots[i].epoch)
		}
	}
	// the latest snapshot (epoch 9) is always live and the cutoff time
	// based on the retention factor is epoch 5, so epochs 7 and 9 are protected
	protectedSnapshots := s.getProtectedSnapshots(liveSnapshots)
	if len(protectedSnapshots) <= 1 {
		t.Fatalf("expected more than 1 protected snapshot, got %d",
			len(protectedSnapshots))
	}
	for _, epoch := range []uint64{9, 7} {
		if _, ok := protectedSnapshots[epoch]; !ok {
			t.Fatalf("expected epoch %d to be protected, got %v",
				epoch, protectedSnapshots)
		}
	}
}

func TestLatestSnapshotRetentionWindow(t *testing.T) {
	rootBolt := openTestBolt(t, "TestLatestSnapshotRetentionWindow")
	s := &Scorch{
		rootBolt:                 rootBolt,
		rollbackSamplingInterval: 10 * time.Minute,
		numSnapshotsToKeep:       3,
		rollbackRetentionFactor:  0.5,
	}
	d := s.rollbackSamplingInterval
	tc := time.Now()
	seedBoltSnapshots(t, rootBolt, map[uint64]time.Time{
		9: tc.Add(-(3 * d)),
		7: tc.Add(-(4 * d)),
		5: tc.Add(-(5 * d)),
		3: tc.Add(-(6 * d)),
		1: tc.Add(-(7 * d)),
	})
	s.checkPoints = []*snapshotMetaData{
		{epoch: 5, timeStamp: tc.Add(-(5 * d))},
		{epoch: 3, timeStamp: tc.Add(-(6 * d))},
		{epoch: 1, timeStamp: tc.Add(-(7 * d))},
	}
	liveSnapshots, err := s.getLiveSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	expectedLiveEpochs := []uint64{9, 7, 5}
	if len(liveSnapshots) != len(expectedLiveEpochs) {
		t.Fatalf("expected %d live snapshots, got %d", len(expectedLiveEpochs),
			len(liveSnapshots))
	}
	for i, epoch := range expectedLiveEpochs {
		if liveSnapshots[i].epoch != epoch {
			t.Fatalf("expected epoch %d at index %d, got %d", epoch, i,
				liveSnapshots[i].epoch)
		}
	}
}
