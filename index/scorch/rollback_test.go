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
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
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
		{
			title: "sparse time series with a recent snapshot must still retain " +
				"numSnapshotsToKeep by filling the skipped middle snapshots",
			metaData: []*snapshotMetaData{
				{epoch: 100, timeStamp: currentTimeStamp},
				{epoch: 99, timeStamp: currentTimeStamp.Add(-(6 * interval / 10))},
				{epoch: 88, timeStamp: currentTimeStamp.Add(-(7 * interval / 10))},
				{epoch: 77, timeStamp: currentTimeStamp.Add(-(8 * interval / 10))},
				{epoch: 10, timeStamp: currentTimeStamp.Add(-(15 * interval / 10))},
			},
			numSnapshotsToKeep: 4,
			expCount:           4,
			expEpochs:          []uint64{100, 99, 88, 10},
		},
		{
			title: "latest snapshot exactly one interval from the oldest must " +
				"still retain numSnapshotsToKeep",
			metaData: []*snapshotMetaData{
				{epoch: 100, timeStamp: currentTimeStamp},
				{epoch: 99, timeStamp: currentTimeStamp.Add(-(interval / 2))},
				{epoch: 10, timeStamp: currentTimeStamp.Add(-(interval))},
			},
			numSnapshotsToKeep: 3,
			expCount:           3,
			expEpochs:          []uint64{100, 99, 10},
		},
		{
			title: "numSnapshotsToKeep=2 with a boundary-spaced older snapshot must " +
				"protect exactly the latest and the oldest",
			metaData: []*snapshotMetaData{
				{epoch: 100, timeStamp: currentTimeStamp},
				{epoch: 99, timeStamp: currentTimeStamp.Add(-(interval / 12))},
				{epoch: 50, timeStamp: currentTimeStamp.Add(-(2 * interval))},
				{epoch: 10, timeStamp: currentTimeStamp.Add(-(3 * interval))},
			},
			numSnapshotsToKeep: 2,
			expCount:           2,
			expEpochs:          []uint64{100, 10},
		},
	}
	for i, test := range tests {
		s.numSnapshotsToKeep = test.numSnapshotsToKeep
		protectedEpochs := s.getProtectedSnapshots(test.metaData)
		if len(protectedEpochs) != test.expCount {
			t.Errorf("test %d: %s, getProtectedSnapshots expected to return %d "+
				"snapshots, but got: %d", i, test.title, test.expCount, len(protectedEpochs))
		}
		for _, e := range test.expEpochs {
			if _, found := protectedEpochs[e]; !found {
				t.Errorf("test %d: %s, %d epoch expected to be protected, "+
					"but missing from protected list: %v", i, test.title, e, protectedEpochs)
			}
		}
	}
}

// updateRoot creates a new snapshot, updates the root
// to point to it, and returns the new snapshot.
// if empty is true, then the new snapshot will be a delete-only
// snapshot, otherwise it will contain a single document.
func updateRoot(t *testing.T, s *Scorch, empty bool) *IndexSnapshot {
	t.Helper()
	var seg segment.Segment
	var ids []string
	var err error
	if !empty {
		doc := genDoc(t)
		docs := []index.Document{doc}
		ids = []string{doc.ID()}
		seg, _, err = s.segPlugin.New(docs)
		if err != nil {
			t.Fatalf("failed to create segment: %v", err)
		}
	} else {
		seg = nil
		ids = []string{strconv.Itoa(rand.Intn(1000000))}
	}
	intro := &segmentIntroduction{
		id:      atomic.AddUint64(&s.nextSegmentID, 1),
		data:    seg,
		ids:     ids,
		applied: make(chan error),
	}
	go func(intro *segmentIntroduction) {
		s.introduceSegment(intro)
	}(intro)
	err = <-intro.applied
	if err != nil {
		t.Fatalf("failed to apply segment introduction: %v", err)
	}
	return s.root
}

// persistToBolt persists the given snapshot to bolt,
// sets the timestamp for the snapshot in the bolt metadata and
// persists any unpersisted segments to the directory.
func persistToBolt(t *testing.T, snapshot *IndexSnapshot, s *Scorch, timeStamp time.Time) error {
	t.Helper()
	tx, err := s.rootBolt.Begin(true)
	if err != nil {
		return err
	}
	_, _, err = prepareBoltSnapshot(snapshot, tx, s.path, s.segPlugin, nil)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	snapshotsBucket, err := tx.CreateBucketIfNotExists(util.BoltSnapshotsBucket)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	newSnapshotKey := encodeUvarintAscending(nil, snapshot.epoch)
	snapshotBucket, err := snapshotsBucket.CreateBucketIfNotExists(newSnapshotKey)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	metaBucket, err := snapshotBucket.CreateBucketIfNotExists(util.BoltMetaDataKey)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	timeStampBinary, err := timeStamp.MarshalText()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = metaBucket.Put(util.BoltMetaDataTimeStamp, timeStampBinary, nil)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	for _, segmentSnapshot := range snapshot.segment {
		if seg, ok := segmentSnapshot.segment.(segment.UnpersistedSegment); ok {
			filename := zapFileName(segmentSnapshot.id)
			path := filepath.Join(s.path, filename)
			err := persistToDirectory(seg, nil, path)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			persistedSeg, err := s.segPlugin.OpenUsing(path, s.segmentConfig)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
			segmentSnapshot.segment = persistedSeg
		}
	}
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	err = s.rootBolt.Sync()
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return nil
}

func TestLatestSnapshotProtected(t *testing.T) {
	cfg := CreateConfig("TestLatestSnapshotProtected")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = DestroyTest(cfg) }()
	dirPath := cfg["path"].(string)
	if err = os.Mkdir(dirPath, 0o755); err != nil {
		t.Fatal(err)
	}
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatalf("failed to create Scorch: %v", err)
	}
	s, ok := idx.(*Scorch)
	if !ok {
		t.Fatalf("expected *Scorch, got %T", s)
	}
	s.numSnapshotsToKeep = 3
	s.rollbackSamplingInterval = 10 * time.Second
	s.path = dirPath
	err = s.openBolt()
	if err != nil {
		t.Fatalf("failed to open bolt: %v", err)
	}
	// replicate the following scenario of persistence of snapshots
	// tc - 9d/5, tc - 6d/5, tc - 3d/4, tc - d/6, tc - d/12, tc
	// approximate timestamps where there's a chance that the latest snapshot
	// might not fit into the time-series and get purged along with its segment
	// files.
	const (
		numSnapshots = 6
	)
	curTime := time.Now()
	d := s.rollbackSamplingInterval
	// old --> new
	timeStamps := []time.Time{
		curTime.Add(-(9 * d / 5)),
		curTime.Add(-(6 * d / 5)),
		curTime.Add(-(3 * d / 4)),
		curTime.Add(-(d / 6)),
		curTime.Add(-(d / 12)),
		curTime,
	}
	snapshots := make([]*IndexSnapshot, numSnapshots)
	for i := 0; i < numSnapshots; i++ {
		snapshots[i] = updateRoot(t, s, false)
		err = persistToBolt(t, snapshots[i], s, timeStamps[i])
		if err != nil {
			t.Fatalf("failed to persist snapshot %d: %v", i, err)
		}
	}
	meta, err := s.getLiveSnapshots()
	if err != nil {
		t.Fatalf("failed to get live snapshots: %v", err)
	}
	if len(meta) != numSnapshots {
		t.Fatalf("expected %d live snapshots, got %d", numSnapshots, len(meta))
	}
	for i, m := range meta {
		expectIdx := numSnapshots - i - 1
		expectedEpoch := snapshots[expectIdx].epoch
		if m.epoch != expectedEpoch {
			t.Fatalf("expected epoch %d, got %d", expectedEpoch, m.epoch)
		}
		expectedTimeStamp := timeStamps[expectIdx]
		if !m.timeStamp.Equal(expectedTimeStamp) {
			t.Fatalf("expected timestamp %v, got %v", expectedTimeStamp, m.timeStamp)
		}
	}
	protectedSnapshots := s.getProtectedSnapshots(meta)
	if len(protectedSnapshots) != s.numSnapshotsToKeep {
		t.Fatalf("expected %d protected snapshots, got %d", s.numSnapshotsToKeep, len(protectedSnapshots))
	}
	expectedProtectedEpochs := []uint64{6, 2, 1}
	for _, expectedEpoch := range expectedProtectedEpochs {
		if _, found := protectedSnapshots[expectedEpoch]; !found {
			t.Fatalf("expected epoch %d to be protected, but not found", expectedEpoch)
		}
	}
	err = s.Close()
	if err != nil {
		t.Fatalf("failed to close Scorch: %v", err)
	}
}

// testFSDirectory implements index.Directory by writing files under a
// directory on the local filesystem, mirroring bleve.FileSystemDirectory. It
// is used as the backup target for CopyTo.
type testFSDirectory string

func (d testFSDirectory) GetWriter(filePath string) (io.WriteCloser, error) {
	dir, file := filepath.Split(filePath)
	if dir != "" {
		if err := os.MkdirAll(filepath.Join(string(d), dir), os.ModePerm); err != nil {
			return nil, err
		}
	}
	return os.OpenFile(filepath.Join(string(d), dir, file),
		os.O_RDWR|os.O_CREATE, 0o600)
}

func TestBackupRacingWithPurge(t *testing.T) {
	cfg := CreateConfig("TestBackupRacingWithPurge")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = DestroyTest(cfg) }()
	dirPath := cfg["path"].(string)
	if err = os.Mkdir(dirPath, 0o755); err != nil {
		t.Fatal(err)
	}
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatalf("failed to create Scorch: %v", err)
	}
	s, ok := idx.(*Scorch)
	if !ok {
		t.Fatalf("expected *Scorch, got %T", s)
	}
	s.numSnapshotsToKeep = 3
	s.rollbackSamplingInterval = 10 * time.Second
	s.path = dirPath
	err = s.openBolt()
	if err != nil {
		t.Fatalf("failed to open bolt: %v", err)
	}
	// replicate the following scenario of persistence of snapshots
	// tc - 9d/5, tc - 6d/5, tc - 3d/4, tc - d/6, tc - d/12, tc
	// approximate timestamps where there's a chance that the latest snapshot
	// might not fit into the time-series and get purged along with its segment
	// files.
	const (
		numSnapshots = 6
	)
	curTime := time.Now()
	d := s.rollbackSamplingInterval
	// old --> new
	timeStamps := []time.Time{
		curTime.Add(-(9 * d / 5)),
		curTime.Add(-(6 * d / 5)),
		curTime.Add(-(3 * d / 4)),
		curTime.Add(-(d / 6)),
		curTime.Add(-(d / 12)),
		curTime,
	}
	for i := 0; i < numSnapshots; i++ {
		snapshot := updateRoot(t, s, false)
		if err = persistToBolt(t, snapshot, s, timeStamps[i]); err != nil {
			t.Fatalf("failed to persist snapshot %d: %v", i, err)
		}
	}
	// now update the root again, but with an empty snapshot,
	// so that the latest root now references a segment that
	// belongs to the snapshot which was just persisted to bolt.
	updateRoot(t, s, true)
	// now if the purge code is invoked, there's a possibility of the latest snapshot
	// being removed from bolt and the corresponding file segment getting cleaned up.
	s.removeOldData()
	// acquire the copy reader which will now refer to
	// the latest in-memory snapshot which was the
	// empty snapshot we just introduced.
	copyReader := s.CopyReader()
	defer func() { copyReader.CloseCopyReader() }()

	backupidxConfig := CreateConfig("backup-directory")
	err = InitTest(backupidxConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := DestroyTest(backupidxConfig)
		if err != nil {
			t.Log(err)
		}
	}()
	// if the latest snapshot was purged, the following will return error
	err = copyReader.CopyTo(testFSDirectory(backupidxConfig["path"].(string)))
	if err != nil {
		t.Fatalf("error copying the index: %v", err)
	}
	err = s.Close()
	if err != nil {
		t.Fatalf("failed to close Scorch: %v", err)
	}
}

func TestSparseMutationCheckpointing(t *testing.T) {
	cfg := CreateConfig("TestSparseMutationCheckpointing")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = DestroyTest(cfg) }()
	dirPath := cfg["path"].(string)
	if err = os.Mkdir(dirPath, 0o755); err != nil {
		t.Fatal(err)
	}

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatalf("failed to create Scorch: %v", err)
	}
	s, ok := idx.(*Scorch)
	if !ok {
		t.Fatalf("expected *Scorch, got %T", s)
	}
	s.numSnapshotsToKeep = 3
	s.rollbackSamplingInterval = 2 * time.Second
	s.rollbackRetentionFactor = 0.5
	s.path = dirPath
	err = s.openBolt()
	if err != nil {
		t.Fatalf("failed to open bolt: %v", err)
	}
	const (
		numSnapshots = 5
	)
	curTime := time.Now()
	d := s.rollbackSamplingInterval
	// old --> new
	timeStamps := []time.Time{
		curTime.Add(-(6 * d)),
		curTime.Add(-(5 * d)),
		curTime.Add(-(4 * d)),
		curTime.Add(-(3 * d)),
		curTime,
	}
	for i := 0; i < numSnapshots; i++ {
		snapshot := updateRoot(t, s, false)
		if err = persistToBolt(t, snapshot, s, timeStamps[i]); err != nil {
			t.Fatalf("failed to persist snapshot %d: %v", i, err)
		}
	}
	s.checkPoints = newCheckPoints(map[uint64]time.Time{
		2: timeStamps[1],
		3: timeStamps[2],
		4: timeStamps[3],
	})
	meta, err := s.getLiveSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) != s.numSnapshotsToKeep {
		t.Fatalf("expected %d live snapshots, got %d", s.numSnapshotsToKeep, len(meta))
	}
	for i, m := range meta {
		expectIdx := numSnapshots - i - 1
		expectedEpoch := uint64(expectIdx + 1)
		if m.epoch != expectedEpoch {
			t.Fatalf("expected epoch %d, got %d", expectedEpoch, m.epoch)
		}
		expectedTimeStamp := timeStamps[expectIdx]
		if !m.timeStamp.Equal(expectedTimeStamp) {
			t.Fatalf("expected timestamp %v, got %v", expectedTimeStamp, m.timeStamp)
		}
	}
	protectedSnapshots := s.getProtectedSnapshots(meta)
	if len(protectedSnapshots) != s.numSnapshotsToKeep {
		t.Fatalf("expected %d protected snapshots, got %d", s.numSnapshotsToKeep, len(protectedSnapshots))
	}
	expectedProtectedEpochs := []uint64{5, 4, 3}
	for _, expectedEpoch := range expectedProtectedEpochs {
		if _, found := protectedSnapshots[expectedEpoch]; !found {
			t.Fatalf("expected epoch %d to be protected, but not found", expectedEpoch)
		}
	}
	err = s.Close()
	if err != nil {
		t.Fatalf("failed to close Scorch: %v", err)
	}
}

func TestRollbackCheckpointsOnRestart(t *testing.T) {
	cfg := CreateConfig("TestRollbackCheckpointsOnRestart")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = DestroyTest(cfg) }()
	dirPath := cfg["path"].(string)
	if err = os.Mkdir(dirPath, 0o755); err != nil {
		t.Fatal(err)
	}
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatalf("failed to create Scorch: %v", err)
	}
	s, ok := idx.(*Scorch)
	if !ok {
		t.Fatalf("expected *Scorch, got %T", s)
	}
	s.numSnapshotsToKeep = 3
	s.rollbackSamplingInterval = 2 * time.Second
	s.rollbackRetentionFactor = 0.5
	s.path = dirPath
	err = s.openBolt()
	if err != nil {
		t.Fatalf("failed to open bolt: %v", err)
	}
	const (
		numSnapshots = 6
	)
	curTime := time.Now()
	d := s.rollbackSamplingInterval
	// old --> new
	timeStamps := []time.Time{
		curTime.Add(-(10 * d)),
		curTime.Add(-(9 * d)),
		curTime.Add(-(8 * d)),

		curTime.Add(-(2 * d)),
		curTime.Add(-(1 * d)),
		curTime,
	}
	for i := 0; i < 3; i++ {
		snapshot := updateRoot(t, s, false)
		if err = persistToBolt(t, snapshot, s, timeStamps[i]); err != nil {
			t.Fatalf("failed to persist snapshot %d: %v", i, err)
		}
	}
	err = s.Close()
	if err != nil {
		t.Fatalf("failed to close Scorch: %v", err)
	}
	idx, err = NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatalf("failed to create Scorch: %v", err)
	}
	s, ok = idx.(*Scorch)
	if !ok {
		t.Fatalf("expected *Scorch, got %T", s)
	}
	s.numSnapshotsToKeep = 3
	s.rollbackSamplingInterval = 2 * time.Second
	s.rollbackRetentionFactor = 0.5
	s.path = dirPath
	err = s.openBolt()
	if err != nil {
		t.Fatalf("failed to open bolt: %v", err)
	}
	for i := 3; i < numSnapshots; i++ {
		snapshot := updateRoot(t, s, false)
		if err = persistToBolt(t, snapshot, s, timeStamps[i]); err != nil {
			t.Fatalf("failed to persist snapshot %d: %v", i, err)
		}
	}
	meta, err := s.getLiveSnapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(meta) != 5 {
		t.Fatalf("expected %d live snapshots, got %d", 5, len(meta))
	}
	for i, m := range meta {
		expectIdx := numSnapshots - i - 1
		expectedEpoch := uint64(expectIdx + 1)
		if m.epoch != expectedEpoch {
			t.Fatalf("expected epoch %d, got %d", expectedEpoch, m.epoch)
		}
		expectedTimeStamp := timeStamps[expectIdx]
		if !m.timeStamp.Equal(expectedTimeStamp) {
			t.Fatalf("expected timestamp %v, got %v", expectedTimeStamp, m.timeStamp)
		}
	}
	protectedSnapshots := s.getProtectedSnapshots(meta)
	if len(protectedSnapshots) != s.numSnapshotsToKeep {
		t.Fatalf("expected %d protected snapshots, got %d", s.numSnapshotsToKeep, len(protectedSnapshots))
	}
	expectedProtectedEpochs := []uint64{6, 3, 2}
	for _, expectedEpoch := range expectedProtectedEpochs {
		if _, found := protectedSnapshots[expectedEpoch]; !found {
			t.Fatalf("expected epoch %d to be protected, but not found", expectedEpoch)
		}
	}
	err = s.Close()
	if err != nil {
		t.Fatalf("failed to close Scorch: %v", err)
	}
}

func TestRollackOptions(t *testing.T) {
	cfg := CreateConfig("TestRollackOptions")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = DestroyTest(cfg) }()
	dirPath := cfg["path"].(string)
	if err = os.Mkdir(dirPath, 0o755); err != nil {
		t.Fatal(err)
	}
	type testCase struct {
		cfg       map[string]interface{}
		expectErr bool
	}
	tests := []testCase{
		{
			cfg: map[string]interface{}{
				"numSnapshotsToKeep":       3,
				"rollbackSamplingInterval": "10m",
				"rollbackRetentionFactor":  0.5,
			},
			expectErr: false,
		},
		{
			cfg: map[string]interface{}{
				"numSnapshotsToKeep":       0.5,
				"rollbackSamplingInterval": "10a",
				"rollbackRetentionFactor":  1,
			},
			expectErr: true,
		},
		{
			cfg: map[string]interface{}{
				"numSnapshotsToKeep":       3.0,
				"rollbackSamplingInterval": 10,
				"rollbackRetentionFactor":  1.5,
			},
			expectErr: true,
		},
		{
			cfg: map[string]interface{}{
				"numSnapshotsToKeep":       3.0,
				"rollbackSamplingInterval": "10s",
				"rollbackRetentionFactor":  0,
			},
			expectErr: false,
		},
		{
			cfg: map[string]interface{}{
				"numSnapshotsToKeep":       "3.0",
				"rollbackSamplingInterval": "10s",
				"rollbackRetentionFactor":  2,
			},
			expectErr: true,
		},
		{
			cfg: map[string]interface{}{
				"numSnapshotsToKeep":       3.0,
				"rollbackSamplingInterval": "10s",
				"rollbackRetentionFactor":  "2",
			},
			expectErr: true,
		},
		{
			cfg: map[string]interface{}{
				"numSnapshotsToKeep":       3.0,
				"rollbackSamplingInterval": "10s",
				"rollbackRetentionFactor":  2,
			},
			expectErr: true,
		},
	}
	for i, test := range tests {
		test.cfg["path"] = dirPath
		s, err := NewScorch(Name, test.cfg, index.NewAnalysisQueue(1))
		gotErr := err != nil
		wantErr := test.expectErr
		if gotErr != wantErr {
			t.Fatalf("test %d: expected error: %v, got error: %v", i, wantErr, gotErr)
		}
		if err == nil {
			err = s.Close()
			if err != nil {
				t.Fatalf("test %d: failed to close Scorch: %v", i, err)
			}
		}
	}
}
