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
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/document"
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

func TestGetProtectedSnapshots(t *testing.T) {
	origRollbackSamplingInterval := RollbackSamplingInterval
	defer func() {
		RollbackSamplingInterval = origRollbackSamplingInterval
	}()
	RollbackSamplingInterval = 10 * time.Minute
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
				{epoch: 99, timeStamp: currentTimeStamp.Add(-(RollbackSamplingInterval / 12))},
				{epoch: 88, timeStamp: currentTimeStamp.Add(-(RollbackSamplingInterval / 6))},
				{epoch: 50, timeStamp: currentTimeStamp.Add(-(RollbackSamplingInterval))},
				{epoch: 35, timeStamp: currentTimeStamp.Add(-(6 * RollbackSamplingInterval / 5))},
				{epoch: 10, timeStamp: currentTimeStamp.Add(-(2 * RollbackSamplingInterval))},
			},
			numSnapshotsToKeep: 3,
			expCount:           3,
			expEpochs:          []uint64{100, 50, 10},
		},
		{
			title: "epochs that have exact timestamps as per expectation for protecting",
			metaData: []*snapshotMetaData{
				{epoch: 100, timeStamp: currentTimeStamp},
				{epoch: 99, timeStamp: currentTimeStamp.Add(-(RollbackSamplingInterval / 12))},
				{epoch: 88, timeStamp: currentTimeStamp.Add(-(RollbackSamplingInterval / 6))},
				{epoch: 50, timeStamp: currentTimeStamp.Add(-(RollbackSamplingInterval))},
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
				{epoch: 99, timeStamp: currentTimeStamp.Add(-(RollbackSamplingInterval / 12))},
				{epoch: 88, timeStamp: currentTimeStamp.Add(-(RollbackSamplingInterval / 6))},
				{epoch: 50, timeStamp: currentTimeStamp.Add(-(3 * RollbackSamplingInterval / 4))},
				{epoch: 35, timeStamp: currentTimeStamp.Add(-(6 * RollbackSamplingInterval / 5))},
				{epoch: 10, timeStamp: currentTimeStamp.Add(-(2 * RollbackSamplingInterval))},
			},
			numSnapshotsToKeep: 3,
			expCount:           3,
			expEpochs:          []uint64{100, 35, 10},
		},
		{
			title: "protecting epochs when we don't have enough snapshots with RollbackSamplingInterval" +
				" separated timestamps",
			metaData: []*snapshotMetaData{
				{epoch: 100, timeStamp: currentTimeStamp},
				{epoch: 99, timeStamp: currentTimeStamp.Add(-(RollbackSamplingInterval / 12))},
				{epoch: 88, timeStamp: currentTimeStamp.Add(-(RollbackSamplingInterval / 6))},
				{epoch: 50, timeStamp: currentTimeStamp.Add(-(3 * RollbackSamplingInterval / 4))},
				{epoch: 35, timeStamp: currentTimeStamp.Add(-(5 * RollbackSamplingInterval / 6))},
				{epoch: 10, timeStamp: currentTimeStamp.Add(-(7 * RollbackSamplingInterval / 8))},
			},
			numSnapshotsToKeep: 4,
			expCount:           4,
			expEpochs:          []uint64{100, 99, 88, 10},
		},
		{
			title: "epochs of which some are approximated to the expected timestamps, and" +
				" we don't have enough snapshots with RollbackSamplingInterval separated timestamps",
			metaData: []*snapshotMetaData{
				{epoch: 100, timeStamp: currentTimeStamp},
				{epoch: 99, timeStamp: currentTimeStamp.Add(-(RollbackSamplingInterval / 12))},
				{epoch: 88, timeStamp: currentTimeStamp.Add(-(RollbackSamplingInterval / 6))},
				{epoch: 50, timeStamp: currentTimeStamp.Add(-(3 * RollbackSamplingInterval / 4))},
				{epoch: 35, timeStamp: currentTimeStamp.Add(-(8 * RollbackSamplingInterval / 7))},
				{epoch: 10, timeStamp: currentTimeStamp.Add(-(6 * RollbackSamplingInterval / 5))},
			},
			numSnapshotsToKeep: 3,
			expCount:           3,
			expEpochs:          []uint64{100, 50, 10},
		},
	}

	for i, test := range tests {
		protectedEpochs := getProtectedSnapshots(RollbackSamplingInterval,
			test.numSnapshotsToKeep, test.metaData)
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
func TestLatestSnapshotCheckpointed(t *testing.T) {

	cfg := CreateConfig("TestLatestSnapshotCheckpointed")
	numSnapshotsToKeepOrig := NumSnapshotsToKeep
	NumSnapshotsToKeep = 3
	RollbackSamplingInterval = 10 * time.Second
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

	// disable merger and purger
	RegistryEventCallbacks["test"] = func(e Event) bool {
		if e.Kind == EventKindPreMergeCheck {
			return false
		} else if e.Kind == EventKindPurgerCheck {
			return false
		}
		return true
	}

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}

	scorchy, ok := idx.(*Scorch)
	if !ok {
		t.Fatalf("Not a scorch index?")
	}

	err = scorchy.Open()
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

	err = scorchy.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	persistedSnapshots, err := scorchy.rootBoltSnapshotMetaData()
	if err != nil {
		t.Fatal(err)
	}

	if len(persistedSnapshots) != 1 {
		t.Fatalf("expected 1 persisted snapshot, got %d", len(persistedSnapshots))
	}

	time.Sleep(4 * RollbackSamplingInterval / 5)
	// create a batch, insert 2 new documents
	batch = index.NewBatch()
	doc = document.NewDocument("3")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test1")))
	batch.Update(doc)
	doc = document.NewDocument("4")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test2")))
	batch.Update(doc)

	err = scorchy.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(9 * RollbackSamplingInterval / 20)
	batch = index.NewBatch()
	doc = document.NewDocument("5")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test1")))
	batch.Update(doc)
	doc = document.NewDocument("6")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test2")))
	batch.Update(doc)

	err = scorchy.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(7 * RollbackSamplingInterval / 12)

	batch = index.NewBatch()
	doc = document.NewDocument("7")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test1")))
	batch.Update(doc)
	doc = document.NewDocument("8")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test2")))
	batch.Update(doc)

	err = scorchy.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * RollbackSamplingInterval / 12)
	batch = index.NewBatch()
	doc = document.NewDocument("9")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test1")))
	batch.Update(doc)
	doc = document.NewDocument("10")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test2")))
	batch.Update(doc)

	err = scorchy.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	batch = index.NewBatch()
	doc = document.NewDocument("11")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test1")))
	batch.Update(doc)
	doc = document.NewDocument("12")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test2")))
	batch.Update(doc)

	err = scorchy.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	persistedSnapshots, err = scorchy.rootBoltSnapshotMetaData()
	if err != nil {
		t.Fatal(err)
	}

	protectedSnapshots := getProtectedSnapshots(RollbackSamplingInterval, NumSnapshotsToKeep, persistedSnapshots)

	if len(protectedSnapshots) != 3 {
		t.Fatalf("expected %d protected snapshots, got %d", NumSnapshotsToKeep, len(protectedSnapshots))
	}

	if _, ok := protectedSnapshots[persistedSnapshots[0].epoch]; !ok {
		t.Fatalf("expected %d to be protected, but not found", persistedSnapshots[0].epoch)
	}
}
