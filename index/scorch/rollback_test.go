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

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
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
	tests := []struct {
		title              string
		persistedEpochs    []uint64
		numSnapshotsToKeep int
		expCount           int
		expEpochs          []uint64
	}{
		{
			title:              "most recent epoch numbers, and remaining equidistant epochs",
			persistedEpochs:    []uint64{100, 99, 88, 77, 50, 25, 10},
			numSnapshotsToKeep: 3,
			expCount:           3,
			expEpochs:          []uint64{100, 88, 50},
		},
		{
			title:              "latest and the middle epochs remain protected",
			persistedEpochs:    []uint64{100, 99, 88, 77, 50, 25, 10},
			numSnapshotsToKeep: 2,
			expCount:           2,
			expEpochs:          []uint64{100, 77},
		},
		{
			title:              "only the latest remains protected",
			persistedEpochs:    []uint64{100, 50, 10},
			numSnapshotsToKeep: 1,
			expCount:           1,
			expEpochs:          []uint64{100},
		},
		{
			title:              "all epochs remains protected",
			persistedEpochs:    []uint64{100, 50, 10},
			numSnapshotsToKeep: 3,
			expCount:           3,
			expEpochs:          []uint64{100, 50, 10},
		},
		{
			title:              "epochs at an equidistant interval remains protected",
			persistedEpochs:    []uint64{1990, 1850, 1800, 1550, 1489, 1449, 1200, 1111, 1055, 921, 900, 850, 800, 555, 299, 199, 60, 29},
			numSnapshotsToKeep: 3,
			expCount:           3,
			expEpochs:          []uint64{1990, 1200, 800},
		},
		{
			title:              "epochs at an equidistant interval remains protected",
			persistedEpochs:    []uint64{1990, 1850, 1800, 1550, 1489, 1449},
			numSnapshotsToKeep: 3,
			expCount:           3,
			expEpochs:          []uint64{1990, 1800, 1489},
		},
		{
			title:              "epochs at an equidistant interval remains protected",
			persistedEpochs:    []uint64{1990, 1850, 1800, 1550, 1489, 1449},
			numSnapshotsToKeep: 5,
			expCount:           5,
			expEpochs:          []uint64{1990, 1850, 1800, 1550, 1489},
		},
	}

	for i, test := range tests {
		ps := getProtectedSnapshots(test.persistedEpochs, test.numSnapshotsToKeep)
		if len(ps) != test.expCount {
			t.Errorf("%d test: %s, getProtectedSnapshots expected to return %d "+
				"snapshots, but got: %d", i, test.title, test.expCount, len(ps))
		}
		for _, e := range test.expEpochs {
			if _, found := ps[e]; !found {
				t.Errorf("%d test: %s, %d epoch expected to be protected, "+
					"but missing from protected list: %v", i, test.title, e, ps)
			}
		}
	}

}
