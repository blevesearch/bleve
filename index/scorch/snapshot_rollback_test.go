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
	defer func() {
		err := DestroyTest()
		if err != nil {
			t.Fatal(err)
		}
	}()

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, testConfig, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// create 2 docs
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test1")))
	err = idx.Update(doc)
	if err != nil {
		t.Error(err)
	}

	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test2")))
	err = idx.Update(doc)
	if err != nil {
		t.Error(err)
	}

	// create a batch, insert new doc, update existing doc, delete existing doc
	batch := index.NewBatch()
	doc = document.NewDocument("3")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test3")))
	batch.Update(doc)
	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test2updated")))
	batch.Update(doc)
	batch.Delete("1")

	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	sh, ok := idx.(*Scorch)
	if !ok {
		t.Errorf("Not a scorch index?")
	}

	// Get Last persisted snapshot
	ss, err := sh.PreviousPersistedSnapshot(nil)
	if err != nil {
		t.Error(err)
	}

	// Retrieve the snapshot earlier
	prev, err := sh.PreviousPersistedSnapshot(ss)
	if err != nil {
		t.Error(err)
	}

	if prev != nil {
		err = sh.SnapshotRevert(prev)
		if err != nil {
			t.Error(err)
		}

		newRoot, err := sh.PreviousPersistedSnapshot(nil)
		if err != nil {
			t.Error(err)
		}

		if newRoot == nil {
			t.Errorf("Failed to retrieve latest persisted snapshot")
		}

		if newRoot.epoch <= prev.epoch {
			t.Errorf("Unexpected epoch, %v <= %v", newRoot.epoch, prev.epoch)
		}
	}
}
