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

	sh, ok := idx.(*Scorch)
	if !ok {
		t.Errorf("Not a scorch index?")
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
		t.Error(err)
	}

	// Get last persisted snapshot
	s1, err := sh.PreviousPersistedSnapshot(nil)
	if err != nil {
		t.Error(err)
	}

	// create another batch, insert 1 new document, and delete an existing one
	batch = index.NewBatch()
	doc = document.NewDocument("3")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test3")))
	batch.Update(doc)
	batch.Delete("1")

	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	// Get Last persisted snapshot
	s2, err := sh.PreviousPersistedSnapshot(nil)
	if err != nil {
		t.Error(err)
	}

	// the last persisted snapshot should not contain doc 1, but
	// should contain 2 and 3
	ret, err := s2.Document("1")
	if err != nil || ret != nil {
		t.Error(ret, err)
	}
	ret, err = s2.Document("2")
	if err != nil || ret == nil {
		t.Error(ret, err)
	}
	ret, err = s2.Document("3")
	if err != nil || ret == nil {
		t.Error(ret, err)
	}

	// revert to first persisted snapshot
	err = sh.SnapshotRevert(s1)
	if err != nil {
		t.Error(err)
	}

	// obtain the last persisted snapshot, after rollback
	latestSnapshot, err := sh.PreviousPersistedSnapshot(nil)
	if err != nil {
		t.Error(err)
	}

	// check that in the latest snapshot docs 1 and 2 are
	// available, but not 3
	ret, err = latestSnapshot.Document("1")
	if err != nil || ret == nil {
		t.Error(ret, err)
	}
	ret, err = latestSnapshot.Document("2")
	if err != nil || ret == nil {
		t.Error(ret, err)
	}
	ret, err = latestSnapshot.Document("3")
	if err != nil || ret != nil {
		t.Error(ret, err)
	}
}
