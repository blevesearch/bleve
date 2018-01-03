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

	sh, ok := idx.(*Scorch)
	if !ok {
		t.Fatalf("Not a scorch index?")
	}

	// fetch rollback points available as of here
	rollbackPoints, err := sh.RollbackPoints()
	if err != nil || len(rollbackPoints) == 0 {
		t.Fatal(err, len(rollbackPoints))
	}

	// set this as a rollback point for the future
	rollbackPoint := rollbackPoints[0]

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

	// rollback to the selected rollback point
	err = sh.Rollback(rollbackPoint)
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
}
