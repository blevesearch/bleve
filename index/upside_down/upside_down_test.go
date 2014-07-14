//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package upside_down

import (
	"os"
	"testing"

	_ "github.com/couchbaselabs/bleve/analysis/analyzers/standard_analyzer"
	"github.com/couchbaselabs/bleve/document"
	"github.com/couchbaselabs/bleve/index/store/gouchstore"
)

func TestIndexOpenReopen(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := gouchstore.Open("test")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}

	var expectedCount uint64 = 0
	docCount := idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	// opening database should have inserted version
	expectedLength := uint64(1)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}

	// now close it
	idx.Close()

	store, err = gouchstore.Open("test")
	idx = NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}

	// now close it
	idx.Close()
}

func TestIndexInsert(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := gouchstore.Open("test")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	var expectedCount uint64 = 0
	docCount := idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	docCount = idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	// should have 4 rows (1 for version, 1 for schema field, and 1 for single term, and 1 for the term count,  and 1 for the back index entry)
	expectedLength := uint64(1 + 1 + 1 + 1 + 1)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}
}

func TestIndexInsertThenDelete(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := gouchstore.Open("test")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	var expectedCount uint64 = 0
	docCount := idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	doc2 := document.NewDocument("2")
	doc2.AddField(document.NewTextField("name", []byte("test")))
	err = idx.Update(doc2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	docCount = idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	err = idx.Delete("1")
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}
	expectedCount -= 1

	docCount = idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	err = idx.Delete("2")
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}
	expectedCount -= 1

	docCount = idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	// should have 2 row (1 for version, 1 for schema field)
	expectedLength := uint64(1 + 1)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}
}

func TestIndexInsertThenUpdate(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := gouchstore.Open("test")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	// this update should overwrite one term, and introduce one new one
	doc = document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []byte("test fail")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}

	// should have 2 row (1 for version, 1 for schema field, and 2 for the two term, and 2 for the term counts, and 1 for the back index entry)
	expectedLength := uint64(1 + 1 + 2 + 2 + 1)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}

	// now do another update that should remove one of term
	doc = document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []byte("fail")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}

	// should have 2 row (1 for version, 1 for schema field, and 1 for the remaining term, and 1 for the term count, and 1 for the back index entry)
	expectedLength = uint64(1 + 1 + 1 + 1 + 1)
	rowCount = idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}
}

func TestIndexInsertMultiple(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := gouchstore.Open("test")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}

	var expectedCount uint64 = 0

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	// should have 4 rows (1 for version, 1 for schema field, and 2 for single term, and 1 for the term count,  and 2 for the back index entries)
	expectedLength := uint64(1 + 1 + 2 + 1 + 2)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}

	// close and reopen and and one more to testing counting works correctly
	idx.Close()
	store, err = gouchstore.Open("test")
	idx = NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}

	doc = document.NewDocument("3")
	doc.AddField(document.NewTextField("name", []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	docCount := idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("expected doc count: %d, got %d", expectedCount, docCount)
	}
}

func TestIndexInsertWithStore(t *testing.T) {
	defer os.RemoveAll("test")

	store, err := gouchstore.Open("test")
	idx := NewUpsideDownCouch(store)
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer idx.Close()

	var expectedCount uint64 = 0
	docCount := idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []byte("test"), document.INDEX_FIELD|document.STORE_FIELD))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount += 1

	docCount = idx.DocCount()
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	// should have 6 rows (1 for version, 1 for schema field, and 1 for single term, and 1 for the stored field and 1 for the term count,  and 1 for the back index entry)
	expectedLength := uint64(1 + 1 + 1 + 1 + 1 + 1)
	rowCount := idx.rowCount()
	if rowCount != expectedLength {
		t.Errorf("expected %d rows, got: %d", expectedLength, rowCount)
	}

	storedDoc, err := idx.Document("1")
	if err != nil {
		t.Error(err)
	}

	if len(storedDoc.Fields) != 1 {
		t.Errorf("expected 1 stored field, got %d", len(storedDoc.Fields))
	}
	if string(storedDoc.Fields[0].Value()) != "test" {
		t.Errorf("expected field content 'test', got '%s'", string(storedDoc.Fields[0].Value()))
	}
}
