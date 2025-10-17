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
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
	"github.com/blevesearch/bleve/v2/analysis/analyzer/standard"
	regexpTokenizer "github.com/blevesearch/bleve/v2/analysis/tokenizer/regexp"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/index/scorch/mergeplan"
	"github.com/blevesearch/bleve/v2/mapping"
	index "github.com/blevesearch/bleve_index_api"
)

func init() {
	// override for tests
	DefaultPersisterNapTimeMSec = 1
}

func InitTest(cfg map[string]interface{}) error {
	return os.RemoveAll(cfg["path"].(string))
}

func DestroyTest(cfg map[string]interface{}) error {
	return os.RemoveAll(cfg["path"].(string))
}

func CreateConfig(name string) map[string]interface{} {
	// TODO: Use t.Name() when Go 1.7 support terminates.
	rv := make(map[string]interface{})
	rv["path"] = os.TempDir() + "/bleve-scorch-test-" + name
	return rv
}

var testAnalyzer = &analysis.DefaultAnalyzer{
	Tokenizer: regexpTokenizer.NewRegexpTokenizer(regexp.MustCompile(`\w+`)),
}

func TestIndexOpenReopen(t *testing.T) {
	cfg := CreateConfig("TestIndexOpenReopen")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// insert a doc
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now close it
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err = NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}

	// check the doc count again after reopening it
	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now close it
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexOpenReopenWithInsert(t *testing.T) {
	cfg := CreateConfig("TestIndexOpenReopen")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// insert a doc
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now close it
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	// try to open the index and insert data
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}

	// insert a doc
	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test2")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	// check the doc count again after reopening it
	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now close it
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsert(t *testing.T) {
	cfg := CreateConfig("TestIndexInsert")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsertThenDelete(t *testing.T) {
	cfg := CreateConfig("TestIndexInsertThenDelete")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc2 := document.NewDocument("2")
	doc2.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	iid, err := reader.InternalID("1")
	if err != nil || iid == nil {
		t.Errorf("unexpected on doc id 1")
	}
	iid, err = reader.InternalID("2")
	if err != nil || iid == nil {
		t.Errorf("unexpected on doc id 2")
	}
	iid, err = reader.InternalID("3")
	if err != nil || iid != nil {
		t.Errorf("unexpected on doc id 3")
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Delete("1")
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}
	expectedCount--

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	storedDoc, err := reader.Document("1")
	if err != nil {
		t.Error(err)
	}
	if storedDoc != nil {
		t.Errorf("expected nil for deleted stored doc #1, got %v", storedDoc)
	}
	storedDoc, err = reader.Document("2")
	if err != nil {
		t.Error(err)
	}
	if storedDoc == nil {
		t.Errorf("expected stored doc for #2, got nil")
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now close it
	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}

	idx, err = NewScorch(Name, cfg, analysisQueue) // reopen
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Errorf("error reopening index: %v", err)
	}

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	storedDoc, err = reader.Document("1")
	if err != nil {
		t.Error(err)
	}
	if storedDoc != nil {
		t.Errorf("expected nil for deleted stored doc #1, got %v", storedDoc)
	}
	storedDoc, err = reader.Document("2")
	if err != nil {
		t.Error(err)
	}
	if storedDoc == nil {
		t.Errorf("expected stored doc for #2, got nil")
	}
	iid, err = reader.InternalID("1")
	if err != nil || iid != nil {
		t.Errorf("unexpected on doc id 1")
	}
	iid, err = reader.InternalID("2")
	if err != nil || iid == nil {
		t.Errorf("unexpected on doc id 2, should exist")
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Delete("2")
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}
	expectedCount--

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	storedDoc, err = reader.Document("1")
	if err != nil {
		t.Error(err)
	}
	if storedDoc != nil {
		t.Errorf("expected nil for deleted stored doc #1, got %v", storedDoc)
	}
	storedDoc, err = reader.Document("2")
	if err != nil {
		t.Error(err)
	}
	if storedDoc != nil {
		t.Errorf("expected nil for deleted stored doc #2, got nil")
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsertThenUpdate(t *testing.T) {
	cfg := CreateConfig("TestIndexInsertThenUpdate")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	var expectedCount uint64
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

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	// this update should overwrite one term, and introduce one new one
	doc = document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithAnalyzer("name", []uint64{}, []byte("test fail"), testAnalyzer))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}

	// now do another update that should remove one of the terms
	doc = document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("fail")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsertMultiple(t *testing.T) {
	cfg := CreateConfig("TestIndexInsertMultiple")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	var expectedCount uint64

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = document.NewDocument("3")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsertWithStore(t *testing.T) {
	cfg := CreateConfig("TestIndexInsertWithStore")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), index.IndexField|index.StoreField))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	storedDocInt, err := indexReader.Document("1")
	if err != nil {
		t.Error(err)
	}

	storedDoc := storedDocInt.(*document.Document)

	if len(storedDoc.Fields) != 1 {
		t.Errorf("expected 1 stored field, got %d", len(storedDoc.Fields))
	}
	for _, field := range storedDoc.Fields {
		if field.Name() == "name" {
			textField, ok := field.(*document.TextField)
			if !ok {
				t.Errorf("expected text field")
			}
			if string(textField.Value()) != "test" {
				t.Errorf("expected field content 'test', got '%s'", string(textField.Value()))
			}
		} else if field.Name() == "_id" {
			t.Errorf("not expecting _id field")
		}
	}
}

func TestIndexInternalCRUD(t *testing.T) {
	cfg := CreateConfig("TestIndexInternalCRUD")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}

	if len(indexReader.(*IndexSnapshot).segment) != 0 {
		t.Errorf("expected 0 segments")
	}

	// get something that doesn't exist yet
	val, err := indexReader.GetInternal([]byte("key"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected nil, got %s", val)
	}

	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}

	// set
	err = idx.SetInternal([]byte("key"), []byte("abc"))
	if err != nil {
		t.Error(err)
	}

	indexReader2, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}

	if len(indexReader2.(*IndexSnapshot).segment) != 0 {
		t.Errorf("expected 0 segments")
	}

	// get
	val, err = indexReader2.GetInternal([]byte("key"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "abc" {
		t.Errorf("expected %s, got '%s'", "abc", val)
	}

	err = indexReader2.Close()
	if err != nil {
		t.Fatal(err)
	}

	// delete
	err = idx.DeleteInternal([]byte("key"))
	if err != nil {
		t.Error(err)
	}

	indexReader3, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}

	if len(indexReader3.(*IndexSnapshot).segment) != 0 {
		t.Errorf("expected 0 segments")
	}

	// get again
	val, err = indexReader3.GetInternal([]byte("key"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected nil, got %s", val)
	}

	err = indexReader3.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexBatch(t *testing.T) {
	cfg := CreateConfig("TestIndexBatch")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	var expectedCount uint64

	// first create 2 docs the old fashioned way
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test2")))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	// now create a batch which does 3 things
	// insert new doc
	// update existing doc
	// delete existing doc
	// net document count change 0

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

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	numSegments := len(indexReader.(*IndexSnapshot).segment)
	if numSegments <= 0 {
		t.Errorf("expected some segments, got: %d", numSegments)
	}

	docCount, err := indexReader.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	docIDReader, err := indexReader.DocIDReaderAll()
	if err != nil {
		t.Error(err)
	}
	var docIds []index.IndexInternalID
	docID, err := docIDReader.Next()
	for docID != nil && err == nil {
		docIds = append(docIds, docID)
		docID, err = docIDReader.Next()
	}
	if err != nil {
		t.Error(err)
	}
	externalDocIds := map[string]struct{}{}
	// convert back to external doc ids
	for _, id := range docIds {
		externalID, err := indexReader.ExternalID(id)
		if err != nil {
			t.Fatal(err)
		}
		externalDocIds[externalID] = struct{}{}
	}
	expectedDocIds := map[string]struct{}{
		"2": {},
		"3": {},
	}
	if !reflect.DeepEqual(externalDocIds, expectedDocIds) {
		t.Errorf("expected ids: %v, got ids: %v", expectedDocIds, externalDocIds)
	}
}

func TestIndexBatchWithCallbacks(t *testing.T) {
	cfg := CreateConfig("TestIndexBatchWithCallbacks")
	defer func() {
		err := DestroyTest(cfg)
		if err != nil {
			t.Fatal(err)
		}
	}()

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}
	defer func() {
		cerr := idx.Close()
		if cerr != nil {
			t.Fatal(cerr)
		}
	}()

	// Check that callback function works
	var wg sync.WaitGroup
	wg.Add(1)

	batch := index.NewBatch()
	doc := document.NewDocument("3")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test3")))
	batch.Update(doc)
	batch.SetPersistedCallback(func(e error) {
		wg.Done()
	})

	err = idx.Batch(batch)
	if err != nil {
		t.Error(err)
	}

	wg.Wait()
	// test has no assertion but will timeout if callback doesn't fire
}

func TestIndexInsertUpdateDeleteWithMultipleTypesStored(t *testing.T) {
	cfg := CreateConfig("TestIndexInsertUpdateDeleteWithMultipleTypesStored")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	var expectedCount uint64
	reader, err := idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), index.IndexField|index.StoreField))
	doc.AddField(document.NewNumericFieldWithIndexingOptions("age", []uint64{}, 35.99, index.IndexField|index.StoreField))
	df, err := document.NewDateTimeFieldWithIndexingOptions("unixEpoch", []uint64{}, time.Unix(0, 0), time.RFC3339, index.IndexField|index.StoreField)
	if err != nil {
		t.Error(err)
	}
	doc.AddField(df)
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	expectedCount++

	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}

	storedDocInt, err := indexReader.Document("1")
	if err != nil {
		t.Error(err)
	}

	storedDoc := storedDocInt.(*document.Document)

	err = indexReader.Close()
	if err != nil {
		t.Error(err)
	}

	if len(storedDoc.Fields) != 3 {
		t.Errorf("expected 3 stored field, got %d", len(storedDoc.Fields))
	}
	for _, field := range storedDoc.Fields {
		if field.Name() == "name" {
			textField, ok := field.(*document.TextField)
			if !ok {
				t.Errorf("expected text field")
			}
			if string(textField.Value()) != "test" {
				t.Errorf("expected field content 'test', got '%s'", string(textField.Value()))
			}
		} else if field.Name() == "age" {
			numField, ok := field.(*document.NumericField)
			if !ok {
				t.Errorf("expected numeric field")
			}
			numFieldNumer, err := numField.Number()
			if err != nil {
				t.Error(err)
			} else {
				if numFieldNumer != 35.99 {
					t.Errorf("expected numeric value 35.99, got %f", numFieldNumer)
				}
			}
		} else if field.Name() == "unixEpoch" {
			dateField, ok := field.(*document.DateTimeField)
			if !ok {
				t.Errorf("expected date field")
			}
			dateFieldDate, _, err := dateField.DateTime()
			if err != nil {
				t.Error(err)
			} else {
				if dateFieldDate != time.Unix(0, 0).UTC() {
					t.Errorf("expected date value unix epoch, got %v", dateFieldDate)
				}
			}
		} else if field.Name() == "_id" {
			t.Errorf("not expecting _id field")
		}
	}

	// now update the document, but omit one of the fields
	doc = document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("testup"), index.IndexField|index.StoreField))
	doc.AddField(document.NewNumericFieldWithIndexingOptions("age", []uint64{}, 36.99, index.IndexField|index.StoreField))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader2, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}

	// expected doc count shouldn't have changed
	docCount, err = indexReader2.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	// should only get 2 fields back now though
	storedDocInt, err = indexReader2.Document("1")
	if err != nil {
		t.Error(err)
	}

	storedDoc = storedDocInt.(*document.Document)

	err = indexReader2.Close()
	if err != nil {
		t.Error(err)
	}

	if len(storedDoc.Fields) != 2 {
		t.Errorf("expected 2 stored field, got %d", len(storedDoc.Fields))
	}

	for _, field := range storedDoc.Fields {
		if field.Name() == "name" {
			textField, ok := field.(*document.TextField)
			if !ok {
				t.Errorf("expected text field")
			}
			if string(textField.Value()) != "testup" {
				t.Errorf("expected field content 'testup', got '%s'", string(textField.Value()))
			}
		} else if field.Name() == "age" {
			numField, ok := field.(*document.NumericField)
			if !ok {
				t.Errorf("expected numeric field")
			}
			numFieldNumer, err := numField.Number()
			if err != nil {
				t.Error(err)
			} else {
				if numFieldNumer != 36.99 {
					t.Errorf("expected numeric value 36.99, got %f", numFieldNumer)
				}
			}
		} else if field.Name() == "_id" {
			t.Errorf("not expecting _id field")
		}
	}

	// now delete the document
	err = idx.Delete("1")
	if err != nil {
		t.Errorf("Error deleting entry from index: %v", err)
	}
	expectedCount--

	// expected doc count shouldn't have changed
	reader, err = idx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err = reader.DocCount()
	if err != nil {
		t.Error(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexInsertFields(t *testing.T) {
	cfg := CreateConfig("TestIndexInsertFields")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), index.IndexField|index.StoreField))
	doc.AddField(document.NewNumericFieldWithIndexingOptions("age", []uint64{}, 35.99, index.IndexField|index.StoreField))
	dateField, err := document.NewDateTimeFieldWithIndexingOptions("unixEpoch", []uint64{}, time.Unix(0, 0), time.RFC3339, index.IndexField|index.StoreField)
	if err != nil {
		t.Error(err)
	}
	doc.AddField(dateField)
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	fields, err := indexReader.Fields()
	if err != nil {
		t.Error(err)
	} else {
		fieldsMap := map[string]struct{}{}
		for _, field := range fields {
			fieldsMap[field] = struct{}{}
		}
		expectedFieldsMap := map[string]struct{}{
			"_id":       {},
			"name":      {},
			"age":       {},
			"unixEpoch": {},
		}
		if !reflect.DeepEqual(fieldsMap, expectedFieldsMap) {
			t.Errorf("expected fields: %v, got %v", expectedFieldsMap, fieldsMap)
		}
	}
}

func TestIndexUpdateComposites(t *testing.T) {
	cfg := CreateConfig("TestIndexUpdateComposites")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), index.IndexField|index.StoreField))
	doc.AddField(document.NewTextFieldWithIndexingOptions("title", []uint64{}, []byte("mister"), index.IndexField|index.StoreField))
	doc.AddField(document.NewCompositeFieldWithIndexingOptions("_all", true, nil, nil, index.IndexField))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	// now lets update it
	doc = document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("testupdated"), index.IndexField|index.StoreField))
	doc.AddField(document.NewTextFieldWithIndexingOptions("title", []uint64{}, []byte("misterupdated"), index.IndexField|index.StoreField))
	doc.AddField(document.NewCompositeFieldWithIndexingOptions("_all", true, nil, nil, index.IndexField))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// make sure new values are in index
	storedDocInt, err := indexReader.Document("1")
	if err != nil {
		t.Error(err)
	}
	storedDoc := storedDocInt.(*document.Document)
	if len(storedDoc.Fields) != 2 {
		t.Errorf("expected 2 stored field, got %d", len(storedDoc.Fields))
	}
	for _, field := range storedDoc.Fields {
		if field.Name() == "name" {
			textField, ok := field.(*document.TextField)
			if !ok {
				t.Errorf("expected text field")
			}
			if string(textField.Value()) != "testupdated" {
				t.Errorf("expected field content 'test', got '%s'", string(textField.Value()))
			}
		} else if field.Name() == "_id" {
			t.Errorf("not expecting _id field")
		}
	}
}

func TestIndexTermReaderCompositeFields(t *testing.T) {
	cfg := CreateConfig("TestIndexTermReaderCompositeFields")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), index.IndexField|index.StoreField|index.IncludeTermVectors))
	doc.AddField(document.NewTextFieldWithIndexingOptions("title", []uint64{}, []byte("mister"), index.IndexField|index.StoreField|index.IncludeTermVectors))
	doc.AddField(document.NewCompositeFieldWithIndexingOptions("_all", true, nil, nil, index.IndexField|index.IncludeTermVectors))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	termFieldReader, err := indexReader.TermFieldReader(context.TODO(), []byte("mister"), "_all", true, true, true)
	if err != nil {
		t.Error(err)
	}

	tfd, err := termFieldReader.Next(nil)
	for tfd != nil && err == nil {
		externalID, err := indexReader.ExternalID(tfd.ID)
		if err != nil {
			t.Fatal(err)
		}

		if externalID != "1" {
			t.Errorf("expected to find document id 1")
		}

		tfd, err = termFieldReader.Next(nil)
		if err != nil {
			t.Error(err)
		}
	}
	if err != nil {
		t.Error(err)
	}
}

func TestIndexDocValueReader(t *testing.T) {
	cfg := CreateConfig("TestIndexDocumentVisitFieldTerms")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), index.IndexField|index.StoreField|index.IncludeTermVectors))
	doc.AddField(document.NewTextFieldWithIndexingOptions("title", []uint64{}, []byte("mister"), index.IndexField|index.StoreField|index.IncludeTermVectors))
	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := indexReader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	actualFieldTerms := make(fieldTerms)

	internalID, err := indexReader.InternalID("1")
	if err != nil {
		t.Fatal(err)
	}

	dvr, err := indexReader.DocValueReader([]string{"name", "title"})
	if err != nil {
		t.Error(err)
	}

	err = dvr.VisitDocValues(internalID, func(field string, term []byte) {
		actualFieldTerms[field] = append(actualFieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms := fieldTerms{
		"name":  []string{"test"},
		"title": []string{"mister"},
	}
	if !reflect.DeepEqual(actualFieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, actualFieldTerms)
	}
}

func TestDocValueReaderConcurrent(t *testing.T) {
	cfg := CreateConfig("TestFieldTermsConcurrent")

	// setting path to empty string disables persistence/merging
	// which ensures we have in-memory segments
	// which is important for this test, to trigger the right code
	// path, where fields exist, but have NOT been uninverted by
	// the Segment impl (in memory segments are still SegmentBase)
	cfg["path"] = ""

	defer func() {
		err := DestroyTest(cfg)
		if err != nil {
			t.Fatal(err)
		}
	}()

	mp := mapping.NewIndexMapping()
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}
	defer func() {
		cerr := idx.Close()
		if cerr != nil {
			t.Fatal(cerr)
		}
	}()

	// create a single bath (leading to 1 in-memory segment)
	// have one field named "name" and 100 others named f0-f99
	batch := index.NewBatch()
	for i := 0; i < 1000; i++ {
		data := map[string]string{
			"name": fmt.Sprintf("doc-%d", i),
		}
		for j := 0; j < 100; j++ {
			data[fmt.Sprintf("f%d", j)] = fmt.Sprintf("v%d", i)
		}
		doc := document.NewDocument(fmt.Sprintf("%d", i))
		err = mp.MapDocument(doc, data)
		if err != nil {
			t.Errorf("error mapping doc: %v", err)
		}
		batch.Update(doc)
	}

	err = idx.Batch(batch)
	if err != nil {
		t.Fatal(err)
	}

	// now have 10 goroutines try to visit field values for doc 1
	// in a random field
	var wg sync.WaitGroup
	for j := 0; j < 10; j++ {
		wg.Add(1)
		go func() {
			r, err := idx.Reader()
			if err != nil {
				t.Errorf("error getting reader: %v", err)
				wg.Done()
				return
			}
			docNumber, err := r.InternalID("1")
			if err != nil {
				t.Errorf("error getting internal ID: %v", err)
				wg.Done()
				return
			}
			dvr, err := r.DocValueReader([]string{fmt.Sprintf("f%d", rand.Intn(100))})
			if err != nil {
				t.Errorf("error getting doc value reader: %v", err)
				wg.Done()
				return
			}
			err = dvr.VisitDocValues(docNumber, func(field string, term []byte) {})
			if err != nil {
				t.Errorf("error visiting doc values: %v", err)
				wg.Done()
				return
			}
			wg.Done()
		}()
	}

	wg.Wait()
}

func TestConcurrentUpdate(t *testing.T) {
	cfg := CreateConfig("TestConcurrentUpdate")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	// do some concurrent updates
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			doc := document.NewDocument("1")
			doc.AddField(document.NewTextFieldWithIndexingOptions(strconv.Itoa(i), []uint64{}, []byte(strconv.Itoa(i)), index.StoreField))
			err := idx.Update(doc)
			if err != nil {
				t.Errorf("Error updating index: %v", err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	// now load the name field and see what we get
	r, err := idx.Reader()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := r.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	docInt, err := r.Document("1")
	if err != nil {
		log.Fatal(err)
	}

	doc := docInt.(*document.Document)

	if len(doc.Fields) > 2 {
		t.Errorf("expected no more than 2 fields, found %d", len(doc.Fields))
	}
}

func TestLargeField(t *testing.T) {
	cfg := CreateConfig("TestLargeField")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	var largeFieldValue []byte
	for len(largeFieldValue) < 4096 {
		largeFieldValue = append(largeFieldValue, bleveWikiArticle1K...)
	}

	d := document.NewDocument("large")
	f := document.NewTextFieldWithIndexingOptions("desc", nil, largeFieldValue, index.IndexField|index.StoreField)
	d.AddField(f)

	err = idx.Update(d)
	if err != nil {
		t.Fatal(err)
	}
}

var bleveWikiArticle1K = []byte(`Boiling liquid expanding vapor explosion
From Wikipedia, the free encyclopedia
See also: Boiler explosion and Steam explosion

Flames subsequent to a flammable liquid BLEVE from a tanker. BLEVEs do not necessarily involve fire.

This article's tone or style may not reflect the encyclopedic tone used on Wikipedia. See Wikipedia's guide to writing better articles for suggestions. (July 2013)
A boiling liquid expanding vapor explosion (BLEVE, /ˈblɛviː/ blev-ee) is an explosion caused by the rupture of a vessel containing a pressurized liquid above its boiling point.[1]
Contents  [hide]
1 Mechanism
1.1 Water example
1.2 BLEVEs without chemical reactions
2 Fires
3 Incidents
4 Safety measures
5 See also
6 References
7 External links
Mechanism[edit]

This section needs additional citations for verification. Please help improve this article by adding citations to reliable sources. Unsourced material may be challenged and removed. (July 2013)
There are three characteristics of liquids which are relevant to the discussion of a BLEVE:`)

func TestIndexDocValueReaderWithMultipleDocs(t *testing.T) {
	cfg := CreateConfig("TestIndexDocumentVisitFieldTermsWithMultipleDocs")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	doc := document.NewDocument("1")
	doc.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test"), index.IndexField|index.StoreField|index.IncludeTermVectors))
	doc.AddField(document.NewTextFieldWithIndexingOptions("title", []uint64{}, []byte("mister"), index.IndexField|index.StoreField|index.IncludeTermVectors))

	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}

	actualFieldTerms := make(fieldTerms)
	docNumber, err := indexReader.InternalID("1")
	if err != nil {
		t.Fatal(err)
	}

	dvr, err := indexReader.DocValueReader([]string{"name", "title"})
	if err != nil {
		t.Fatal(err)
	}

	err = dvr.VisitDocValues(docNumber, func(field string, term []byte) {
		actualFieldTerms[field] = append(actualFieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms := fieldTerms{
		"name":  []string{"test"},
		"title": []string{"mister"},
	}
	if !reflect.DeepEqual(actualFieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, actualFieldTerms)
	}
	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc2 := document.NewDocument("2")
	doc2.AddField(document.NewTextFieldWithIndexingOptions("name", []uint64{}, []byte("test2"), index.IndexField|index.StoreField|index.IncludeTermVectors))
	doc2.AddField(document.NewTextFieldWithIndexingOptions("title", []uint64{}, []byte("mister2"), index.IndexField|index.StoreField|index.IncludeTermVectors))
	err = idx.Update(doc2)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	indexReader, err = idx.Reader()
	if err != nil {
		t.Error(err)
	}

	actualFieldTerms = make(fieldTerms)
	docNumber, err = indexReader.InternalID("2")
	if err != nil {
		t.Fatal(err)
	}

	dvr, err = indexReader.DocValueReader([]string{"name", "title"})
	if err != nil {
		t.Fatal(err)
	}

	err = dvr.VisitDocValues(docNumber, func(field string, term []byte) {
		actualFieldTerms[field] = append(actualFieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms = fieldTerms{
		"name":  []string{"test2"},
		"title": []string{"mister2"},
	}
	if !reflect.DeepEqual(actualFieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, actualFieldTerms)
	}
	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}

	doc3 := document.NewDocument("3")
	doc3.AddField(document.NewTextFieldWithIndexingOptions("name3", []uint64{}, []byte("test3"), index.IndexField|index.StoreField|index.IncludeTermVectors))
	doc3.AddField(document.NewTextFieldWithIndexingOptions("title3", []uint64{}, []byte("mister3"), index.IndexField|index.StoreField|index.IncludeTermVectors))
	err = idx.Update(doc3)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	indexReader, err = idx.Reader()
	if err != nil {
		t.Error(err)
	}

	actualFieldTerms = make(fieldTerms)
	docNumber, err = indexReader.InternalID("3")
	if err != nil {
		t.Fatal(err)
	}

	dvr, err = indexReader.DocValueReader([]string{"name3", "title3"})
	if err != nil {
		t.Fatal(err)
	}

	err = dvr.VisitDocValues(docNumber, func(field string, term []byte) {
		actualFieldTerms[field] = append(actualFieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms = fieldTerms{
		"name3":  []string{"test3"},
		"title3": []string{"mister3"},
	}
	if !reflect.DeepEqual(actualFieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, actualFieldTerms)
	}

	actualFieldTerms = make(fieldTerms)
	docNumber, err = indexReader.InternalID("1")
	if err != nil {
		t.Fatal(err)
	}

	dvr, err = indexReader.DocValueReader([]string{"name", "title"})
	if err != nil {
		t.Fatal(err)
	}

	err = dvr.VisitDocValues(docNumber, func(field string, term []byte) {
		actualFieldTerms[field] = append(actualFieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms = fieldTerms{
		"name":  []string{"test"},
		"title": []string{"mister"},
	}
	if !reflect.DeepEqual(actualFieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, actualFieldTerms)
	}
	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexDocValueReaderWithMultipleFieldOptions(t *testing.T) {
	cfg := CreateConfig("TestIndexDocumentVisitFieldTermsWithMultipleFieldOptions")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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

	// mix of field options, this exercises the run time/ on the fly un inverting of
	// doc values for custom options enabled field like designation, dept.
	options := index.IndexField | index.StoreField | index.IncludeTermVectors
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))    // default doc value persisted
	doc.AddField(document.NewTextField("title", []uint64{}, []byte("mister"))) // default doc value persisted
	doc.AddField(document.NewTextFieldWithIndexingOptions("designation", []uint64{}, []byte("engineer"), options))
	doc.AddField(document.NewTextFieldWithIndexingOptions("dept", []uint64{}, []byte("bleve"), options))

	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}

	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}

	actualFieldTerms := make(fieldTerms)
	docNumber, err := indexReader.InternalID("1")
	if err != nil {
		t.Fatal(err)
	}

	dvr, err := indexReader.DocValueReader([]string{"name", "designation", "dept"})
	if err != nil {
		t.Fatal(err)
	}

	err = dvr.VisitDocValues(docNumber, func(field string, term []byte) {
		actualFieldTerms[field] = append(actualFieldTerms[field], string(term))
	})
	if err != nil {
		t.Error(err)
	}
	expectedFieldTerms := fieldTerms{
		"name":        []string{"test"},
		"designation": []string{"engineer"},
		"dept":        []string{"bleve"},
	}
	if !reflect.DeepEqual(actualFieldTerms, expectedFieldTerms) {
		t.Errorf("expected field terms: %#v, got: %#v", expectedFieldTerms, actualFieldTerms)
	}
	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestAllFieldWithDifferentTermVectorsEnabled(t *testing.T) {
	// Based on https://github.com/blevesearch/bleve/issues/895 from xeizmendi
	cfg := CreateConfig("TestAllFieldWithDifferentTermVectorsEnabled")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := DestroyTest(cfg)
		if err != nil {
			t.Log(err)
		}
	}()

	testConfig := cfg
	mp := mapping.NewIndexMapping()

	keywordMapping := mapping.NewTextFieldMapping()
	keywordMapping.Analyzer = keyword.Name
	keywordMapping.IncludeTermVectors = false
	keywordMapping.IncludeInAll = true

	textMapping := mapping.NewTextFieldMapping()
	textMapping.Analyzer = standard.Name
	textMapping.IncludeTermVectors = true
	textMapping.IncludeInAll = true

	docMapping := mapping.NewDocumentStaticMapping()
	docMapping.AddFieldMappingsAt("keyword", keywordMapping)
	docMapping.AddFieldMappingsAt("text", textMapping)

	mp.DefaultMapping = docMapping

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch("storeName", testConfig, analysisQueue)
	if err != nil {
		log.Fatalln(err)
	}
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	data := map[string]string{
		"keyword": "something",
		"text":    "A sentence that includes something within.",
	}

	doc := document.NewDocument("1")
	err = mp.MapDocument(doc, data)
	if err != nil {
		t.Errorf("error mapping doc: %v", err)
	}

	err = idx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
}

func TestForceVersion(t *testing.T) {
	cfg := map[string]interface{}{}
	cfg["forceSegmentType"] = "zap"
	cfg["forceSegmentVersion"] = 11
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatalf("error opening a supported version: %v", err)
	}
	s := idx.(*Scorch)
	if s.segPlugin.Version() != 11 {
		t.Fatalf("wrong segment wrapper version loaded, expected %d got %d", 11, s.segPlugin.Version())
	}
	cfg["forceSegmentVersion"] = 12
	idx, err = NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatalf("error opening a supported version: %v", err)
	}
	s = idx.(*Scorch)
	if s.segPlugin.Version() != 12 {
		t.Fatalf("wrong segment wrapper version loaded, expected %d got %d", 12, s.segPlugin.Version())
	}
	cfg["forceSegmentVersion"] = 10
	_, err = NewScorch(Name, cfg, analysisQueue)
	if err == nil {
		t.Fatalf("expected an error opening an unsupported version, got nil")
	}
}

func TestIndexForceMerge(t *testing.T) {
	cfg := CreateConfig("TestIndexForceMerge")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := DestroyTest(cfg)
		if err != nil {
			t.Log(err)
		}
	}()

	tmp := struct {
		MaxSegmentsPerTier   int   `json:"maxSegmentsPerTier"`
		SegmentsPerMergeTask int   `json:"segmentsPerMergeTask"`
		FloorSegmentSize     int64 `json:"floorSegmentSize"`
	}{
		int(1),
		int(1),
		int64(2),
	}
	cfg["scorchMergePlanOptions"] = &tmp

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}

	err = idx.Open()
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}

	var expectedCount uint64
	batch := index.NewBatch()
	for i := 0; i < 10; i++ {
		doc := document.NewDocument(fmt.Sprintf("doc1-%d", i))
		doc.AddField(document.NewTextField("name", []uint64{}, []byte(fmt.Sprintf("text1-%d", i))))
		batch.Update(doc)
		doc = document.NewDocument(fmt.Sprintf("doc2-%d", i))
		doc.AddField(document.NewTextField("name", []uint64{}, []byte(fmt.Sprintf("text2-%d", i))))
		batch.Update(doc)
		err = idx.Batch(batch)
		if err != nil {
			t.Error(err)
		}
		batch.Reset()
		expectedCount += 2
	}

	// verify doc count
	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}

	docCount, err := indexReader.DocCount()
	if err != nil {
		t.Fatal(err)
	}

	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}

	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}

	var si *Scorch
	var ok bool
	if si, ok = idx.(*Scorch); !ok {
		t.Errorf("expects a scorch index")
	}

	nfs := atomic.LoadUint64(&si.stats.TotFileSegmentsAtRoot)
	if nfs != 10 {
		t.Errorf("expected 10 root file segments, got: %d", nfs)
	}

	ctx := context.Background()
	for atomic.LoadUint64(&si.stats.TotFileSegmentsAtRoot) != 1 {
		err := si.ForceMerge(ctx, &mergeplan.MergePlanOptions{
			MaxSegmentsPerTier:   1,
			MaxSegmentSize:       10000,
			SegmentsPerMergeTask: 10,
			FloorSegmentSize:     10000,
		})
		if err != nil {
			t.Errorf("ForceMerge failed, err: %v", err)
		}
	}

	// verify the final root segment count
	if atomic.LoadUint64(&si.stats.TotFileSegmentsAtRoot) != 1 {
		t.Errorf("expected a single root file segments, got: %d",
			atomic.LoadUint64(&si.stats.TotFileSegmentsAtRoot))
	}

	// verify with an invalid merge plan
	err = si.ForceMerge(ctx, &mergeplan.MergePlanOptions{
		MaxSegmentsPerTier:   1,
		MaxSegmentSize:       1 << 33,
		SegmentsPerMergeTask: 10,
		FloorSegmentSize:     10000,
	})
	if err != mergeplan.ErrMaxSegmentSizeTooLarge {
		t.Errorf("ForceMerge expected to fail with ErrMaxSegmentSizeTooLarge")
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestCancelIndexForceMerge(t *testing.T) {
	cfg := CreateConfig("TestCancelIndexForceMerge")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := DestroyTest(cfg)
		if err != nil {
			t.Log(err)
		}
	}()

	tmp := struct {
		MaxSegmentsPerTier   int   `json:"maxSegmentsPerTier"`
		SegmentsPerMergeTask int   `json:"segmentsPerMergeTask"`
		FloorSegmentSize     int64 `json:"floorSegmentSize"`
	}{
		int(1),
		int(1),
		int64(2),
	}
	cfg["scorchMergePlanOptions"] = &tmp

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = idx.Open()
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}

	var expectedCount uint64
	batch := index.NewBatch()
	for i := 0; i < 20; i++ {
		doc := document.NewDocument(fmt.Sprintf("doc1-%d", i))
		doc.AddField(document.NewTextField("name", []uint64{}, []byte(fmt.Sprintf("text1-%d", i))))
		batch.Update(doc)
		doc = document.NewDocument(fmt.Sprintf("doc2-%d", i))
		doc.AddField(document.NewTextField("name", []uint64{}, []byte(fmt.Sprintf("text2-%d", i))))
		batch.Update(doc)
		err = idx.Batch(batch)
		if err != nil {
			t.Error(err)
		}
		batch.Reset()
		expectedCount += 2
	}

	// verify doc count
	indexReader, err := idx.Reader()
	if err != nil {
		t.Error(err)
	}
	docCount, err := indexReader.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if docCount != expectedCount {
		t.Errorf("Expected document count to be %d got %d", expectedCount, docCount)
	}
	err = indexReader.Close()
	if err != nil {
		t.Fatal(err)
	}

	var si *Scorch
	var ok bool
	if si, ok = idx.(*Scorch); !ok {
		t.Fatal("expects a scorch index")
	}

	// no merge operations are expected as per the original merge policy.
	nfsr := atomic.LoadUint64(&si.stats.TotFileSegmentsAtRoot)
	if nfsr != 20 {
		t.Errorf("expected 20 root file segments, got: %d", nfsr)
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	// cancel the force merge operation once the root has some new merge
	// introductions. ie if the root has lesser file segments than earlier.
	go func() {
		for {
			nval := atomic.LoadUint64(&si.stats.TotFileSegmentsAtRoot)
			if nval < nfsr {
				cancel()
				return
			}
			time.Sleep(time.Millisecond * 5)
		}
	}()

	err = si.ForceMerge(ctx, &mergeplan.MergePlanOptions{
		MaxSegmentsPerTier:   1,
		MaxSegmentSize:       10000,
		SegmentsPerMergeTask: 5,
		FloorSegmentSize:     10000,
	})
	if err != nil {
		t.Errorf("ForceMerge failed, err: %v", err)
	}

	// verify the final root file segment count or forceMerge completion
	if atomic.LoadUint64(&si.stats.TotFileSegmentsAtRoot) == 1 {
		t.Errorf("expected many files at root, but got: %d segments",
			atomic.LoadUint64(&si.stats.TotFileSegmentsAtRoot))
	}

	err = idx.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexSeekBackwardsStats(t *testing.T) {
	cfg := CreateConfig("TestIndexOpenReopen")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
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
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	defer func() {
		err := idx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	// insert a doc
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("cat")))
	err = idx.Update(doc)
	if err != nil {
		t.Fatalf("error updating index: %v", err)
	}

	// insert another doc
	doc = document.NewDocument("2")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("cat")))
	err = idx.Update(doc)
	if err != nil {
		t.Fatalf("error updating index: %v", err)
	}

	reader, err := idx.Reader()
	if err != nil {
		t.Fatalf("error getting index reader: %v", err)
	}
	defer reader.Close()

	tfr, err := reader.TermFieldReader(context.TODO(), []byte("cat"), "name", false, false, false)
	if err != nil {
		t.Fatalf("error getting term field readyer for name/cat: %v", err)
	}

	tfdFirst, err := tfr.Next(nil)
	if err != nil {
		t.Fatalf("error getting first tfd: %v", err)
	}

	_, err = tfr.Next(nil)
	if err != nil {
		t.Fatalf("error getting second tfd: %v", err)
	}

	// seek backwards to the first
	_, err = tfr.Advance(tfdFirst.ID, nil)
	if err != nil {
		t.Fatalf("error adancing backwards: %v", err)
	}

	err = tfr.Close()
	if err != nil {
		t.Fatalf("error closing term field reader: %v", err)
	}

	if idx.(*Scorch).stats.TotTermSearchersStarted != idx.(*Scorch).stats.TotTermSearchersFinished {
		t.Errorf("expected term searchers started %d to equal term searchers finished %d",
			idx.(*Scorch).stats.TotTermSearchersStarted,
			idx.(*Scorch).stats.TotTermSearchersFinished)
	}
}

// fieldTerms contains the terms used by a document, keyed by field
type fieldTerms map[string][]string

// FieldsNotYetCached returns a list of fields not yet cached out of a larger list of fields
func (f fieldTerms) FieldsNotYetCached(fields []string) []string {
	rv := make([]string, 0, len(fields))
	for _, field := range fields {
		if _, ok := f[field]; !ok {
			rv = append(rv, field)
		}
	}
	return rv
}

// Merge will combine two fieldTerms
// it assumes that the terms lists are complete (thus do not need to be merged)
// field terms from the other list always replace the ones in the receiver
func (f fieldTerms) Merge(other fieldTerms) {
	for field, terms := range other {
		f[field] = terms
	}
}

func TestOpenBoltTimeout(t *testing.T) {
	cfg := CreateConfig("TestIndexOpenReopen")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := DestroyTest(cfg)
		if err != nil {
			t.Log(err)
		}
	}()

	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch("storeName", cfg, analysisQueue)
	if err != nil {
		log.Fatalln(err)
	}
	err = idx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}

	// new config
	cfg2 := CreateConfig("TestIndexOpenReopen")
	// copy path from original config
	cfg2["path"] = cfg["path"]
	// set timeout in this cfg
	cfg2["bolt_timeout"] = "100ms"

	idx2, err := NewScorch("storeName", cfg2, analysisQueue)
	if err != nil {
		log.Fatalln(err)
	}
	err = idx2.Open()
	if err == nil {
		t.Error("expected timeout error opening index again")
	}
}

func TestReadOnlyIndex(t *testing.T) {
	// https://github.com/blevesearch/bleve/issues/1623
	cfg := CreateConfig("TestReadOnlyIndex")
	err := InitTest(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := DestroyTest(cfg)
		if err != nil {
			t.Log(err)
		}
	}()

	analysisQueue := index.NewAnalysisQueue(1)
	writeIdx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatal(err)
	}
	err = writeIdx.Open()
	if err != nil {
		t.Fatalf("error opening index: %v", err)
	}
	writeIdxClosed := false
	defer func() {
		if !writeIdxClosed {
			err := writeIdx.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	// Add a single document to the index.
	doc := document.NewDocument("1")
	doc.AddField(document.NewTextField("name", []uint64{}, []byte("test")))
	err = writeIdx.Update(doc)
	if err != nil {
		t.Errorf("Error updating index: %v", err)
	}
	writeIdx.Close()
	writeIdxClosed = true

	// After the index is written, change permissions on every file
	// in the index to read-only.
	var permissionsFunc func(folder string)
	permissionsFunc = func(folder string) {
		entries, _ := os.ReadDir(folder)
		for _, entry := range entries {
			fullName := filepath.Join(folder, entry.Name())
			if entry.IsDir() {
				permissionsFunc(fullName)
			} else {
				if err := os.Chmod(fullName, 0o555); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	permissionsFunc(cfg["path"].(string))

	// Now reopen the index in read-only mode and attempt to read from it.
	cfg["read_only"] = true
	readIdx, err := NewScorch(Name, cfg, analysisQueue)
	defer func() {
		err := readIdx.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	if err != nil {
		t.Fatal(err)
	}
	err = readIdx.Open()
	if err != nil {
		t.Errorf("error opening index: %v", err)
	}
	reader, err := readIdx.Reader()
	if err != nil {
		t.Fatal(err)
	}
	docCount, err := reader.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if docCount != 1 {
		t.Errorf("Expected document count to be %d got %d", 1, docCount)
	}
}

func BenchmarkAggregateFieldStats(b *testing.B) {
	fieldStatsArray := make([]*fieldStats, 1000)

	for i := range fieldStatsArray {
		fieldStatsArray[i] = newFieldStats()

		fieldStatsArray[i].Store("num_vectors", "vector", uint64(rand.Intn(1000)))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		aggFieldStats := newFieldStats()

		for _, fs := range fieldStatsArray {
			aggFieldStats.Aggregate(fs)
		}
	}
}

func TestPersistorMergerOptions(t *testing.T) {
	type test struct {
		config    string
		expectErr bool
	}
	tests := []test{
		{
			// valid config and no error expected
			config: `{
				"scorchPersisterOptions": {
					"persisterNapTimeMSec": 1110,
					"memoryPressurePauseThreshold" : 333
				}
			}`,
			expectErr: false,
		},
		{
			// valid json with invalid config values
			// and error expected
			config: `{
				"scorchPersisterOptions": {
					"persisterNapTimeMSec": "1110",
					"memoryPressurePauseThreshold" : [333]
				}
			}`,
			expectErr: true,
		},
		{
			// valid json with invalid config values
			// and error expected
			config: `{
				"scorchPersisterOptions": {
					"persisterNapTimeMSec": 1110.2,
					"memoryPressurePauseThreshold" : 333
				}
			}`,
			expectErr: true,
		},
		{
			// invalid setting for scorchMergePlanOptions
			config: `{
				"scorchPersisterOptions": {
					"persisterNapTimeMSec": 1110,
					"memoryPressurePauseThreshold" : 333
				},
				"scorchMergePlanOptions": [{
					"maxSegmentSize": 10000,
					"maxSegmentsPerTier": 10,
					"segmentsPerMergeTask": 10
				}]
			}`,
			expectErr: true,
		},
		{
			// valid setting
			config: `{
				"scorchPersisterOptions": {
					"persisterNapTimeMSec": 1110,
					"memoryPressurePauseThreshold" : 333
				},
				"scorchMergePlanOptions": {
					"maxSegmentSize": 10000,
					"maxSegmentsPerTier": 10,
					"segmentsPerMergeTask": 10
				}
			}`,
			expectErr: false,
		},
		{
			config: `{
				"scorchPersisterOptions": {
					"persisterNapTimeMSec": 1110,
					"memoryPressurePauseThreshold" : 333
				},
				"scorchMergePlanOptions": {
					"maxSegmentSize": 5.6,
					"maxSegmentsPerTier": 10,
					"segmentsPerMergeTask": 10
				}
			}`,
			expectErr: true,
		},
	}
	for i, test := range tests {
		cfg := map[string]interface{}{}
		err := json.Unmarshal([]byte(test.config), &cfg)
		if err != nil {
			t.Fatalf("test %d: error unmarshalling config: %v", i, err)
		}
		analysisQueue := index.NewAnalysisQueue(1)
		_, err = NewScorch(Name, cfg, analysisQueue)
		if test.expectErr {
			if err == nil {
				t.Errorf("test %d: expected error, got nil", i)
			}
		} else {
			if err != nil {
				t.Errorf("test %d: unexpected error: %v", i, err)
			}
		}
	}
}
