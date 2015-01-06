//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

import (
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

func TestCrud(t *testing.T) {
	defer os.RemoveAll("testidx")

	index, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}

	doca := map[string]interface{}{
		"name": "marty",
		"desc": "gophercon india",
	}
	err = index.Index("a", doca)
	if err != nil {
		t.Error(err)
	}

	docy := map[string]interface{}{
		"name": "jasper",
		"desc": "clojure",
	}
	err = index.Index("y", docy)
	if err != nil {
		t.Error(err)
	}

	err = index.Delete("y")
	if err != nil {
		t.Error(err)
	}

	docx := map[string]interface{}{
		"name": "rose",
		"desc": "googler",
	}
	err = index.Index("x", docx)
	if err != nil {
		t.Error(err)
	}

	err = index.SetInternal([]byte("status"), []byte("pending"))
	if err != nil {
		t.Error(err)
	}

	docb := map[string]interface{}{
		"name": "steve",
		"desc": "cbft master",
	}
	batch := NewBatch()
	batch.Index("b", docb)
	batch.Delete("x")
	batch.SetInternal([]byte("batchi"), []byte("batchv"))
	batch.DeleteInternal([]byte("status"))
	err = index.Batch(batch)
	if err != nil {
		t.Error(err)
	}
	val, err := index.GetInternal([]byte("batchi"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "batchv" {
		t.Errorf("expected 'batchv', got '%s'", val)
	}
	val, err = index.GetInternal([]byte("status"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected nil, got '%s'", val)
	}

	err = index.SetInternal([]byte("seqno"), []byte("7"))
	if err != nil {
		t.Error(err)
	}
	err = index.SetInternal([]byte("status"), []byte("ready"))
	if err != nil {
		t.Error(err)
	}
	err = index.DeleteInternal([]byte("status"))
	if err != nil {
		t.Error(err)
	}
	val, err = index.GetInternal([]byte("status"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected nil, got '%s'", val)
	}

	val, err = index.GetInternal([]byte("seqno"))
	if err != nil {
		t.Error(err)
	}
	if string(val) != "7" {
		t.Errorf("expected '7', got '%s'", val)
	}

	// close the index, open it again, and try some more things
	index.Close()

	index, err = Open("testidx")
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	count, err := index.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("expected doc count 2, got %d", count)
	}

	doc, err := index.Document("a")
	if err != nil {
		t.Fatal(err)
	}
	if doc == nil {
		t.Errorf("expected doc not nil, got nil")
	}
	foundNameField := false
	for _, field := range doc.Fields {
		if field.Name() == "name" && string(field.Value()) == "marty" {
			foundNameField = true
		}
	}
	if !foundNameField {
		t.Errorf("expected to find field named 'name' with value 'marty'")
	}

	fields, err := index.Fields()
	if err != nil {
		t.Fatal(err)
	}
	expectedFields := map[string]bool{
		"_all": false,
		"name": false,
		"desc": false,
	}
	if len(fields) != len(expectedFields) {
		t.Fatalf("expected %d fields got %d", len(expectedFields), len(fields))
	}
	for _, f := range fields {
		expectedFields[f] = true
	}
	for ef, efp := range expectedFields {
		if !efp {
			t.Errorf("field %s is missing", ef)
		}
	}
}

func TestIndexCreateNewOverExisting(t *testing.T) {
	defer os.RemoveAll("testidx")

	index, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	index.Close()
	index, err = New("testidx", NewIndexMapping())
	if err != ErrorIndexPathExists {
		t.Fatalf("expected error index path exists, got %v", err)
	}
}

func TestIndexOpenNonExisting(t *testing.T) {
	_, err := Open("doesnotexist")
	if err != ErrorIndexPathDoesNotExist {
		t.Fatalf("expected error index path does not exist, got %v", err)
	}
}

func TestIndexOpenMetaMissingOrCorrupt(t *testing.T) {
	defer os.RemoveAll("testidx")

	index, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	index.Close()

	// now intentionally change the storage type
	ioutil.WriteFile("testidx/index_meta.json", []byte(`{"storage":"mystery"}`), 0666)

	index, err = Open("testidx")
	if err != ErrorUnknownStorageType {
		t.Fatalf("expected error unknown storage type, got %v", err)
	}

	// now intentionally corrupt the metadata
	ioutil.WriteFile("testidx/index_meta.json", []byte("corrupted"), 0666)

	index, err = Open("testidx")
	if err != ErrorIndexMetaCorrupt {
		t.Fatalf("expected error index metadata corrupted, got %v", err)
	}

	// now intentionally remove the metadata
	os.Remove("testidx/index_meta.json")

	index, err = Open("testidx")
	if err != ErrorIndexMetaMissing {
		t.Fatalf("expected error index metadata missing, got %v", err)
	}
}

func TestInMemIndex(t *testing.T) {

	index, err := New("", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	index.Close()
}

func TestClosedIndex(t *testing.T) {
	index, err := New("", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	index.Close()

	err = index.Index("test", "test")
	if err != ErrorIndexClosed {
		t.Errorf("expected error index closed, got %v", err)
	}

	err = index.Delete("test")
	if err != ErrorIndexClosed {
		t.Errorf("expected error index closed, got %v", err)
	}

	b := NewBatch()
	err = index.Batch(b)
	if err != ErrorIndexClosed {
		t.Errorf("expected error index closed, got %v", err)
	}

	_, err = index.Document("test")
	if err != ErrorIndexClosed {
		t.Errorf("expected error index closed, got %v", err)
	}

	_, err = index.DocCount()
	if err != ErrorIndexClosed {
		t.Errorf("expected error index closed, got %v", err)
	}

	_, err = index.Search(NewSearchRequest(NewTermQuery("test")))
	if err != ErrorIndexClosed {
		t.Errorf("expected error index closed, got %v", err)
	}

	_, err = index.Fields()
	if err != ErrorIndexClosed {
		t.Errorf("expected error index closed, got %v", err)
	}
}

func TestSlowSearch(t *testing.T) {
	defer os.RemoveAll("testidx")

	defer func() {
		// reset logger back to normal
		SetLog(log.New(ioutil.Discard, "bleve", log.LstdFlags))
	}()
	// set custom logger
	var sdw sawDataWriter
	SetLog(log.New(&sdw, "bleve", log.LstdFlags))

	index, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()

	Config.SlowSearchLogThreshold = 1 * time.Minute

	query := NewTermQuery("water")
	req := NewSearchRequest(query)
	index.Search(req)

	if sdw.sawData {
		t.Errorf("expected to not see slow query logged, but did")
	}

	Config.SlowSearchLogThreshold = 1 * time.Microsecond
	index.Search(req)

	if !sdw.sawData {
		t.Errorf("expected to see slow query logged, but didn't")
	}
}

type sawDataWriter struct {
	sawData bool
}

func (s *sawDataWriter) Write(p []byte) (n int, err error) {
	s.sawData = true
	return len(p), nil
}
