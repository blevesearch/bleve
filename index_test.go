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
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestCrud(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

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
	batch := index.NewBatch()
	err = batch.Index("b", docb)
	if err != nil {
		t.Error(err)
	}
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
	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}

	index, err = Open("testidx")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

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
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	index, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
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
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	index, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}

	// now intentionally change the storage type
	err = ioutil.WriteFile("testidx/index_meta.json", []byte(`{"storage":"mystery"}`), 0666)
	if err != nil {
		t.Fatal(err)
	}

	index, err = Open("testidx")
	if err != ErrorUnknownStorageType {
		t.Fatalf("expected error unknown storage type, got %v", err)
	}

	// now intentionally corrupt the metadata
	err = ioutil.WriteFile("testidx/index_meta.json", []byte("corrupted"), 0666)
	if err != nil {
		t.Fatal(err)
	}

	index, err = Open("testidx")
	if err != ErrorIndexMetaCorrupt {
		t.Fatalf("expected error index metadata corrupted, got %v", err)
	}

	// now intentionally remove the metadata
	err = os.Remove("testidx/index_meta.json")
	if err != nil {
		t.Fatal(err)
	}

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
	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestClosedIndex(t *testing.T) {
	index, err := New("", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = index.Index("test", "test")
	if err != ErrorIndexClosed {
		t.Errorf("expected error index closed, got %v", err)
	}

	err = index.Delete("test")
	if err != ErrorIndexClosed {
		t.Errorf("expected error index closed, got %v", err)
	}

	b := index.NewBatch()
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
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

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
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	Config.SlowSearchLogThreshold = 1 * time.Minute

	query := NewTermQuery("water")
	req := NewSearchRequest(query)
	_, err = index.Search(req)
	if err != nil {
		t.Fatal(err)
	}

	if sdw.sawData {
		t.Errorf("expected to not see slow query logged, but did")
	}

	Config.SlowSearchLogThreshold = 1 * time.Microsecond
	_, err = index.Search(req)
	if err != nil {
		t.Fatal(err)
	}

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

func TestStoredFieldPreserved(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	index, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	doca := map[string]interface{}{
		"name": "Marty",
		"desc": "GopherCON India",
	}
	err = index.Index("a", doca)
	if err != nil {
		t.Error(err)
	}

	q := NewTermQuery("marty")
	req := NewSearchRequest(q)
	req.Fields = []string{"name", "desc"}
	res, err := index.Search(req)
	if err != nil {
		t.Error(err)
	}

	if len(res.Hits) != 1 {
		t.Errorf("expected 1 hit, got %d", len(res.Hits))
	}

	if res.Hits[0].Fields["name"] != "Marty" {
		t.Errorf("expected 'Marty' got '%s'", res.Hits[0].Fields["name"])
	}
	if res.Hits[0].Fields["desc"] != "GopherCON India" {
		t.Errorf("expected 'GopherCON India' got '%s'", res.Hits[0].Fields["desc"])
	}

}

func TestDict(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

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

	docx := map[string]interface{}{
		"name": "rose",
		"desc": "googler",
	}
	err = index.Index("x", docx)
	if err != nil {
		t.Error(err)
	}

	dict, err := index.FieldDict("name")
	if err != nil {
		t.Error(err)
	}

	terms := []string{}
	de, err := dict.Next()
	for err == nil && de != nil {
		terms = append(terms, string(de.Term))
		de, err = dict.Next()
	}

	expectedTerms := []string{"jasper", "marty", "rose"}
	if !reflect.DeepEqual(terms, expectedTerms) {
		t.Errorf("expected %v, got %v", expectedTerms, terms)
	}

	err = dict.Close()
	if err != nil {
		t.Fatal(err)
	}

	// test start and end range
	dict, err = index.FieldDictRange("name", []byte("marty"), []byte("rose"))
	if err != nil {
		t.Error(err)
	}

	terms = []string{}
	de, err = dict.Next()
	for err == nil && de != nil {
		terms = append(terms, string(de.Term))
		de, err = dict.Next()
	}

	expectedTerms = []string{"marty", "rose"}
	if !reflect.DeepEqual(terms, expectedTerms) {
		t.Errorf("expected %v, got %v", expectedTerms, terms)
	}

	err = dict.Close()
	if err != nil {
		t.Fatal(err)
	}

	docz := map[string]interface{}{
		"name": "prefix",
		"desc": "bob cat cats catting dog doggy zoo",
	}
	err = index.Index("z", docz)
	if err != nil {
		t.Error(err)
	}

	dict, err = index.FieldDictPrefix("desc", []byte("cat"))
	if err != nil {
		t.Error(err)
	}

	terms = []string{}
	de, err = dict.Next()
	for err == nil && de != nil {
		terms = append(terms, string(de.Term))
		de, err = dict.Next()
	}

	expectedTerms = []string{"cat", "cats", "catting"}
	if !reflect.DeepEqual(terms, expectedTerms) {
		t.Errorf("expected %v, got %v", expectedTerms, terms)
	}

	stats := index.Stats()
	if stats == nil {
		t.Errorf("expected IndexStat, got nil")
	}

	err = dict.Close()
	if err != nil {
		t.Fatal(err)
	}

	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBatchString(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	index, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}

	batch := index.NewBatch()
	err = batch.Index("a", []byte("{}"))
	if err != nil {
		t.Fatal(err)
	}
	batch.Delete("b")
	batch.SetInternal([]byte("c"), []byte{})
	batch.DeleteInternal([]byte("d"))

	batchStr := batch.String()
	if !strings.HasPrefix(batchStr, "Batch (2 ops, 2 internal ops)") {
		t.Errorf("expected to start with Batch (2 ops, 2 internal ops), did not")
	}
	if !strings.Contains(batchStr, "INDEX - 'a'") {
		t.Errorf("expected to contain INDEX - 'a', did not")
	}
	if !strings.Contains(batchStr, "DELETE - 'b'") {
		t.Errorf("expected to contain DELETE - 'b', did not")
	}
	if !strings.Contains(batchStr, "SET INTERNAL - 'c'") {
		t.Errorf("expected to contain SET INTERNAL - 'c', did not")
	}
	if !strings.Contains(batchStr, "DELETE INTERNAL - 'd'") {
		t.Errorf("expected to contain DELETE INTERNAL - 'd', did not")
	}

}

func TestIndexMetadataRaceBug198(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	index, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for {
			_, err := index.DocCount()
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	for i := 0; i < 100; i++ {
		batch := index.NewBatch()
		err = batch.Index("a", []byte("{}"))
		if err != nil {
			t.Fatal(err)
		}
		err = index.Batch(batch)
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestIndexCountMatchSearch(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	index, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			b := index.NewBatch()
			for j := 0; j < 200; j++ {
				id := fmt.Sprintf("%d", (i*200)+j)
				doc := struct {
					Body string
				}{
					Body: "match",
				}
				err := b.Index(id, doc)
				if err != nil {
					t.Fatal(err)
				}
			}
			err := index.Batch(b)
			if err != nil {
				t.Fatal(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	// search for something that should match all documents
	sr, err := index.Search(NewSearchRequest(NewMatchQuery("match")))
	if err != nil {
		t.Fatal(err)
	}

	// get the index document count
	dc, err := index.DocCount()
	if err != nil {
		t.Fatal(err)
	}

	// make sure test is working correctly, doc count should 2000
	if dc != 2000 {
		t.Errorf("expected doc count 2000, got %d", dc)
	}

	// make sure our search found all the documents
	if dc != sr.Total {
		t.Errorf("expected search result total %d to match doc count %d", sr.Total, dc)
	}

	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}
