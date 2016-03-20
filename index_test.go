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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/context"

	"encoding/json"
	"strconv"

	"github.com/blevesearch/bleve/analysis/analyzers/keyword_analyzer"
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store/null"
	"github.com/blevesearch/bleve/search"
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

type slowQuery struct {
	actual Query
	delay  time.Duration
}

func (s *slowQuery) Boost() float64 {
	return s.actual.Boost()
}

func (s *slowQuery) SetBoost(b float64) Query {
	return s.actual.SetBoost(b)
}

func (s *slowQuery) Field() string {
	return s.actual.Field()
}

func (s *slowQuery) SetField(f string) Query {
	return s.actual.SetField(f)
}

func (s *slowQuery) Searcher(i index.IndexReader, m *IndexMapping, explain bool) (search.Searcher, error) {
	time.Sleep(s.delay)
	return s.actual.Searcher(i, m, explain)
}

func (s *slowQuery) Validate() error {
	return s.actual.Validate()
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

	sq := &slowQuery{
		actual: query,
		delay:  50 * time.Millisecond, // on Windows timer resolution is 15ms
	}
	req.Query = sq
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
		"bool": true,
		"num":  float64(1),
	}
	err = index.Index("a", doca)
	if err != nil {
		t.Error(err)
	}

	q := NewTermQuery("marty")
	req := NewSearchRequest(q)
	req.Fields = []string{"name", "desc", "bool", "num"}
	res, err := index.Search(req)
	if err != nil {
		t.Error(err)
	}

	if len(res.Hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(res.Hits))
	}
	if res.Hits[0].Fields["name"] != "Marty" {
		t.Errorf("expected 'Marty' got '%s'", res.Hits[0].Fields["name"])
	}
	if res.Hits[0].Fields["desc"] != "GopherCON India" {
		t.Errorf("expected 'GopherCON India' got '%s'", res.Hits[0].Fields["desc"])
	}
	if res.Hits[0].Fields["num"] != float64(1) {
		t.Errorf("expected '1' got '%v'", res.Hits[0].Fields["num"])
	}
	if res.Hits[0].Fields["bool"] != true {
		t.Errorf("expected 'true' got '%v'", res.Hits[0].Fields["bool"])
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
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

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
	defer func() {
		err := index.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				_, err := index.DocCount()
				if err != nil {
					t.Fatal(err)
				}
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
	close(done)
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

func TestBatchReset(t *testing.T) {
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
	err = batch.Index("k1", struct {
		Body string
	}{
		Body: "v1",
	})
	if err != nil {
		t.Error(err)
	}
	batch.Delete("k2")
	batch.SetInternal([]byte("k3"), []byte("v3"))
	batch.DeleteInternal([]byte("k4"))

	if batch.Size() != 4 {
		t.Logf("%v", batch)
		t.Errorf("expected batch size 4, got %d", batch.Size())
	}

	batch.Reset()

	if batch.Size() != 0 {
		t.Errorf("expected batch size 0 after reset, got %d", batch.Size())
	}

	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDocumentFieldArrayPositions(t *testing.T) {
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

	// index a document with an array of strings
	err = index.Index("k", struct {
		Messages []string
	}{
		Messages: []string{
			"first",
			"second",
			"third",
			"last",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// load the document
	doc, err := index.Document("k")
	if err != nil {
		t.Fatal(err)
	}

	for _, f := range doc.Fields {
		if reflect.DeepEqual(f.Value(), []byte("first")) {
			ap := f.ArrayPositions()
			if len(ap) < 1 {
				t.Errorf("expected an array position, got none")
				continue
			}
			if ap[0] != 0 {
				t.Errorf("expected 'first' in array position 0, got %d", ap[0])
			}
		}
		if reflect.DeepEqual(f.Value(), []byte("second")) {
			ap := f.ArrayPositions()
			if len(ap) < 1 {
				t.Errorf("expected an array position, got none")
				continue
			}
			if ap[0] != 1 {
				t.Errorf("expected 'second' in array position 1, got %d", ap[0])
			}
		}
		if reflect.DeepEqual(f.Value(), []byte("third")) {
			ap := f.ArrayPositions()
			if len(ap) < 1 {
				t.Errorf("expected an array position, got none")
				continue
			}
			if ap[0] != 2 {
				t.Errorf("expected 'third' in array position 2, got %d", ap[0])
			}
		}
		if reflect.DeepEqual(f.Value(), []byte("last")) {
			ap := f.ArrayPositions()
			if len(ap) < 1 {
				t.Errorf("expected an array position, got none")
				continue
			}
			if ap[0] != 3 {
				t.Errorf("expected 'last' in array position 3, got %d", ap[0])
			}
		}
	}

	// now index a document in the same field with a single string
	err = index.Index("k2", struct {
		Messages string
	}{
		Messages: "only",
	})
	if err != nil {
		t.Fatal(err)
	}

	// load the document
	doc, err = index.Document("k2")
	if err != nil {
		t.Fatal(err)
	}

	for _, f := range doc.Fields {
		if reflect.DeepEqual(f.Value(), []byte("only")) {
			ap := f.ArrayPositions()
			if len(ap) != 0 {
				t.Errorf("expected no array positions, got %d", len(ap))
				continue
			}
		}
	}

	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestKeywordSearchBug207(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	f := NewTextFieldMapping()
	f.Analyzer = keyword_analyzer.Name

	m := NewIndexMapping()
	m.DefaultMapping = NewDocumentMapping()
	m.DefaultMapping.AddFieldMappingsAt("Body", f)

	index, err := New("testidx", m)
	if err != nil {
		t.Fatal(err)
	}

	doc1 := struct {
		Body string
	}{
		Body: "a555c3bb06f7a127cda000005",
	}

	err = index.Index("a", doc1)
	if err != nil {
		t.Fatal(err)
	}

	doc2 := struct {
		Body string
	}{
		Body: "555c3bb06f7a127cda000005",
	}

	err = index.Index("b", doc2)
	if err != nil {
		t.Fatal(err)
	}

	// now search for these terms
	sreq := NewSearchRequest(NewTermQuery("a555c3bb06f7a127cda000005"))
	sres, err := index.Search(sreq)
	if err != nil {
		t.Fatal(err)
	}
	if sres.Total != 1 {
		t.Errorf("expected 1 result, got %d", sres.Total)
	}
	if sres.Hits[0].ID != "a" {
		t.Errorf("expecated id 'a', got '%s'", sres.Hits[0].ID)
	}

	sreq = NewSearchRequest(NewTermQuery("555c3bb06f7a127cda000005"))
	sres, err = index.Search(sreq)
	if err != nil {
		t.Fatal(err)
	}
	if sres.Total != 1 {
		t.Errorf("expected 1 result, got %d", sres.Total)
	}
	if sres.Hits[0].ID != "b" {
		t.Errorf("expecated id 'b', got '%s'", sres.Hits[0].ID)
	}

	// now do the same searches using query strings
	sreq = NewSearchRequest(NewQueryStringQuery("Body:a555c3bb06f7a127cda000005"))
	sres, err = index.Search(sreq)
	if err != nil {
		t.Fatal(err)
	}
	if sres.Total != 1 {
		t.Errorf("expected 1 result, got %d", sres.Total)
	}
	if sres.Hits[0].ID != "a" {
		t.Errorf("expecated id 'a', got '%s'", sres.Hits[0].ID)
	}

	sreq = NewSearchRequest(NewQueryStringQuery(`Body:555c3bb06f7a127cda000005`))
	sres, err = index.Search(sreq)
	if err != nil {
		t.Fatal(err)
	}
	if sres.Total != 1 {
		t.Errorf("expected 1 result, got %d", sres.Total)
	}
	if sres.Hits[0].ID != "b" {
		t.Errorf("expecated id 'b', got '%s'", sres.Hits[0].ID)
	}

	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestTermVectorArrayPositions(t *testing.T) {
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

	// index a document with an array of strings
	err = index.Index("k", struct {
		Messages []string
	}{
		Messages: []string{
			"first",
			"second",
			"third",
			"last",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// search for this document in all field
	tq := NewTermQuery("second")
	tsr := NewSearchRequest(tq)
	results, err := index.Search(tsr)
	if err != nil {
		t.Fatal(err)
	}
	if results.Total != 1 {
		t.Fatalf("expected 1 result, got %d", results.Total)
	}
	if len(results.Hits[0].Locations["Messages"]["second"]) < 1 {
		t.Fatalf("expected at least one location")
	}
	if len(results.Hits[0].Locations["Messages"]["second"][0].ArrayPositions) < 1 {
		t.Fatalf("expected at least one location array position")
	}
	if results.Hits[0].Locations["Messages"]["second"][0].ArrayPositions[0] != 1 {
		t.Fatalf("expected array position 1, got %f", results.Hits[0].Locations["Messages"]["second"][0].ArrayPositions[0])
	}

	// repeat search for this document in Messages field
	tq2 := NewTermQuery("third").SetField("Messages")
	tsr = NewSearchRequest(tq2)
	results, err = index.Search(tsr)
	if err != nil {
		t.Fatal(err)
	}
	if results.Total != 1 {
		t.Fatalf("expected 1 result, got %d", results.Total)
	}
	if len(results.Hits[0].Locations["Messages"]["third"]) < 1 {
		t.Fatalf("expected at least one location")
	}
	if len(results.Hits[0].Locations["Messages"]["third"][0].ArrayPositions) < 1 {
		t.Fatalf("expected at least one location array position")
	}
	if results.Hits[0].Locations["Messages"]["third"][0].ArrayPositions[0] != 2 {
		t.Fatalf("expected array position 2, got %f", results.Hits[0].Locations["Messages"]["third"][0].ArrayPositions[0])
	}

	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDocumentStaticMapping(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	m := NewIndexMapping()
	m.DefaultMapping = NewDocumentStaticMapping()
	m.DefaultMapping.AddFieldMappingsAt("Text", NewTextFieldMapping())
	m.DefaultMapping.AddFieldMappingsAt("Date", NewDateTimeFieldMapping())
	m.DefaultMapping.AddFieldMappingsAt("Numeric", NewNumericFieldMapping())

	index, err := New("testidx", m)
	if err != nil {
		t.Fatal(err)
	}

	doc1 := struct {
		Text           string
		IgnoredText    string
		Numeric        float64
		IgnoredNumeric float64
		Date           time.Time
		IgnoredDate    time.Time
	}{
		Text:           "valid text",
		IgnoredText:    "ignored text",
		Numeric:        10,
		IgnoredNumeric: 20,
		Date:           time.Unix(1, 0),
		IgnoredDate:    time.Unix(2, 0),
	}

	err = index.Index("a", doc1)
	if err != nil {
		t.Fatal(err)
	}

	fields, err := index.Fields()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(fields)
	expectedFields := []string{"Date", "Numeric", "Text", "_all"}
	if len(fields) != len(expectedFields) {
		t.Fatalf("invalid field count: %d", len(fields))
	}
	for i, expected := range expectedFields {
		if expected != fields[i] {
			t.Fatalf("unexpected field[%d]: %s", i, fields[i])
		}
	}

	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestIndexEmptyDocId(t *testing.T) {
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

	doc := map[string]interface{}{
		"body": "nodocid",
	}

	err = index.Index("", doc)
	if err != ErrorEmptyID {
		t.Errorf("expect index empty doc id to fail")
	}

	err = index.Delete("")
	if err != ErrorEmptyID {
		t.Errorf("expect delete empty doc id to fail")
	}

	batch := index.NewBatch()
	err = batch.Index("", doc)
	if err != ErrorEmptyID {
		t.Errorf("expect index empty doc id in batch to fail")
	}

	batch.Delete("")
	if batch.Size() > 0 {
		t.Errorf("expect delete empty doc id in batch to be ignored")
	}
}

func TestDateTimeFieldMappingIssue287(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	f := NewDateTimeFieldMapping()

	m := NewIndexMapping()
	m.DefaultMapping = NewDocumentMapping()
	m.DefaultMapping.AddFieldMappingsAt("Date", f)

	index, err := New("testidx", m)
	if err != nil {
		t.Fatal(err)
	}

	type doc struct {
		Date time.Time
	}

	now := time.Now()

	// 3hr ago to 1hr ago
	for i := 0; i < 3; i++ {
		d := doc{now.Add(time.Duration((i - 3)) * time.Hour)}

		docJson, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}

		err = index.Index(strconv.FormatInt(int64(i), 10), docJson)
		if err != nil {
			t.Fatal(err)
		}
	}

	// search range across all docs
	start := now.Add(-4 * time.Hour).Format(time.RFC3339)
	end := now.Format(time.RFC3339)
	sreq := NewSearchRequest(NewDateRangeQuery(&start, &end))
	sres, err := index.Search(sreq)
	if err != nil {
		t.Fatal(err)
	}
	if sres.Total != 3 {
		t.Errorf("expected 3 results, got %d", sres.Total)
	}

	// search range includes only oldest
	start = now.Add(-4 * time.Hour).Format(time.RFC3339)
	end = now.Add(-121 * time.Minute).Format(time.RFC3339)
	sreq = NewSearchRequest(NewDateRangeQuery(&start, &end))
	sres, err = index.Search(sreq)
	if err != nil {
		t.Fatal(err)
	}
	if sres.Total != 1 {
		t.Errorf("expected 1 results, got %d", sres.Total)
	}
	if sres.Total > 0 && sres.Hits[0].ID != "0" {
		t.Errorf("expecated id '0', got '%s'", sres.Hits[0].ID)
	}

	// search range includes only newest
	start = now.Add(-61 * time.Minute).Format(time.RFC3339)
	end = now.Format(time.RFC3339)
	sreq = NewSearchRequest(NewDateRangeQuery(&start, &end))
	sres, err = index.Search(sreq)
	if err != nil {
		t.Fatal(err)
	}
	if sres.Total != 1 {
		t.Errorf("expected 1 results, got %d", sres.Total)
	}
	if sres.Total > 0 && sres.Hits[0].ID != "2" {
		t.Errorf("expecated id '2', got '%s'", sres.Hits[0].ID)
	}

	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestDocumentFieldArrayPositionsBug295(t *testing.T) {
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

	// index a document with an array of strings
	err = index.Index("k", struct {
		Messages []string
		Another  string
		MoreData []string
	}{
		Messages: []string{
			"bleve",
			"bleve",
		},
		Another: "text",
		MoreData: []string{
			"a",
			"b",
			"c",
			"bleve",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// search for it in the messages field
	tq := NewTermQuery("bleve").SetField("Messages")
	tsr := NewSearchRequest(tq)
	results, err := index.Search(tsr)
	if err != nil {
		t.Fatal(err)
	}
	if results.Total != 1 {
		t.Fatalf("expected 1 result, got %d", results.Total)
	}
	if len(results.Hits[0].Locations["Messages"]["bleve"]) != 2 {
		t.Fatalf("expected 2 locations of 'bleve', got %d", len(results.Hits[0].Locations["Messages"]["bleve"]))
	}
	if results.Hits[0].Locations["Messages"]["bleve"][0].ArrayPositions[0] != 0 {
		t.Errorf("expected array position to be 0")
	}
	if results.Hits[0].Locations["Messages"]["bleve"][1].ArrayPositions[0] != 1 {
		t.Errorf("expected array position to be 1")
	}

	// search for it in all
	tq = NewTermQuery("bleve")
	tsr = NewSearchRequest(tq)
	results, err = index.Search(tsr)
	if err != nil {
		t.Fatal(err)
	}
	if results.Total != 1 {
		t.Fatalf("expected 1 result, got %d", results.Total)
	}
	if len(results.Hits[0].Locations["Messages"]["bleve"]) != 2 {
		t.Fatalf("expected 2 locations of 'bleve', got %d", len(results.Hits[0].Locations["Messages"]["bleve"]))
	}
	if results.Hits[0].Locations["Messages"]["bleve"][0].ArrayPositions[0] != 0 {
		t.Errorf("expected array position to be 0")
	}
	if results.Hits[0].Locations["Messages"]["bleve"][1].ArrayPositions[0] != 1 {
		t.Errorf("expected array position to be 1")
	}

	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestBooleanFieldMappingIssue109(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()

	m := NewIndexMapping()
	m.DefaultMapping = NewDocumentMapping()
	m.DefaultMapping.AddFieldMappingsAt("Bool", NewBooleanFieldMapping())

	index, err := New("testidx", m)
	if err != nil {
		t.Fatal(err)
	}

	type doc struct {
		Bool bool
	}
	err = index.Index("true", &doc{Bool: true})
	if err != nil {
		t.Fatal(err)
	}
	err = index.Index("false", &doc{Bool: false})
	if err != nil {
		t.Fatal(err)
	}

	sreq := NewSearchRequest(NewBoolFieldQuery(true).SetField("Bool"))
	sres, err := index.Search(sreq)
	if err != nil {
		t.Fatal(err)
	}
	if sres.Total != 1 {
		t.Errorf("expected 1 results, got %d", sres.Total)
	}

	sreq = NewSearchRequest(NewBoolFieldQuery(false).SetField("Bool"))
	sres, err = index.Search(sreq)
	if err != nil {
		t.Fatal(err)
	}
	if sres.Total != 1 {
		t.Errorf("expected 1 results, got %d", sres.Total)
	}

	sreq = NewSearchRequest(NewBoolFieldQuery(true))
	sres, err = index.Search(sreq)
	if err != nil {
		t.Fatal(err)
	}
	if sres.Total != 1 {
		t.Errorf("expected 1 results, got %d", sres.Total)
	}

	err = index.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSearchTimeout(t *testing.T) {
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

	// first run a search with an absurdly long timeout (should succeeed)
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	query := NewTermQuery("water")
	req := NewSearchRequest(query)
	_, err = index.SearchInContext(ctx, req)
	if err != nil {
		t.Fatal(err)
	}

	// now run a search again with an absurdly low timeout (should timeout)
	ctx, _ = context.WithTimeout(context.Background(), 1*time.Microsecond)
	sq := &slowQuery{
		actual: query,
		delay:  50 * time.Millisecond, // on Windows timer resolution is 15ms
	}
	req.Query = sq
	_, err = index.SearchInContext(ctx, req)
	if err != context.DeadlineExceeded {
		t.Fatalf("exected %v, got: %v", context.DeadlineExceeded, err)
	}

	// now run a search with a long timeout, but with a long query, and cancel it
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	sq = &slowQuery{
		actual: query,
		delay:  100 * time.Millisecond, // on Windows timer resolution is 15ms
	}
	req = NewSearchRequest(sq)
	cancel()
	_, err = index.SearchInContext(ctx, req)
	if err != context.Canceled {
		t.Fatalf("exected %v, got: %v", context.Canceled, err)
	}
}

// TestConfigCache exposes a concurrent map write with go 1.6
func TestConfigCache(t *testing.T) {
	for i := 0; i < 100; i++ {
		go func() {
			_, err := Config.Cache.HighlighterNamed(Config.DefaultHighlighter)
			if err != nil {
				t.Error(err)
			}
		}()
	}
}

func TestBatchRaceBug260(t *testing.T) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			t.Fatal(err)
		}
	}()
	i, err := New("testidx", NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	b := i.NewBatch()
	err = b.Index("1", 1)
	if err != nil {
		t.Fatal(err)
	}
	err = i.Batch(b)
	if err != nil {
		t.Fatal(err)
	}
	b.Reset()
	err = b.Index("2", 2)
	if err != nil {
		t.Fatal(err)
	}
	err = i.Batch(b)
	if err != nil {
		t.Fatal(err)
	}
	b.Reset()
}

func BenchmarkBatchOverhead(b *testing.B) {
	defer func() {
		err := os.RemoveAll("testidx")
		if err != nil {
			b.Fatal(err)
		}
	}()
	m := NewIndexMapping()
	i, err := NewUsing("testidx", m, Config.DefaultIndexType, null.Name, nil)
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		// put 1000 items in a batch
		batch := i.NewBatch()
		for i := 0; i < 1000; i++ {
			err = batch.Index(fmt.Sprintf("%d", i), map[string]interface{}{"name": "bleve"})
			if err != nil {
				b.Fatal(err)
			}
		}
		err = i.Batch(batch)
		if err != nil {
			b.Fatal(err)
		}
		batch.Reset()
	}
}
