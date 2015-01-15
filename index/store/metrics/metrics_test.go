//  Copyright (c) 2015 Couchbase, Inc.

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// +build debug

package metrics

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/index/store"
	_ "github.com/blevesearch/bleve/index/store/gtreap"
)

func TestMetricsStore(t *testing.T) {
	s, err := StoreConstructor(map[string]interface{}{})
	if err == nil {
		t.Errorf("expected err when bad config")
	}

	s, err = StoreConstructor(map[string]interface{}{
		"kvStoreName_actual": "some-invalid-kvstore-name",
	})
	if err == nil {
		t.Errorf("expected err when unknown kvStoreName_actual")
	}

	s, err = StoreConstructor(map[string]interface{}{
		"kvStoreName_actual": "gtreap",
	})
	if err != nil {
		t.Fatal(err)
	}

	CommonTestKVStore(t, s)

	b := bytes.NewBuffer(nil)
	s.(*Store).WriteJSON(b)
	if b.Len() <= 0 {
		t.Errorf("expected some output from WriteJSON")
	}
	var m map[string]interface{}
	err = json.Unmarshal(b.Bytes(), &m)
	if err != nil {
		t.Errorf("expected WriteJSON to be unmarshallable")
	}
	if len(m) <= 0 {
		t.Errorf("expected some entries")
	}

	b = bytes.NewBuffer(nil)
	s.(*Store).WriteCSVHeader(b)
	if b.Len() <= 0 {
		t.Errorf("expected some output from WriteCSVHeader")
	}

	b = bytes.NewBuffer(nil)
	s.(*Store).WriteCSV(b)
	if b.Len() <= 0 {
		t.Errorf("expected some output from WriteCSV")
	}
}

func TestReaderIsolation(t *testing.T) {
	s, err := StoreConstructor(map[string]interface{}{
		"kvStoreName_actual": "gtreap",
	})
	if err != nil {
		t.Fatal(err)
	}

	CommonTestReaderIsolation(t, s)
}

func CommonTestKVStore(t *testing.T, s store.KVStore) {

	writer, err := s.Writer()
	if err != nil {
		t.Error(err)
	}
	err = writer.Set([]byte("a"), []byte("val-a"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Set([]byte("z"), []byte("val-z"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Delete([]byte("z"))
	if err != nil {
		t.Fatal(err)
	}

	batch := writer.NewBatch()
	batch.Set([]byte("b"), []byte("val-b"))
	batch.Set([]byte("c"), []byte("val-c"))
	batch.Set([]byte("d"), []byte("val-d"))
	batch.Set([]byte("e"), []byte("val-e"))
	batch.Set([]byte("f"), []byte("val-f"))
	batch.Set([]byte("g"), []byte("val-g"))
	batch.Set([]byte("h"), []byte("val-h"))
	batch.Set([]byte("i"), []byte("val-i"))
	batch.Set([]byte("j"), []byte("val-j"))

	err = batch.Execute()
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()

	reader, err := s.Reader()
	if err != nil {
		t.Error(err)
	}
	defer reader.Close()
	it := reader.Iterator([]byte("b"))
	key, val, valid := it.Current()
	if !valid {
		t.Fatalf("valid false, expected true")
	}
	if string(key) != "b" {
		t.Fatalf("expected key b, got %s", key)
	}
	if string(val) != "val-b" {
		t.Fatalf("expected value val-b, got %s", val)
	}

	it.Next()
	key, val, valid = it.Current()
	if !valid {
		t.Fatalf("valid false, expected true")
	}
	if string(key) != "c" {
		t.Fatalf("expected key c, got %s", key)
	}
	if string(val) != "val-c" {
		t.Fatalf("expected value val-c, got %s", val)
	}

	it.Seek([]byte("i"))
	key, val, valid = it.Current()
	if !valid {
		t.Fatalf("valid false, expected true")
	}
	if string(key) != "i" {
		t.Fatalf("expected key i, got %s", key)
	}
	if string(val) != "val-i" {
		t.Fatalf("expected value val-i, got %s", val)
	}

	it.Close()
}

func CommonTestReaderIsolation(t *testing.T, s store.KVStore) {
	// insert a kv pair
	writer, err := s.Writer()
	if err != nil {
		t.Error(err)
	}
	err = writer.Set([]byte("a"), []byte("val-a"))
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()

	// create an isolated reader
	reader, err := s.Reader()
	if err != nil {
		t.Error(err)
	}
	defer reader.Close()

	// verify that we see the value already inserted
	val, err := reader.Get([]byte("a"))
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(val, []byte("val-a")) {
		t.Errorf("expected val-a, got nil")
	}

	// verify that an iterator sees it
	count := 0
	it := reader.Iterator([]byte{0})
	defer it.Close()
	for it.Valid() {
		it.Next()
		count++
	}
	if count != 1 {
		t.Errorf("expected iterator to see 1, saw %d", count)
	}

	// add something after the reader was created
	writer, err = s.Writer()
	if err != nil {
		t.Error(err)
	}
	err = writer.Set([]byte("b"), []byte("val-b"))
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()

	// ensure that a newer reader sees it
	newReader, err := s.Reader()
	if err != nil {
		t.Error(err)
	}
	defer newReader.Close()
	val, err = newReader.Get([]byte("b"))
	if err != nil {
		t.Error(err)
	}
	if !reflect.DeepEqual(val, []byte("val-b")) {
		t.Errorf("expected val-b, got nil")
	}

	// ensure that the director iterator sees it
	count = 0
	it = newReader.Iterator([]byte{0})
	defer it.Close()
	for it.Valid() {
		it.Next()
		count++
	}
	if count != 2 {
		t.Errorf("expected iterator to see 2, saw %d", count)
	}

	// but that the isolated reader does not
	val, err = reader.Get([]byte("b"))
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}

	// and ensure that the iterator on the isolated reader also does not
	count = 0
	it = reader.Iterator([]byte{0})
	defer it.Close()
	for it.Valid() {
		it.Next()
		count++
	}
	if count != 1 {
		t.Errorf("expected iterator to see 1, saw %d", count)
	}

}
