//  Copyright (c) 2014 Couchbase, Inc.

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package cznicb

import (
	"testing"

	"github.com/blevesearch/bleve/index/store"
)

func TestCznicBStore(t *testing.T) {
	s, err := StoreConstructor(nil)
	if err != nil {
		t.Fatal(err)
	}

	CommonTestKVStore(t, s)
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
	v, err := writer.Get([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "val-a" {
		t.Errorf("expected val-a")
	}
	v, err = writer.Get([]byte("not-there"))
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Errorf("expected nil v")
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
