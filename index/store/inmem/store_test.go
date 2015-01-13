//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package inmem

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/index/store"
)

func TestStore(t *testing.T) {
	s, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	CommonTestKVStore(t, s)
}

func TestReaderIsolation(t *testing.T) {
	s, err := Open()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	CommonTestReaderIsolation(t, s)
	CommonTestReaderIsolationIteratorNext(t, s)
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

func CommonTestReaderIsolationIteratorNext(t *testing.T, s store.KVStore) {
	// store already has keys a and b

	// add key c
	writer, err := s.Writer()
	if err != nil {
		t.Error(err)
	}
	err = writer.Set([]byte("c"), []byte("val-c"))
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

	// delete keys c then b
	writer, err = s.Writer()
	if err != nil {
		t.Error(err)
	}
	err = writer.Delete([]byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Delete([]byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Set([]byte("c"), []byte("val-c"))
	if err != nil {
		t.Fatal(err)
	}
	writer.Close()

	// get iterator from reader
	iter := reader.Iterator([]byte(""))
	// seek to a
	iter.Seek([]byte("a"))
	k, v, valid := iter.Current()
	if !valid {
		t.Errorf("expected iterator valid, got invalid")
	}
	if string(k) != "a" {
		t.Errorf("expected key 'a', got '%s'", k)
	}
	if string(v) != "val-a" {
		t.Errorf("expected value 'val-a', got '%s'", v)
	}

	// now call next
	iter.Next()

	// now check where we are
	k, v, valid = iter.Current()
	if !valid {
		t.Errorf("expected iterator valid, got invalid")
	}
	if string(k) != "b" {
		t.Errorf("expected key 'b', got '%s'", k)
	}
	if string(v) != "val-b" {
		t.Errorf("expected value 'val-b', got '%s'", v)
	}

	// now call next
	iter.Next()

	k, v, valid = iter.Current()
	if !valid {
		t.Errorf("expected iterator valid, got invalid")
	}
	if string(k) != "c" {
		t.Errorf("expected key 'c', got '%s'", k)
	}
	if string(v) != "val-c" {
		t.Errorf("expected value 'val-c', got '%s'", v)
	}
}
