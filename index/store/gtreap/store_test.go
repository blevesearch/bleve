//  Copyright (c) 2014 Couchbase, Inc.
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

package gtreap

import (
	"bytes"
	"testing"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/store/test"
)

func open(t *testing.T, mo store.MergeOperator) store.KVStore {
	rv, err := New(mo, map[string]interface{}{
		"path": "",
	})
	if err != nil {
		t.Fatal(err)
	}
	return rv
}

func cleanup(t *testing.T, s store.KVStore) {
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestGTreapKVCrud(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestKVCrud(t, s)
}

func TestGTreapReaderIsolation(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestReaderIsolation(t, s)
}

func TestGTreapReaderOwnsGetBytes(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestReaderOwnsGetBytes(t, s)
}

func TestGTreapWriterOwnsBytes(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestWriterOwnsBytes(t, s)
}

func TestGTreapPrefixIterator(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIterator(t, s)
}

func TestGTreapPrefixIteratorSeek(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIteratorSeek(t, s)
}

func TestGTreapRangeIterator(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestRangeIterator(t, s)
}

func TestGTreapRangeIteratorSeek(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestRangeIteratorSeek(t, s)
}

func TestGTreapMerge(t *testing.T) {
	s := open(t, &test.TestMergeCounter{})
	defer cleanup(t, s)
	test.CommonTestMerge(t, s)
}

func TestGTreapCompact(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)

	writer, err := s.Writer()
	if err != nil {
		t.Error(err)
	}

	batch := writer.NewBatch()
	// Should preserve non-dictionary row (key doesn't start with 'd').
	batch.Set([]byte("a1"), []byte{0})
	// Should delete for dictionary row with zero reference.
	batch.Set([]byte("d1"), []byte{0})
	batch.Set([]byte("d2"), []byte{0})
	// Should preserve for dictionary row with non-zero reference.
	batch.Set([]byte("d3"), []byte{1})

	err = writer.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}

	validate(t, s, []byte("a1"), []byte{0})
	validate(t, s, []byte("d1"), []byte{0})
	validate(t, s, []byte("d2"), []byte{0})
	validate(t, s, []byte("d3"), []byte{1})

	if err := s.(*Store).Compact(); err != nil {
		t.Fatal(err)
	}

	validate(t, s, []byte("a1"), []byte{0})
	validate(t, s, []byte("d1"), nil)
	validate(t, s, []byte("d2"), nil)
	validate(t, s, []byte("d3"), []byte{1})
}

func validate(t *testing.T, s store.KVStore, key []byte, value []byte) {
	reader, err := s.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()

	got, err := reader.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, value) {
		t.Errorf("key %v value got %v, want %v", string(key), got, value)
	}
}
