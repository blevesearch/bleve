
package rocksdb

import (
	"os"
	"testing"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/store/test"
)

func open(t *testing.T, mo store.MergeOperator) store.KVStore {
	rv, err := New(mo, map[string]interface{}{
		"path":              "test",
		"create_if_missing": true,
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
	err = os.RemoveAll("test")
	if err != nil {
		t.Fatal(err)
	}
}

func TestRocksDBKVCrud(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestKVCrud(t, s)
}

func TestRocksDBReaderIsolation(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestReaderIsolation(t, s)
}

func TestRocksDBReaderOwnsGetBytes(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestReaderOwnsGetBytes(t, s)
}

func TestRocksDBWriterOwnsBytes(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestWriterOwnsBytes(t, s)
}

func TestRocksDBPrefixIterator(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIterator(t, s)
}

func TestRocksDBPrefixIteratorSeek(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIteratorSeek(t, s)
}

func TestRocksDBRangeIterator(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestRangeIterator(t, s)
}

func TestRocksDBRangeIteratorSeek(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestRangeIteratorSeek(t, s)
}

func TestRocksDBMerge(t *testing.T) {
	s := open(t, &test.TestMergeCounter{})
	defer cleanup(t, s)
	test.CommonTestMerge(t, s)
}

func TestRocksDBGet(t *testing.T) {
	s := open(t, &test.TestMergeCounter{})
	defer cleanup(t, s)
	w, err := s.Writer()
	if err != nil {
		t.Fatalf("new writer failed, err %v", err)
	}
	batch := w.NewBatch()
	batch.Set([]byte("key"), []byte("value"))
	err = w.ExecuteBatch(batch)
	if err != nil {
		t.Fatalf("writer batch failed, err %v", err)
	}
	w.Close()
	r, err := s.Reader()
	if err != nil {
		t.Fatalf("new reader failed, err %v", err)
	}
	defer r.Close()
	_, err = r.Get([]byte("key"))
	if err != nil {
		t.Fatalf("get key failed, err %v", err)
	}

	v, err := r.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("get key failed, err %v", err)
	}
	if v != nil {
		t.Fatal("read failed!!!!")
	}
}
