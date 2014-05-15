package goforestdb

import (
	"github.com/couchbaselabs/bleve/index/store"
	"github.com/couchbaselabs/goforestdb"
)

type ForestDBStore struct {
	path string
	db   *forestdb.Database
}

func Open(path string) (*ForestDBStore, error) {
	rv := ForestDBStore{
		path: path,
	}

	var err error
	rv.db, err = forestdb.Open(path, nil)
	if err != nil {
		return nil, err
	}

	return &rv, nil
}

func (f *ForestDBStore) Get(key []byte) ([]byte, error) {
	return f.db.GetKV(key)
}

func (f *ForestDBStore) Set(key, val []byte) error {
	return f.db.SetKV(key, val)
}

func (f *ForestDBStore) Delete(key []byte) error {
	return f.db.DeleteKV(key)
}

func (f *ForestDBStore) Commit() error {
	return f.db.Commit(forestdb.COMMIT_NORMAL)
}

func (f *ForestDBStore) Close() error {
	return f.db.Close()
}

func (f *ForestDBStore) Iterator(key []byte) store.KVIterator {
	rv := newForestDBIterator(f)
	rv.Seek(key)
	return rv
}

func (f *ForestDBStore) NewBatch() store.KVBatch {
	return newForestDBBatch(f)
}
