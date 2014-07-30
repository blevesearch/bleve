//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build forestdb

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
	res, err := f.db.GetKV(key)
	if err != nil && err != forestdb.RESULT_KEY_NOT_FOUND {
		return nil, err
	}
	return res, nil
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
