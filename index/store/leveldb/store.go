//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package leveldb

import (
	"github.com/couchbaselabs/bleve/index/store"
	"github.com/jmhodges/levigo"
)

type LevelDBStore struct {
	path string
	opts *levigo.Options
	db   *levigo.DB
}

func Open(path string, createIfMissing bool) (*LevelDBStore, error) {
	rv := LevelDBStore{
		path: path,
	}

	opts := levigo.NewOptions()
	opts.SetCreateIfMissing(createIfMissing)
	rv.opts = opts

	var err error
	rv.db, err = levigo.Open(rv.path, rv.opts)
	if err != nil {
		return nil, err
	}

	return &rv, nil
}

func (ldbs *LevelDBStore) Get(key []byte) ([]byte, error) {
	return ldbs.db.Get(defaultReadOptions(), key)
}

func (ldbs *LevelDBStore) Set(key, val []byte) error {
	return ldbs.db.Put(defaultWriteOptions(), key, val)
}

func (ldbs *LevelDBStore) Delete(key []byte) error {
	return ldbs.db.Delete(defaultWriteOptions(), key)
}

func (ldbs *LevelDBStore) Commit() error {
	return nil
}

func (ldbs *LevelDBStore) Close() error {
	ldbs.db.Close()
	return nil
}

func (ldbs *LevelDBStore) Iterator(key []byte) store.KVIterator {
	rv := newLevelDBIterator(ldbs)
	rv.Seek(key)
	return rv
}

func (ldbs *LevelDBStore) NewBatch() store.KVBatch {
	return newLevelDBBatch(ldbs)
}
