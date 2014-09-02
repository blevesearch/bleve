//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package boltdb

import (
	"fmt"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/boltdb/bolt"
)

const Name = "boltdb"

type BoltDBStore struct {
	path   string
	bucket string
	db     *bolt.DB
}

func Open(path string, bucket string) (*BoltDBStore, error) {
	rv := BoltDBStore{
		path:   path,
		bucket: bucket,
	}

	var err error
	rv.db, err = bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}

	err = rv.db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(rv.bucket))

		return err
	})
	if err != nil {
		return nil, err
	}

	return &rv, nil
}

func (bs *BoltDBStore) Get(key []byte) ([]byte, error) {
	var rv []byte = nil

	err := bs.db.View(func(tx *bolt.Tx) error {
		rv = tx.Bucket([]byte(bs.bucket)).Get(key)

		return nil
	})

	return rv, err
}

func (bs *BoltDBStore) Set(key, val []byte) error {
	return bs.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bs.bucket)).Put(key, val)
	})
}

func (bs *BoltDBStore) Delete(key []byte) error {
	return bs.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(bs.bucket)).Delete(key)
	})
}

func (bs *BoltDBStore) Commit() error {
	return nil
}

func (bs *BoltDBStore) Close() error {
	return bs.db.Close()
}

func (bs *BoltDBStore) Iterator(key []byte) store.KVIterator {
	rv := newBoltDBIterator(bs)
	rv.Seek(key)
	return rv
}

func (bs *BoltDBStore) NewBatch() store.KVBatch {
	return newBoltDBBatch(bs)
}

func StoreConstructor(config map[string]interface{}) (store.KVStore, error) {
	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}

	bucket, ok := config["bucket"].(string)
	if !ok {
		bucket = "bleve"
	}

	return Open(path, bucket)
}

func init() {
	registry.RegisterKVStore(Name, StoreConstructor)
}
