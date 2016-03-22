//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// Package boltdb implements a store.KVStore on top of BoltDB. It supports the
// following options:
//
// "bucket" (string): the name of BoltDB bucket to use, defaults to "bleve".
//
// "nosync" (bool): if true, set boltdb.DB.NoSync to true. It speeds up index
// operations in exchange of losing integrity guarantees if indexation aborts
// without closing the index. Use it when rebuilding indexes from zero.
package boltdb

import (
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/boltdb/bolt"
)

const Name = "boltdb"

type Store struct {
	path   string
	bucket string
	db     *bolt.DB
	noSync bool
	mo     store.MergeOperator
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}

	bucket, ok := config["bucket"].(string)
	if !ok {
		bucket = "bleve"
	}

	noSync, _ := config["nosync"].(bool)

	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	db.NoSync = noSync

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))

		return err
	})
	if err != nil {
		return nil, err
	}

	rv := Store{
		path:   path,
		bucket: bucket,
		db:     db,
		mo:     mo,
		noSync: noSync,
	}
	return &rv, nil
}

func (bs *Store) Close() error {
	return bs.db.Close()
}

func (bs *Store) Reader() (store.KVReader, error) {
	tx, err := bs.db.Begin(false)
	if err != nil {
		return nil, err
	}
	return &Reader{
		store:  bs,
		tx:     tx,
		bucket: tx.Bucket([]byte(bs.bucket)),
	}, nil
}

func (bs *Store) Writer() (store.KVWriter, error) {
	return &Writer{
		store: bs,
	}, nil
}

func (bs *Store) Stats() json.Marshaler {
	return &stats{
		s: bs,
	}
}

func init() {
	registry.RegisterKVStore(Name, New)
}
