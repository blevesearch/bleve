//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package goleveldb

import (
	"fmt"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const Name = "goleveldb"

type Store struct {
	path string
	opts *opt.Options
	db   *leveldb.DB
	mo   store.MergeOperator

	defaultWriteOptions *opt.WriteOptions
	defaultReadOptions  *opt.ReadOptions
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {

	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}

	opts, err := applyConfig(&opt.Options{}, config)
	if err != nil {
		return nil, err
	}

	db, err := leveldb.OpenFile(path, opts)
	if err != nil {
		return nil, err
	}

	rv := Store{
		path:                path,
		opts:                opts,
		db:                  db,
		mo:                  mo,
		defaultReadOptions:  &opt.ReadOptions{},
		defaultWriteOptions: &opt.WriteOptions{},
	}
	rv.defaultWriteOptions.Sync = true
	return &rv, nil
}

func (ldbs *Store) Close() error {
	return ldbs.db.Close()
}

func (ldbs *Store) Reader() (store.KVReader, error) {
	snapshot, _ := ldbs.db.GetSnapshot()
	return &Reader{
		store:    ldbs,
		snapshot: snapshot,
	}, nil
}

func (ldbs *Store) Writer() (store.KVWriter, error) {
	return &Writer{
		store: ldbs,
	}, nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}
