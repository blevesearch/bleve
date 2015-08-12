//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build rocksdb

package rocksdb

import (
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/tecbot/gorocksdb"
)

const Name = "rocksdb"

type Store struct {
	path   string
	opts   *gorocksdb.Options
	db     *gorocksdb.DB
	writer sync.Mutex
}

func New(path string, config map[string]interface{}) (*Store, error) {
	rv := Store{
		path: path,
		opts: gorocksdb.NewDefaultOptions(),
	}

	_, err := applyConfig(rv.opts, config)
	if err != nil {
		return nil, err
	}

	return &rv, nil
}

func (ldbs *Store) Open() error {
	var err error
	ldbs.db, err = gorocksdb.OpenDb(ldbs.opts, ldbs.path)
	if err != nil {
		return err
	}
	return nil
}

func (ldbs *Store) SetMergeOperator(mo store.MergeOperator) {
	ldbs.opts.SetMergeOperator(mo)
}

func (ldbs *Store) get(key []byte) ([]byte, error) {
	options := defaultReadOptions()
	b, err := ldbs.db.Get(options, key)
	return b.Data(), err
}

func (ldbs *Store) getWithSnapshot(key []byte, snapshot *gorocksdb.Snapshot) ([]byte, error) {
	options := defaultReadOptions()
	options.SetSnapshot(snapshot)
	b, err := ldbs.db.Get(options, key)
	return b.Data(), err
}

func (ldbs *Store) set(key, val []byte) error {
	ldbs.writer.Lock()
	defer ldbs.writer.Unlock()
	return ldbs.setlocked(key, val)
}

func (ldbs *Store) setlocked(key, val []byte) error {
	options := defaultWriteOptions()
	err := ldbs.db.Put(options, key, val)
	return err
}

func (ldbs *Store) delete(key []byte) error {
	ldbs.writer.Lock()
	defer ldbs.writer.Unlock()
	return ldbs.deletelocked(key)
}

func (ldbs *Store) deletelocked(key []byte) error {
	options := defaultWriteOptions()
	err := ldbs.db.Delete(options, key)
	return err
}

func (ldbs *Store) Close() error {
	ldbs.db.Close()
	return nil
}

func (ldbs *Store) iterator(key []byte) store.KVIterator {
	rv := newIterator(ldbs)
	rv.Seek(key)
	return rv
}

func (ldbs *Store) Reader() (store.KVReader, error) {
	return newReader(ldbs)
}

func (ldbs *Store) Writer() (store.KVWriter, error) {
	return newWriter(ldbs)
}

func StoreConstructor(config map[string]interface{}) (store.KVStore, error) {
	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}
	return New(path, config)
}

func init() {
	registry.RegisterKVStore(Name, StoreConstructor)
}

func applyConfig(o *gorocksdb.Options, config map[string]interface{}) (
	*gorocksdb.Options, error) {

	cim, ok := config["create_if_missing"].(bool)
	if ok {
		o.SetCreateIfMissing(cim)
	}

	eie, ok := config["error_if_exists"].(bool)
	if ok {
		o.SetErrorIfExists(eie)
	}

	wbs, ok := config["write_buffer_size"].(float64)
	if ok {
		o.SetWriteBufferSize(int(wbs))
	}

	mof, ok := config["max_open_files"].(float64)
	if ok {
		o.SetMaxOpenFiles(int(mof))
	}

	tt, ok := config["total_threads"].(float64)
	if ok {
		o.IncreaseParallelism(int(tt))
	}

	return o, nil
}
