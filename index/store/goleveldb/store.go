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
	"sync"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/filter"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const Name = "goleveldb"

type Store struct {
	path   string
	opts   *opt.Options
	db     *leveldb.DB
	writer sync.Mutex
}

func Open(path string, config map[string]interface{}) (*Store, error) {
	rv := Store{
		path: path,
		opts: &opt.Options{},
	}

	applyConfig(rv.opts, config)

	var err error
	rv.db, err = leveldb.OpenFile(rv.path, rv.opts)
	if err != nil {
		return nil, err
	}

	return &rv, nil
}

func (ldbs *Store) get(key []byte) ([]byte, error) {
	options := defaultReadOptions()
	b, err := ldbs.db.Get(key, options)
	return b, err
}

func (ldbs *Store) getWithSnapshot(key []byte, snapshot *leveldb.Snapshot) ([]byte, error) {
	options := defaultReadOptions()
	b, err := snapshot.Get(key, options)
	return b, err
}

func (ldbs *Store) set(key, val []byte) error {
	ldbs.writer.Lock()
	defer ldbs.writer.Unlock()
	return ldbs.setlocked(key, val)
}

func (ldbs *Store) setlocked(key, val []byte) error {
	options := defaultWriteOptions()
	err := ldbs.db.Put(key, val, options)
	return err
}

func (ldbs *Store) delete(key []byte) error {
	ldbs.writer.Lock()
	defer ldbs.writer.Unlock()
	return ldbs.deletelocked(key)
}

func (ldbs *Store) deletelocked(key []byte) error {
	options := defaultWriteOptions()
	err := ldbs.db.Delete(key, options)
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

func (ldbs *Store) newBatch() store.KVBatch {
	return newBatch(ldbs)
}

func StoreConstructor(config map[string]interface{}) (store.KVStore, error) {
	path, ok := config["path"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify path")
	}
	return Open(path, config)
}

func init() {
	registry.RegisterKVStore(Name, StoreConstructor)
}

func applyConfig(o *opt.Options, config map[string]interface{}) (
	*opt.Options, error) {

	cim, ok := config["create_if_missing"].(bool)
	if ok {
		o.ErrorIfMissing = !cim
	}

	eie, ok := config["error_if_exists"].(bool)
	if ok {
		o.ErrorIfExist = eie
	}

	wbs, ok := config["write_buffer_size"].(float64)
	if ok {
		o.WriteBuffer = int(wbs)
	}

	bs, ok := config["block_size"].(float64)
	if ok {
		o.BlockSize = int(bs)
	}

	bri, ok := config["block_restart_interval"].(float64)
	if ok {
		o.BlockRestartInterval = int(bri)
	}

	lcc, ok := config["lru_cache_capacity"].(float64)
	if ok {
		o.BlockCacheCapacity = int(lcc)
	}

	bfbpk, ok := config["bloom_filter_bits_per_key"].(float64)
	if ok {
		bf := filter.NewBloomFilter(int(bfbpk))
		o.Filter = bf
	}

	return o, nil
}
