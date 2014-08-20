//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package inmem

import (
	"github.com/couchbaselabs/bleve/index/store"
	"github.com/couchbaselabs/bleve/registry"
	"github.com/ryszard/goskiplist/skiplist"
)

const Name = "mem"

type InMemStore struct {
	list *skiplist.SkipList
}

func Open() (*InMemStore, error) {
	rv := InMemStore{
		list: skiplist.NewStringMap(),
	}

	return &rv, nil
}

func MustOpen() *InMemStore {
	rv := InMemStore{
		list: skiplist.NewStringMap(),
	}

	return &rv
}

func (i *InMemStore) Get(key []byte) ([]byte, error) {
	val, ok := i.list.Get(string(key))
	if ok {
		return []byte(val.(string)), nil
	}
	return nil, nil
}

func (i *InMemStore) Set(key, val []byte) error {
	i.list.Set(string(key), string(val))
	return nil
}

func (i *InMemStore) Delete(key []byte) error {
	i.list.Delete(string(key))
	return nil
}

func (i *InMemStore) Commit() error {
	return nil
}

func (i *InMemStore) Close() error {
	return nil
}

func (i *InMemStore) Iterator(key []byte) store.KVIterator {
	rv := newInMemIterator(i)
	rv.Seek(key)
	return rv
}

func (i *InMemStore) NewBatch() store.KVBatch {
	return newInMemBatch(i)
}

func StoreConstructor(config map[string]interface{}) (store.KVStore, error) {
	return Open()
}

func init() {
	registry.RegisterKVStore(Name, StoreConstructor)
}
