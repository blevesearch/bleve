//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// +build go1.4

// Package cznicb provides an in-memory implementation of the KVStore
// interfaces using the cznic/b in-memory btree.  Of note: this
// implementation does not have reader isolation.
package cznicb

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"

	"github.com/cznic/b"
)

const Name = "cznicb"

const MAX_CONCURRENT_WRITERS = 1

func init() {
	registry.RegisterKVStore(Name, StoreConstructor)
}

func StoreConstructor(config map[string]interface{}) (store.KVStore, error) {
	s := &Store{
		t:                b.TreeNew(itemCompare),
		availableWriters: make(chan bool, MAX_CONCURRENT_WRITERS),
	}
	for i := 0; i < MAX_CONCURRENT_WRITERS; i++ {
		s.availableWriters <- true
	}
	return s, nil
}

func itemCompare(a, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

type Store struct {
	availableWriters chan bool
	m                sync.RWMutex
	t                *b.Tree
	mo               store.MergeOperator
}

func (s *Store) Open() error {
	return nil
}

func (s *Store) SetMergeOperator(mo store.MergeOperator) {
	s.mo = mo
}

func (s *Store) Reader() (store.KVReader, error) {
	return &Reader{s: s}, nil
}

func (s *Store) Writer() (store.KVWriter, error) {
	available, ok := <-s.availableWriters
	if !ok || !available {
		return nil, fmt.Errorf("no available writers")
	}
	return &Writer{s: s, r: &Reader{s: s}}, nil
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) get(k []byte) ([]byte, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	v, ok := s.t.Get(k)
	if !ok || v == nil {
		return nil, nil
	}
	return v.([]byte), nil
}

func (s *Store) iterator(k []byte) store.KVIterator {
	iter := &Iterator{s: s}
	iter.Seek(k)
	return iter
}

func (s *Store) set(k, v []byte) (err error) {
	s.m.Lock()
	defer s.m.Unlock()
	s.t.Set(k, v)
	return nil
}

func (s *Store) delete(k []byte) (err error) {
	s.m.Lock()
	defer s.m.Unlock()
	s.t.Delete(k)
	return nil
}
