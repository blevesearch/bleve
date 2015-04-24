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

// Package gtreap provides an in-memory implementation of the
// KVStore interfaces using the gtreap balanced-binary treap,
// copy-on-write data structure.
package gtreap

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"

	"github.com/steveyen/gtreap"
)

const Name = "gtreap"

func init() {
	registry.RegisterKVStore(Name, StoreConstructor)
}

const MAX_CONCURRENT_WRITERS = 1

func StoreConstructor(config map[string]interface{}) (store.KVStore, error) {
	s := &Store{
		availableWriters: make(chan bool, MAX_CONCURRENT_WRITERS),
		t:                gtreap.NewTreap(itemCompare),
	}
	for i := 0; i < MAX_CONCURRENT_WRITERS; i++ {
		s.availableWriters <- true
	}
	return s, nil
}

type Item struct {
	k []byte
	v []byte
}

func itemCompare(a, b interface{}) int {
	return bytes.Compare(a.(*Item).k, b.(*Item).k)
}

type Store struct {
	availableWriters chan bool

	m sync.Mutex
	t *gtreap.Treap

	mo store.MergeOperator
}

type Writer struct {
	s *Store
}

func (s *Store) Open() error {
	return nil
}

func (s *Store) SetMergeOperator(mo store.MergeOperator) {
	s.mo = mo
}

func (s *Store) Close() error {
	close(s.availableWriters)
	return nil
}

func (s *Store) Reader() (store.KVReader, error) {
	s.m.Lock()
	t := s.t
	s.m.Unlock()
	return &Reader{t: t}, nil
}

func (s *Store) Writer() (store.KVWriter, error) {
	available, ok := <-s.availableWriters
	if !ok || !available {
		return nil, fmt.Errorf("no available writers")
	}

	return &Writer{s: s}, nil
}
