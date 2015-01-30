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
	"errors"
	"sync"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"

	"github.com/cznic/b"
)

const Name = "cznicb"

var iteratorDoneErr = errors.New("iteratorDoneErr") // A sentinel value.

func init() {
	registry.RegisterKVStore(Name, StoreConstructor)
}

func StoreConstructor(config map[string]interface{}) (
	store.KVStore, error) {
	return &Store{t: b.TreeNew(itemCompare)}, nil
}

func itemCompare(a, b interface{}) int {
	return bytes.Compare(a.([]byte), b.([]byte))
}

type Store struct {
	m sync.Mutex
	t *b.Tree
}

type Iterator struct { // Assuming that iterators are used single-threaded.
	s *Store
	e *b.Enumerator

	currK   interface{}
	currV   interface{}
	currErr error
}

type op struct {
	k []byte
	v []byte
}

type Batch struct {
	s   *Store
	ops []op
	ms  map[string]store.AssociativeMergeChain
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) Reader() (store.KVReader, error) {
	return s, nil
}

func (s *Store) Writer() (store.KVWriter, error) {
	return s, nil
}

func (s *Store) Get(k []byte) ([]byte, error) {
	s.m.Lock()
	v, ok := s.t.Get(k)
	s.m.Unlock()
	if !ok || v == nil {
		return nil, nil
	}
	return v.([]byte), nil
}

func (s *Store) Iterator(k []byte) store.KVIterator {
	iter := &Iterator{s: s}
	iter.Seek(k)
	return iter
}

func (s *Store) Set(k, v []byte) (err error) {
	s.m.Lock()
	s.t.Set(k, v)
	s.m.Unlock()
	return nil
}

func (s *Store) Delete(k []byte) (err error) {
	s.m.Lock()
	s.t.Delete(k)
	s.m.Unlock()
	return nil
}

func (s *Store) NewBatch() store.KVBatch {
	return &Batch{
		s:   s,
		ops: make([]op, 0, 1000),
		ms:  map[string]store.AssociativeMergeChain{},
	}
}

func (w *Iterator) SeekFirst() {
	w.currK = nil
	w.currV = nil
	w.currErr = nil

	var err error
	w.s.m.Lock()
	w.e, err = w.s.t.SeekFirst()
	w.s.m.Unlock()
	if err != nil {
		w.currK = nil
		w.currV = nil
		w.currErr = iteratorDoneErr
	}

	w.Next()
}

func (w *Iterator) Seek(k []byte) {
	w.currK = nil
	w.currV = nil
	w.currErr = nil

	w.s.m.Lock()
	w.e, _ = w.s.t.Seek(k)
	w.s.m.Unlock()

	w.Next()
}

func (w *Iterator) Next() {
	if w.currErr != nil {
		w.currK = nil
		w.currV = nil
		w.currErr = iteratorDoneErr
		return
	}

	w.s.m.Lock()
	w.currK, w.currV, w.currErr = w.e.Next()
	w.s.m.Unlock()
}

func (w *Iterator) Current() ([]byte, []byte, bool) {
	if w.currErr == iteratorDoneErr ||
		w.currK == nil ||
		w.currV == nil {
		return nil, nil, false
	}

	return w.currK.([]byte), w.currV.([]byte), true
}

func (w *Iterator) Key() []byte {
	k, _, ok := w.Current()
	if !ok {
		return nil
	}
	return k
}

func (w *Iterator) Value() []byte {
	_, v, ok := w.Current()
	if !ok {
		return nil
	}
	return v
}

func (w *Iterator) Valid() bool {
	_, _, ok := w.Current()
	return ok
}

func (w *Iterator) Close() error {
	if w.e != nil {
		w.e.Close()
	}
	w.e = nil
	return nil
}

func (w *Batch) Set(k, v []byte) {
	w.ops = append(w.ops, op{k, v})
}

func (w *Batch) Delete(k []byte) {
	w.ops = append(w.ops, op{k, nil})
}

func (w *Batch) Merge(k []byte, oper store.AssociativeMerge) {
	w.ms[string(k)] = append(w.ms[string(k)], oper)
}

func (w *Batch) Execute() (err error) {
	w.s.m.Lock()
	defer w.s.m.Unlock()

	t := w.s.t
	for key, mc := range w.ms {
		k := []byte(key)
		t.Put(k, func(oldV interface{}, exists bool) (newV interface{}, write bool) {
			b := []byte(nil)
			if exists && oldV != nil {
				b = oldV.([]byte)
			}
			b, err := mc.Merge(k, b)
			if err != nil {
				return nil, false
			}
			return b, b != nil
		})
	}

	for _, op := range w.ops {
		if op.v != nil {
			t.Set(op.k, op.v)
		} else {
			t.Delete(op.k)
		}
	}

	return nil
}

func (w *Batch) Close() error {
	return nil
}
