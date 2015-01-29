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
	"math/rand"
	"sync"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"

	"github.com/steveyen/gtreap"
)

const Name = "gtreap"

func init() {
	registry.RegisterKVStore(Name, StoreConstructor)
}

func StoreConstructor(config map[string]interface{}) (store.KVStore, error) {
	return &Store{t: gtreap.NewTreap(itemCompare)}, nil
}

type Item struct {
	k []byte
	v []byte
}

func itemCompare(a, b interface{}) int {
	return bytes.Compare(a.(*Item).k, b.(*Item).k)
}

type Store struct {
	m sync.Mutex
	t *gtreap.Treap
}

type Reader struct {
	t *gtreap.Treap
}

type Iterator struct {
	t *gtreap.Treap

	m        sync.Mutex
	cancelCh chan struct{}
	nextCh   chan *Item
	curr     *Item
	currOk   bool
}

func newIterator(t *gtreap.Treap) *Iterator {
	return &Iterator{t: t}
}

type Batch struct {
	s     *Store
	items []*Item
	ms    map[string]store.AssociativeMergeChain
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) Reader() (store.KVReader, error) {
	s.m.Lock()
	t := s.t
	s.m.Unlock()
	return &Reader{t: t}, nil
}

func (s *Store) Writer() (store.KVWriter, error) {
	return s, nil
}

func (w *Reader) Get(k []byte) (v []byte, err error) {
	itm := w.t.Get(&Item{k: k})
	if itm != nil {
		return itm.(*Item).v, nil
	}
	return nil, nil
}

func (w *Reader) Iterator(k []byte) store.KVIterator {
	return newIterator(w.t).restart(&Item{k: k})
}

func (w *Reader) Close() error {
	return nil
}

func (w *Store) Get(k []byte) (v []byte, err error) {
	w.m.Lock()
	t := w.t
	w.m.Unlock()

	itm := t.Get(&Item{k: k})
	if itm != nil {
		return itm.(*Item).v, nil
	}
	return nil, nil
}

func (w *Store) Iterator(k []byte) store.KVIterator {
	w.m.Lock()
	t := w.t
	w.m.Unlock()
	return newIterator(t).restart(&Item{k: k})
}

func (w *Store) Set(k, v []byte) (err error) {
	w.m.Lock()
	w.t = w.t.Upsert(&Item{k: k, v: v}, rand.Int())
	w.m.Unlock()
	return nil
}

func (w *Store) Delete(k []byte) (err error) {
	w.m.Lock()
	w.t = w.t.Delete(&Item{k: k})
	w.m.Unlock()
	return nil
}

func (w *Store) NewBatch() store.KVBatch {
	return &Batch{
		s:     w,
		items: make([]*Item, 0, 100),
		ms:    map[string]store.AssociativeMergeChain{},
	}
}

func (w *Iterator) SeekFirst() {
	min := w.t.Min()
	if min != nil {
		w.restart(min.(*Item))
	} else {
		w.restart(nil)
	}
}

func (w *Iterator) Seek(k []byte) {
	w.restart(&Item{k: k})
}

func (w *Iterator) restart(start *Item) *Iterator {
	cancelCh := make(chan struct{})
	nextCh := make(chan *Item, 1)

	w.m.Lock()
	if w.cancelCh != nil {
		close(w.cancelCh)
	}
	w.cancelCh = cancelCh
	w.nextCh = nextCh
	w.curr = nil
	w.currOk = false
	w.m.Unlock()

	go func() {
		if start != nil {
			w.t.VisitAscend(start, func(itm gtreap.Item) bool {
				select {
				case <-cancelCh:
					return false
				case nextCh <- itm.(*Item):
					return true
				}
			})
		}
		close(nextCh)
	}()

	w.Next()

	return w
}

func (w *Iterator) Next() {
	w.m.Lock()
	nextCh := w.nextCh
	w.m.Unlock()
	w.curr, w.currOk = <-nextCh
}

func (w *Iterator) Current() ([]byte, []byte, bool) {
	w.m.Lock()
	defer w.m.Unlock()
	if !w.currOk || w.curr == nil {
		return nil, nil, false
	}
	return w.curr.k, w.curr.v, w.currOk
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
	w.m.Lock()
	if w.cancelCh != nil {
		close(w.cancelCh)
	}
	w.cancelCh = nil
	w.nextCh = nil
	w.curr = nil
	w.currOk = false
	w.m.Unlock()

	return nil
}

func (w *Batch) Set(k, v []byte) {
	w.items = append(w.items, &Item{k, v})
}

func (w *Batch) Delete(k []byte) {
	w.items = append(w.items, &Item{k, nil})
}

func (w *Batch) Merge(k []byte, oper store.AssociativeMerge) {
	w.ms[string(k)] = append(w.ms[string(k)], oper)
}

func (w *Batch) Execute() (err error) {
	done := false
	for !done {
		w.s.m.Lock()
		torig := w.s.t
		w.s.m.Unlock()

		t := torig
		for key, mc := range w.ms {
			k := []byte(key)
			itm := t.Get(&Item{k: k})
			v := []byte(nil)
			if itm != nil {
				v = itm.(*Item).v
			}
			v, err := mc.Merge(k, v)
			if err != nil {
				return err
			}
			if v != nil {
				t = t.Upsert(&Item{k: k, v: v}, rand.Int())
			} else {
				t = t.Delete(&Item{k: k})
			}
		}

		for _, item := range w.items {
			v := item.v
			if v != nil {
				t = t.Upsert(item, rand.Int())
			} else {
				t = t.Delete(item)
			}
		}

		w.s.m.Lock()
		if w.s.t == torig {
			w.s.t = t
			done = true
		}
		w.s.m.Unlock()
	}

	return nil
}

func (w *Batch) Close() error {
	return nil
}
