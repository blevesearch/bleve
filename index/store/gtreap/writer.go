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
	"math/rand"

	"github.com/blevesearch/bleve/index/store"
)

func (w *Writer) BytesSafeAfterClose() bool {
	return false
}

func (w *Writer) Get(k []byte) (v []byte, err error) {
	w.s.m.Lock()
	t := w.s.t
	w.s.m.Unlock()

	itm := t.Get(&Item{k: k})
	if itm != nil {
		return itm.(*Item).v, nil
	}
	return nil, nil
}

func (w *Writer) Iterator(k []byte) store.KVIterator {
	w.s.m.Lock()
	t := w.s.t
	w.s.m.Unlock()

	return newIterator(t).restart(&Item{k: k})
}

func (w *Writer) Close() error {
	w.s.availableWriters <- true
	w.s = nil

	return nil
}

func (w *Writer) Set(k, v []byte) (err error) {
	w.s.m.Lock()
	w.s.t = w.s.t.Upsert(&Item{k: k, v: v}, rand.Int())
	w.s.m.Unlock()

	return nil
}

func (w *Writer) Delete(k []byte) (err error) {
	w.s.m.Lock()
	w.s.t = w.s.t.Delete(&Item{k: k})
	w.s.m.Unlock()

	return nil
}

func (w *Writer) NewBatch() store.KVBatch {
	return store.NewEmulatedBatch(w, w.s.mo)
}
