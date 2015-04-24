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
	"github.com/blevesearch/bleve/index/store"

	"github.com/steveyen/gtreap"
)

type Reader struct {
	t *gtreap.Treap
}

func (w *Reader) BytesSafeAfterClose() bool {
	return false
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
