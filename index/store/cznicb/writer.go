//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build go1.4

package cznicb

import (
	"github.com/blevesearch/bleve/index/store"
)

type Writer struct {
	s *Store
	r *Reader
}

func (w *Writer) BytesSafeAfterClose() bool {
	return false
}

func (w *Writer) Set(key, val []byte) error {
	return w.s.set(key, val)
}

func (w *Writer) Delete(key []byte) error {
	return w.s.delete(key)
}

func (w *Writer) NewBatch() store.KVBatch {
	return &Batch{
		s:      w.s,
		ops:    make([]op, 0, 1000),
		merges: make(map[string][][]byte),
	}
}

func (w *Writer) Close() error {
	w.s.availableWriters <- true
	w.s = nil
	return nil
}

func (w *Writer) Get(key []byte) ([]byte, error) {
	return w.r.s.get(key)
}

func (w *Writer) Iterator(key []byte) store.KVIterator {
	return w.r.s.iterator(key)
}
