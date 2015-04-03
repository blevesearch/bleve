//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package boltdb

import (
	"github.com/blevesearch/bleve/index/store"
	"github.com/boltdb/bolt"
)

type Writer struct {
	store  *Store
	tx     *bolt.Tx
	reader *Reader
}

func (w *Writer) Set(key, val []byte) error {
	return w.tx.Bucket([]byte(w.store.bucket)).Put(key, val)
}

func (w *Writer) Delete(key []byte) error {
	return w.tx.Bucket([]byte(w.store.bucket)).Delete(key)
}

func (w *Writer) NewBatch() store.KVBatch {
	rv := Batch{
		writer: w,
		ops:    make([]op, 0),
		merges: make(map[string]store.AssociativeMergeChain),
	}
	return &rv
}

func (w *Writer) Close() error {
	w.store.writer.Unlock()
	return w.tx.Commit()
}

func (w *Writer) BytesSafeAfterClose() bool {
	return w.reader.BytesSafeAfterClose()
}

func (w *Writer) Get(key []byte) ([]byte, error) {
	return w.reader.Get(key)
}

func (w *Writer) Iterator(key []byte) store.KVIterator {
	return w.reader.Iterator(key)
}
