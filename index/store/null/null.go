//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package null

import (
	"github.com/blevesearch/bleve/index/store"
)

type Store struct{}

func New() (*Store, error) {
	rv := Store{}
	return &rv, nil
}

func (i *Store) Open() error {
	return nil
}

func (i *Store) SetMergeOperator(mo store.MergeOperator) {

}

func (i *Store) Close() error {
	return nil
}

func (i *Store) iterator(key []byte) store.KVIterator {
	rv := newIterator(i)
	return rv
}

func (i *Store) Reader() (store.KVReader, error) {
	return newReader(i)
}

func (i *Store) Writer() (store.KVWriter, error) {
	return newWriter(i)
}

func (i *Store) newBatch() store.KVBatch {
	return newBatch(i)
}

type Reader struct {
	store *Store
}

func newReader(store *Store) (*Reader, error) {
	return &Reader{
		store: store,
	}, nil
}

func (r *Reader) BytesSafeAfterClose() bool {
	return true
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (r *Reader) Iterator(key []byte) store.KVIterator {
	return r.store.iterator(key)
}

func (r *Reader) Close() error {
	return nil
}

type Iterator struct{}

func newIterator(store *Store) *Iterator {
	return &Iterator{}
}

func (i *Iterator) SeekFirst() {}

func (i *Iterator) Seek(k []byte) {}

func (i *Iterator) Next() {}

func (i *Iterator) Current() ([]byte, []byte, bool) {
	return nil, nil, false
}

func (i *Iterator) Key() []byte {
	return nil
}

func (i *Iterator) Value() []byte {
	return nil
}

func (i *Iterator) Valid() bool {
	return false
}

func (i *Iterator) Close() error {
	return nil
}

type Batch struct{}

func newBatch(s *Store) *Batch {
	rv := Batch{}
	return &rv
}

func (i *Batch) Set(key, val []byte) {
}

func (i *Batch) Delete(key []byte) {
}

func (i *Batch) Merge(key, val []byte) {
}

func (i *Batch) Execute() error {
	return nil
}

func (i *Batch) Close() error {
	return nil
}

type Writer struct {
	store *Store
}

func newWriter(store *Store) (*Writer, error) {
	return &Writer{
		store: store,
	}, nil
}

func (w *Writer) BytesSafeAfterClose() bool {
	return true
}

func (w *Writer) Set(key, val []byte) error {
	return nil
}

func (w *Writer) Delete(key []byte) error {
	return nil
}

func (w *Writer) NewBatch() store.KVBatch {
	return newBatch(w.store)
}

func (w *Writer) Close() error {
	return nil
}

// these two methods can safely read using the regular
// methods without a read transaction, because we know
// that no one else is writing but us
func (w *Writer) Get(key []byte) ([]byte, error) {
	return nil, nil
}

func (w *Writer) Iterator(key []byte) store.KVIterator {
	return w.store.iterator(key)
}
