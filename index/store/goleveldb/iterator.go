//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package goleveldb

import (
	"github.com/mschoch/goleveldb/leveldb"
	"github.com/mschoch/goleveldb/leveldb/iterator"
)

type Iterator struct {
	store    *Store
	iterator iterator.Iterator
	copyk    []byte
	copyv    []byte
}

func newIterator(store *Store) *Iterator {
	ropts := defaultReadOptions()
	iter := store.db.NewIterator(nil, ropts)
	rv := Iterator{
		store:    store,
		iterator: iter,
	}
	return &rv
}

func newIteratorWithSnapshot(store *Store, snapshot *leveldb.Snapshot) *Iterator {
	options := defaultReadOptions()
	iter := snapshot.NewIterator(nil, options)
	rv := Iterator{
		store:    store,
		iterator: iter,
	}
	return &rv
}

func (ldi *Iterator) SeekFirst() {
	ldi.copyk = nil
	ldi.copyv = nil
	ldi.iterator.First()
}

func (ldi *Iterator) Seek(key []byte) {
	ldi.copyk = nil
	ldi.copyv = nil
	ldi.iterator.Seek(key)
}

func (ldi *Iterator) Next() {
	ldi.copyk = nil
	ldi.copyv = nil
	ldi.iterator.Next()
}

func (ldi *Iterator) Current() ([]byte, []byte, bool) {
	if ldi.Valid() {
		return ldi.Key(), ldi.Value(), true
	}
	return nil, nil, false
}

func (ldi *Iterator) Key() []byte {
	k := ldi.iterator.Key()
	if ldi.copyk == nil {
		ldi.copyk = make([]byte, len(k))
		copy(ldi.copyk, k)
	}
	return ldi.copyk
}

func (ldi *Iterator) Value() []byte {
	v := ldi.iterator.Value()
	if ldi.copyv == nil {
		ldi.copyv = make([]byte, len(v))
		copy(ldi.copyv, v)
	}
	return ldi.copyv
}

func (ldi *Iterator) Valid() bool {
	return ldi.iterator.Valid()
}

func (ldi *Iterator) Close() error {
	ldi.copyk = nil
	ldi.copyv = nil
	return nil
}
