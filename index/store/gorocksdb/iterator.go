//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build rocksdb

package rocksdb

import (
	"github.com/tecbot/gorocksdb"
)

type Iterator struct {
	store    *Store
	iterator *gorocksdb.Iterator
}

func newIterator(store *Store) *Iterator {
	ropts := defaultReadOptions()
	rv := Iterator{
		store:    store,
		iterator: store.db.NewIterator(ropts),
	}
	return &rv
}

func newIteratorWithSnapshot(store *Store, snapshot *gorocksdb.Snapshot) *Iterator {
	options := defaultReadOptions()
	options.SetSnapshot(snapshot)
	rv := Iterator{
		store:    store,
		iterator: store.db.NewIterator(options),
	}
	return &rv
}

func (ldi *Iterator) SeekFirst() {
	ldi.iterator.SeekToFirst()
}

func (ldi *Iterator) Seek(key []byte) {
	ldi.iterator.Seek(key)
}

func (ldi *Iterator) Next() {
	ldi.iterator.Next()
}

func (ldi *Iterator) Current() ([]byte, []byte, bool) {
	if ldi.Valid() {
		return ldi.Key(), ldi.Value(), true
	}
	return nil, nil, false
}

func (ldi *Iterator) Key() []byte {
	return ldi.iterator.Key().Data()
}

func (ldi *Iterator) Value() []byte {
	return ldi.iterator.Value().Data()
}

func (ldi *Iterator) Valid() bool {
	return ldi.iterator.Valid()
}

func (ldi *Iterator) Close() error {
	ldi.iterator.Close()
	return nil
}
