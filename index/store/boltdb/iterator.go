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
	"github.com/boltdb/bolt"
)

type BoltDBIterator struct {
	store  *BoltDBStore
	tx     *bolt.Tx
	cursor *bolt.Cursor
	valid  bool
	key    []byte
	val    []byte
}

func newBoltDBIterator(store *BoltDBStore) *BoltDBIterator {
	tx, _ := store.db.Begin(false)
	b := tx.Bucket([]byte(store.bucket))
	cursor := b.Cursor()

	return &BoltDBIterator{
		store:  store,
		tx:     tx,
		cursor: cursor,
	}
}

func (i *BoltDBIterator) SeekFirst() {
	i.key, i.val = i.cursor.First()

	i.valid = (i.key != nil)
}

func (i *BoltDBIterator) Seek(k []byte) {
	i.key, i.val = i.cursor.Seek(k)

	i.valid = (i.key != nil)
}

func (i *BoltDBIterator) Next() {
	i.key, i.val = i.cursor.Next()

	i.valid = (i.key != nil)
}

func (i *BoltDBIterator) Current() ([]byte, []byte, bool) {
	return i.key, i.val, i.valid
}

func (i *BoltDBIterator) Key() []byte {
	return i.key
}

func (i *BoltDBIterator) Value() []byte {
	return i.val
}

func (i *BoltDBIterator) Valid() bool {
	return i.valid
}

func (i *BoltDBIterator) Close() {
	i.tx.Commit()
}
