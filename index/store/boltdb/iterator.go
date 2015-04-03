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

type Iterator struct {
	store  *Store
	tx     *bolt.Tx
	cursor *bolt.Cursor
	valid  bool
	key    []byte
	val    []byte
}

func (i *Iterator) SeekFirst() {
	i.key, i.val = i.cursor.First()
	i.valid = (i.key != nil)
}

func (i *Iterator) Seek(k []byte) {
	i.key, i.val = i.cursor.Seek(k)
	i.valid = (i.key != nil)
}

func (i *Iterator) Next() {
	i.key, i.val = i.cursor.Next()
	i.valid = (i.key != nil)
}

func (i *Iterator) Current() ([]byte, []byte, bool) {
	return i.key, i.val, i.valid
}

func (i *Iterator) Key() []byte {
	return i.key
}

func (i *Iterator) Value() []byte {
	return i.val
}

func (i *Iterator) Valid() bool {
	return i.valid
}

func (i *Iterator) Close() error {
	return nil
}
