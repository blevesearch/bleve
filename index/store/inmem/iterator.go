//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package inmem

import (
	"github.com/ryszard/goskiplist/skiplist"
)

type Iterator struct {
	store    *Store
	iterator skiplist.Iterator
	valid    bool
}

func newIterator(store *Store) *Iterator {
	rv := Iterator{
		store:    store,
		iterator: store.list.Iterator(),
	}
	return &rv
}

func (i *Iterator) SeekFirst() {
	i.Seek([]byte{0})
}

func (i *Iterator) Seek(k []byte) {
	i.valid = i.iterator.Seek(string(k))
}

func (i *Iterator) Next() {
	i.valid = i.iterator.Next()
}

func (i *Iterator) Current() ([]byte, []byte, bool) {
	if i.valid {
		return []byte(i.Key()), []byte(i.Value()), true
	}
	return nil, nil, false
}

func (i *Iterator) Key() []byte {
	if i.valid {
		return []byte(i.iterator.Key().(string))
	}
	return nil
}

func (i *Iterator) Value() []byte {
	if i.valid {
		return []byte(i.iterator.Value().(string))
	}
	return nil
}

func (i *Iterator) Valid() bool {
	return i.valid
}

func (i *Iterator) Close() error {
	i.iterator.Close()
	return nil
}
