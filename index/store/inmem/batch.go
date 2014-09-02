//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package inmem

type InMemBatch struct {
	store *InMemStore
	keys  [][]byte
	vals  [][]byte
}

func newInMemBatch(store *InMemStore) *InMemBatch {
	rv := InMemBatch{
		store: store,
		keys:  make([][]byte, 0),
		vals:  make([][]byte, 0),
	}
	return &rv
}

func (i *InMemBatch) Set(key, val []byte) {
	i.keys = append(i.keys, key)
	i.vals = append(i.vals, val)
}

func (i *InMemBatch) Delete(key []byte) {
	i.keys = append(i.keys, key)
	i.vals = append(i.vals, nil)
}

func (i *InMemBatch) Execute() error {
	for index, key := range i.keys {
		val := i.vals[index]
		if val == nil {
			i.store.list.Delete(string(key))
		} else {
			i.store.Set(key, val)
		}
	}
	return nil
}

func (i *InMemBatch) Close() error {
	return nil
}
