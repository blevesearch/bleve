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
	"github.com/blevesearch/bleve/index/store"
)

// "newentry" marks a key which was previously empty, but was populated after the creation of the Reader.
// If the value stored at the key has changed, the value at the time of creation of the Reader is stored in value.
type readerValue struct {
	value      []byte
	newentry   bool   // was there a new entry on same key in the actual list
	deleted    bool   // was the original key deleted from the actual list
	firstValue bool   // was this the first value in the list
	prevKey    string // the key located before the key of this value in the actual list
}

// This is the readers copy of the data which has changed after the Reader has been created.
type readerData struct {
	valueMap              map[string]*readerValue
	prevKeysOfDeletedKeys map[string]string // contains keys located before the key deleted in the actual list mapped to the keys deleted
	nextKeysOfDeletedKeys map[string]string // contains keys located sfter the key deleted in the actual list mapped to the keys deleted
}

type Reader struct {
	store      *Store
	readerData *readerData
}

func newReader(store *Store) (*Reader, error) {
	readerData := readerData{
		valueMap:              make(map[string]*readerValue),
		prevKeysOfDeletedKeys: make(map[string]string),
		nextKeysOfDeletedKeys: make(map[string]string),
	}
	store.readersData[&readerData] = &readerData

	return &Reader{
		store:      store,
		readerData: &readerData,
	}, nil
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	stringkey := string(key)
	if r.readerData.valueMap[stringkey] != nil {
		if r.readerData.valueMap[stringkey].newentry {
			return nil, nil
		} else {
			return r.readerData.valueMap[stringkey].value, nil
		}
	} else {
		return r.store.get(key)
	}
}

func (r *Reader) Iterator(key []byte) store.KVIterator {
	return r.store.readerIterator(key, r)
}

func (r *Reader) Close() error {
	delete(r.store.readersData, r.readerData)
	return nil
}
