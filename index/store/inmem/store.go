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
	"sync"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/registry"
	"github.com/ryszard/goskiplist/skiplist"
)

const Name = "mem"

// Data is tored for readers in case of changes
type Store struct {
	list        *skiplist.SkipList
	writer      sync.Mutex
	readersData map[*readerData]*readerData
}

func Open() (*Store, error) {
	rv := Store{
		list:        skiplist.NewStringMap(),
		readersData: make(map[*readerData]*readerData),
	}

	return &rv, nil
}

func MustOpen() *Store {
	rv := Store{
		list:        skiplist.NewStringMap(),
		readersData: make(map[*readerData]*readerData),
	}

	return &rv
}

func (i *Store) get(key []byte) ([]byte, error) {
	val, ok := i.list.Get(string(key))
	if ok {
		return []byte(val.(string)), nil
	}
	return nil, nil
}

func (i *Store) set(key, val []byte) error {
	i.writer.Lock()
	defer i.writer.Unlock()
	return i.setlocked(key, val)
}

// Updates storage of Readers created before the function is called with the values of keys at the time of their creation.
func (i *Store) updateReadersData(bytekey []byte, deleted bool) {
	var newentry bool
	var byteval []byte
	var prevKey string
	var nextKey string
	var firstValue bool
	key := string(bytekey)

	val, ok := i.list.Get(key)
	if ok {
		newentry = false
		byteval = []byte(val.(string))
	} else if deleted {
		// return if a deleted value is being deleted again
		return
	} else {
		// a new values is being inserted in the list
		newentry = true
		byteval = nil
	}

	if deleted {
		// locate keys located before and after the deleted key
		iterator := i.list.Seek(key)
		ok = iterator.Previous()
		if ok {
			prevKey = iterator.Key().(string)
		} else {
			firstValue = true
		}
		iterator.Next()
		ok = iterator.Next()
		if ok {
			nextKey = iterator.Key().(string)
		}
	}

	for _, v := range i.readersData {
		if deleted {
			// locate keys located before and after the deleted key if they were deleted
			tempPrevKey, ok := v.nextKeysOfDeletedKeys[key]
			if ok {
				prevKey = tempPrevKey
			}
			tempNextKey, ok := v.prevKeysOfDeletedKeys[key]
			if ok {
				nextKey = tempNextKey
			}
		}

		if v.valueMap[key] == nil {
			// do this if this is the first time this key is being modified after creation of the reader
			v.valueMap[key] = &readerValue{
				value:      byteval,
				newentry:   newentry,
				deleted:    deleted,
				firstValue: firstValue,
				prevKey:    prevKey,
			}
			if deleted {
				if !firstValue {
					v.prevKeysOfDeletedKeys[prevKey] = key
					v.nextKeysOfDeletedKeys[nextKey] = key
				}
			}
		} else if deleted && !v.valueMap[key].deleted {
			// do this if this key is being deleted after it was previously modified
			v.valueMap[key].deleted = deleted
			v.valueMap[key].firstValue = firstValue
			v.valueMap[key].prevKey = prevKey
			if !firstValue {
				v.prevKeysOfDeletedKeys[prevKey] = key
				v.nextKeysOfDeletedKeys[nextKey] = key
			}
		}
	}
}

func (i *Store) setlocked(key, val []byte) error {
	i.updateReadersData(key, false)
	i.list.Set(string(key), string(val))
	return nil
}

func (i *Store) delete(key []byte) error {
	i.writer.Lock()
	defer i.writer.Unlock()
	return i.deletelocked(key)
}

func (i *Store) deletelocked(key []byte) error {
	i.updateReadersData(key, true)
	i.list.Delete(string(key))
	return nil
}

func (i *Store) Close() error {
	return nil
}

func (i *Store) iterator(key []byte) store.KVIterator {
	rv := newIterator(i)
	rv.Seek(key)
	return rv
}

func (i *Store) readerIterator(key []byte, reader *Reader) store.KVIterator {
	rv := newReaderIterator(i, reader)
	rv.Seek(key)
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

func StoreConstructor(config map[string]interface{}) (store.KVStore, error) {
	return Open()
}

func init() {
	registry.RegisterKVStore(Name, StoreConstructor)
}
