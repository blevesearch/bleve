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

// This iterator uses values stored in a Readers storage to do isolated reads.
type ReaderIterator struct {
	store             *Store
	reader            *Reader
	iterator          skiplist.Iterator
	valid             bool
	fromReaderStorage bool
	currentKey        string
}

func newReaderIterator(store *Store, reader *Reader) *ReaderIterator {
	rv := ReaderIterator{
		store:    store,
		iterator: store.list.Iterator(),
		reader:   reader,
	}
	return &rv
}

func (i *ReaderIterator) SeekFirst() {
	i.Seek([]byte{0})
}

func (i *ReaderIterator) Seek(k []byte) {
	stringkey := string(k)
	if i.reader.readerData.valueMap[stringkey] != nil {
		if i.reader.readerData.valueMap[stringkey].newentry {
			i.valid = false
			i.fromReaderStorage = false
		} else if i.reader.readerData.valueMap[stringkey].firstValue {
			i.valid = true
			i.fromReaderStorage = true
			i.currentKey = stringkey
			i.SeekFirst()
		} else if i.reader.readerData.valueMap[stringkey].deleted {
			i.valid = true
			i.fromReaderStorage = true
			i.currentKey = stringkey
			i.iterator.Seek(i.reader.readerData.valueMap[stringkey].prevKey)
		} else {
			i.valid = true
			i.fromReaderStorage = true
			i.currentKey = stringkey
			i.iterator.Seek(stringkey)
		}
	} else {
		i.valid = i.iterator.Seek(stringkey)
		i.fromReaderStorage = false
	}
}

func (i *ReaderIterator) Next() {
	var key string
	if i.fromReaderStorage {
		key = i.currentKey
	} else {
		key = i.iterator.Key().(string)
	}

	nextKey, ok := i.reader.readerData.prevValuesOfDeletedKeys.Get(key)
	if ok {
		i.currentKey = nextKey.(string)
		i.valid = true
		i.fromReaderStorage = true
	} else {
		hasNextValue := i.iterator.Next()
		if hasNextValue {
			key = i.iterator.Key().(string)
			if i.reader.readerData.valueMap[key] != nil {
				if i.reader.readerData.valueMap[key].newentry || i.reader.readerData.valueMap[key].deleted {
					i.Next()
				} else {
					i.valid = true
					i.fromReaderStorage = true
					i.currentKey = key
				}
			} else {
				i.valid = true
				i.fromReaderStorage = false
			}
		} else {
			i.valid = false
		}
	}
}

func (i *ReaderIterator) Current() ([]byte, []byte, bool) {
	if i.valid && i.fromReaderStorage {
		return []byte(i.currentKey), []byte(i.reader.readerData.valueMap[i.currentKey].value), true
	} else if i.valid {
		return []byte(i.Key()), []byte(i.Value()), true
	}
	return nil, nil, false
}

func (i *ReaderIterator) Key() []byte {
	if i.valid && i.fromReaderStorage {
		return []byte(i.currentKey)
	} else if i.valid {
		return []byte(i.iterator.Key().(string))
	}
	return nil
}

func (i *ReaderIterator) Value() []byte {
	if i.valid && i.fromReaderStorage {
		return []byte(i.reader.readerData.valueMap[i.currentKey].value)
	} else if i.valid {
		return []byte(i.iterator.Value().(string))
	}
	return nil
}

func (i *ReaderIterator) Valid() bool {
	return i.valid
}

func (i *ReaderIterator) Close() error {
	i.iterator.Close()
	return nil
}
