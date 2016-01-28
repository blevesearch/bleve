//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package firestorm

import (
	"fmt"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type firestormDictionaryReader struct {
	r     *firestormReader
	field uint16
	start []byte
	i     store.KVIterator
}

func newFirestormDictionaryReader(r *firestormReader, field uint16, start, end []byte) (*firestormDictionaryReader, error) {
	startKey := DictionaryRowKey(field, start)
	logger.Printf("start key '%s' - % x", startKey, startKey)
	if end == nil {
		end = []byte{ByteSeparator}
	}
	endKey := DictionaryRowKey(field, end)
	logger.Printf("end key '%s' - % x", endKey, endKey)
	i := r.r.RangeIterator(startKey, endKey)
	rv := firestormDictionaryReader{
		r:     r,
		field: field,
		start: startKey,
		i:     i,
	}
	return &rv, nil
}

func (r *firestormDictionaryReader) Next() (*index.DictEntry, error) {
	key, val, valid := r.i.Current()
	if !valid {
		return nil, nil
	}

	logger.Printf("see key '%s' - % x", key, key)

	currRow, err := NewDictionaryRowKV(key, val)
	if err != nil {
		return nil, fmt.Errorf("unexpected error parsing dictionary row kv: %v", err)
	}
	rv := index.DictEntry{
		Term:  string(currRow.term),
		Count: currRow.Count(),
	}
	// advance the iterator to the next term
	r.i.Next()
	return &rv, nil
}

func (r *firestormDictionaryReader) Close() error {
	if r.i != nil {
		return r.i.Close()
	}
	return nil
}
