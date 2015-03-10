//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package upside_down

import (
	"bytes"
	"fmt"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type UpsideDownCouchFieldDict struct {
	indexReader *IndexReader
	iterator    store.KVIterator
	endKey      []byte
	field       uint16
}

func newUpsideDownCouchFieldDict(indexReader *IndexReader, field uint16, startTerm, endTerm []byte) (*UpsideDownCouchFieldDict, error) {

	startKey := NewDictionaryRow(startTerm, field, 0).Key()
	if endTerm == nil {
		endTerm = []byte{ByteSeparator}
	}
	endKey := NewDictionaryRow(endTerm, field, 0).Key()

	it := indexReader.kvreader.Iterator(startKey)

	return &UpsideDownCouchFieldDict{
		indexReader: indexReader,
		iterator:    it,
		field:       field,
		endKey:      endKey,
	}, nil

}

func (r *UpsideDownCouchFieldDict) Next() (*index.DictEntry, error) {
	key, val, valid := r.iterator.Current()
	if !valid {
		return nil, nil
	}

	// past end term
	if bytes.Compare(key, r.endKey) > 0 {
		return nil, nil
	}

	currRow, err := NewDictionaryRowKV(key, val)
	if err != nil {
		return nil, fmt.Errorf("unexpected error parsing dictionary row kv: %v", err)
	}
	rv := index.DictEntry{
		Term:  string(currRow.term),
		Count: currRow.count,
	}
	// advance the iterator to the next term
	r.iterator.Next()
	return &rv, nil

}

func (r *UpsideDownCouchFieldDict) Close() error {
	return r.iterator.Close()
}

func incrementBytes(in []byte) []byte {
	rv := make([]byte, len(in))
	copy(rv, in)
	for i := len(rv) - 1; i >= 0; i-- {
		rv[i] = rv[i] + 1
		if rv[i] != 0 {
			// didn't overflow, so stop
			break
		}
	}
	return rv
}
