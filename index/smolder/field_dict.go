//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package smolder

import (
	"fmt"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

type SmolderingCouchFieldDict struct {
	indexReader *IndexReader
	iterator    store.KVIterator
	field       uint16
}

func newSmolderingCouchFieldDict(indexReader *IndexReader, field uint16, startTerm, endTerm []byte) (*SmolderingCouchFieldDict, error) {

	startKey := NewDictionaryRow(startTerm, field, 0).Key()
	if endTerm == nil {
		endTerm = []byte{ByteSeparator}
	} else {
		endTerm = incrementBytes(endTerm)
	}
	endKey := NewDictionaryRow(endTerm, field, 0).Key()

	it := indexReader.kvreader.RangeIterator(startKey, endKey)

	return &SmolderingCouchFieldDict{
		indexReader: indexReader,
		iterator:    it,
		field:       field,
	}, nil

}

func (r *SmolderingCouchFieldDict) Next() (*index.DictEntry, error) {
	key, val, valid := r.iterator.Current()
	if !valid {
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

func (r *SmolderingCouchFieldDict) Close() error {
	return r.iterator.Close()
}
