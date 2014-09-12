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

type UpsideDownCouchFieldReader struct {
	indexReader *IndexReader
	iterator    store.KVIterator
	endKey      []byte
	field       uint16
}

func newUpsideDownCouchFieldReader(indexReader *IndexReader, field uint16, startTerm, endTerm []byte) (*UpsideDownCouchFieldReader, error) {

	startRow := NewTermFrequencyRow(startTerm, field, "", 0, 0)
	startKey := startRow.ScanPrefixForFieldTermPrefix()

	endKey := NewTermFrequencyRow(endTerm, field, "", 0, 0).Key()

	it := indexReader.kvreader.Iterator(startKey)

	return &UpsideDownCouchFieldReader{
		indexReader: indexReader,
		iterator:    it,
		field:       field,
		endKey:      endKey,
	}, nil

}

func (r *UpsideDownCouchFieldReader) Next() (*index.TermFieldDoc, error) {
	key, val, valid := r.iterator.Current()
	if !valid {
		return nil, nil
	}

	// past end term
	if bytes.Compare(key, r.endKey) > 0 {
		return nil, nil
	}

	currRow, err := NewTermFrequencyRowKV(key, val)
	if err != nil {
		return nil, fmt.Errorf("unexpected error parsing term freq row kv: %v", err)
	}
	rv := index.TermFieldDoc{
		Term: string(currRow.term),
		Freq: currRow.freq,
	}
	// advance the iterator to the next term
	// by using invalid doc id (higher sorting)
	nextTerm := incrementBytes(currRow.term)
	nextRow := NewTermFrequencyRow(nextTerm, r.field, "", 0, 0)
	r.iterator.Seek(nextRow.ScanPrefixForFieldTermPrefix())
	return &rv, nil

}

func (r *UpsideDownCouchFieldReader) Close() {
	r.iterator.Close()
}

func incrementBytes(in []byte) []byte {
	rv := make([]byte, len(in))
	copy(rv, in)
	for i := len(rv) - 1; i >= 0; i-- {
		rv[i] = rv[i] + 1
		if rv[i] != 0 {
			// didnt' overflow, so stop
			break
		}
	}
	return rv
}
