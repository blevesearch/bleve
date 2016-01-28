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
	"bytes"
	"fmt"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/store"
)

// the functions in this file are only intended to be used by
// the bleve_dump utility and the debug http handlers
// if your application relies on them, you're doing something wrong
// they may change or be removed at any time

func (f *Firestorm) dumpPrefix(kvreader store.KVReader, rv chan interface{}, prefix []byte) error {
	return visitPrefix(kvreader, prefix, func(key, val []byte) (bool, error) {
		row, err := parseFromKeyValue(key, val)
		if err != nil {
			rv <- err
			return false, err
		}
		rv <- row
		return true, nil
	})
}

func (f *Firestorm) dumpDoc(kvreader store.KVReader, rv chan interface{}, docID []byte) error {
	// without a back index we have no choice but to walk the term freq and stored rows

	// walk the term freqs
	err := visitPrefix(kvreader, TermFreqKeyPrefix, func(key, val []byte) (bool, error) {
		tfr, err := NewTermFreqRowKV(key, val)
		if err != nil {
			rv <- err
			return false, err
		}
		if bytes.Compare(tfr.DocID(), docID) == 0 {
			rv <- tfr
		}
		return true, nil
	})

	if err != nil {
		return err
	}

	// now walk the stored
	err = visitPrefix(kvreader, StoredKeyPrefix, func(key, val []byte) (bool, error) {
		sr, err := NewStoredRowKV(key, val)
		if err != nil {
			rv <- err
			return false, err
		}
		if bytes.Compare(sr.DocID(), docID) == 0 {
			rv <- sr
		}
		return true, nil
	})

	return err
}

func parseFromKeyValue(key, value []byte) (index.IndexRow, error) {
	if len(key) > 0 {
		switch key[0] {
		case VersionKey[0]:
			return NewVersionRowV(value)
		case FieldKeyPrefix[0]:
			return NewFieldRowKV(key, value)
		case DictionaryKeyPrefix[0]:
			return NewDictionaryRowKV(key, value)
		case TermFreqKeyPrefix[0]:
			return NewTermFreqRowKV(key, value)
		case StoredKeyPrefix[0]:
			return NewStoredRowKV(key, value)
		case InternalKeyPrefix[0]:
			return NewInternalRowKV(key, value)
		}
		return nil, fmt.Errorf("Unknown field type '%s'", string(key[0]))
	}
	return nil, fmt.Errorf("Invalid empty key")
}
