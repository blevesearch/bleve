//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build forestdb

package forestdb

import (
	"fmt"

	"github.com/blevesearch/bleve/index/store"
	"github.com/couchbaselabs/goforestdb"
)

type Reader struct {
	store    *Store
	snapshot *forestdb.KVStore
}

func (r *Reader) BytesSafeAfterClose() bool {
	return true
}

func newReader(store *Store) (*Reader, error) {
	snapshot, err := store.newSnapshot()
	if err != nil {
		return nil, fmt.Errorf("error opening snapshot: %v", err)
	}
	return &Reader{
		store:    store,
		snapshot: snapshot,
	}, nil
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	res, err := r.snapshot.GetKV(key)
	if err != nil && err != forestdb.RESULT_KEY_NOT_FOUND {
		return nil, err
	}
	return res, nil
}

func (r *Reader) Iterator(key []byte) store.KVIterator {
	rv := newIteratorWithSnapshot(r.store, r.snapshot)
	rv.Seek(key)
	return rv
}

func (r *Reader) Close() error {
	return r.snapshot.Close()
}
