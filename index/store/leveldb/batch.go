//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build leveldb full

package leveldb

import (
	indexStore "github.com/blevesearch/bleve/index/store"
	"github.com/jmhodges/levigo"
)

type op struct {
	k []byte
	v []byte
}

type Batch struct {
	store         *Store
	ops           []op
	alreadyLocked bool
	merges        map[string]indexStore.AssociativeMergeChain
}

func newBatch(store *Store) *Batch {
	rv := Batch{
		store:  store,
		ops:    make([]op, 0),
		merges: make(map[string]indexStore.AssociativeMergeChain),
	}
	return &rv
}

func newBatchAlreadyLocked(store *Store) *Batch {
	rv := Batch{
		store:         store,
		ops:           make([]op, 0),
		alreadyLocked: true,
		merges:        make(map[string]indexStore.AssociativeMergeChain),
	}
	return &rv
}

func (ldb *Batch) Set(key, val []byte) {
	ldb.ops = append(ldb.ops, op{key, val})
}

func (ldb *Batch) Delete(key []byte) {
	ldb.ops = append(ldb.ops, op{key, nil})
}

func (ldb *Batch) Merge(key []byte, oper indexStore.AssociativeMerge) {
	opers, ok := ldb.merges[string(key)]
	if !ok {
		opers = make(indexStore.AssociativeMergeChain, 0, 1)
	}
	opers = append(opers, oper)
	ldb.merges[string(key)] = opers
}

func (ldb *Batch) Execute() error {
	if !ldb.alreadyLocked {
		ldb.store.writer.Lock()
		defer ldb.store.writer.Unlock()
	}

	batch := levigo.NewWriteBatch()
	defer batch.Close()

	// first process the merges
	for k, mc := range ldb.merges {
		val, err := ldb.store.get([]byte(k))
		if err != nil {
			return err
		}
		val, err = mc.Merge([]byte(k), val)
		if err != nil {
			return err
		}
		if val == nil {
			batch.Delete([]byte(k))
		} else {
			batch.Put([]byte(k), val)
		}
	}

	// now add all the other ops to the batch
	for _, op := range ldb.ops {
		if op.v == nil {
			batch.Delete(op.k)
		} else {
			batch.Put(op.k, op.v)
		}
	}

	return ldb.store.db.Write(defaultWriteOptions(), batch)
}

func (ldb *Batch) Close() error {
	return nil
}
