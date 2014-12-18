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
	indexStore "github.com/blevesearch/bleve/index/store"
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

func (b *Batch) Set(key, val []byte) {
	b.ops = append(b.ops, op{key, val})
}

func (b *Batch) Delete(key []byte) {
	b.ops = append(b.ops, op{key, nil})
}

func (b *Batch) Merge(key []byte, oper indexStore.AssociativeMerge) {
	opers, ok := b.merges[string(key)]
	if !ok {
		opers = make(indexStore.AssociativeMergeChain, 0, 1)
	}
	opers = append(opers, oper)
	b.merges[string(key)] = opers
}

func (b *Batch) Execute() error {
	if !b.alreadyLocked {
		b.store.writer.Lock()
		defer b.store.writer.Unlock()
	}

	// first process the merges
	for k, mc := range b.merges {
		val, err := b.store.get([]byte(k))
		if err != nil {
			return err
		}
		val, err = mc.Merge([]byte(k), val)
		if err != nil {
			return err
		}
		if val == nil {
			b.store.deletelocked([]byte(k))
		} else {
			b.store.setlocked([]byte(k), val)
		}
	}

	// now add all the other ops to the batch
	for _, op := range b.ops {
		if op.v == nil {
			b.store.deletelocked(op.k)
		} else {
			b.store.setlocked(op.k, op.v)
		}
	}

	return b.store.commit()
}

func (b *Batch) Close() error {
	return nil
}
