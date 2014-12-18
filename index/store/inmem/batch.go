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
	indexStore "github.com/blevesearch/bleve/index/store"
)

type Batch struct {
	store         *Store
	keys          [][]byte
	vals          [][]byte
	alreadyLocked bool
	merges        map[string]indexStore.AssociativeMergeChain
}

func newBatch(store *Store) *Batch {
	rv := Batch{
		store:  store,
		keys:   make([][]byte, 0),
		vals:   make([][]byte, 0),
		merges: make(map[string]indexStore.AssociativeMergeChain),
	}
	return &rv
}

func newBatchAlreadyLocked(store *Store) *Batch {
	rv := Batch{
		store:         store,
		keys:          make([][]byte, 0),
		vals:          make([][]byte, 0),
		alreadyLocked: true,
		merges:        make(map[string]indexStore.AssociativeMergeChain),
	}
	return &rv
}

func (i *Batch) Set(key, val []byte) {
	i.keys = append(i.keys, key)
	i.vals = append(i.vals, val)
}

func (i *Batch) Delete(key []byte) {
	i.keys = append(i.keys, key)
	i.vals = append(i.vals, nil)
}

func (i *Batch) Merge(key []byte, oper indexStore.AssociativeMerge) {
	opers, ok := i.merges[string(key)]
	if !ok {
		opers = make(indexStore.AssociativeMergeChain, 0, 1)
	}
	opers = append(opers, oper)
	i.merges[string(key)] = opers
}

func (i *Batch) Execute() error {
	if !i.alreadyLocked {
		i.store.writer.Lock()
		defer i.store.writer.Unlock()
	}

	// first process the merges
	for k, mc := range i.merges {
		val, err := i.store.get([]byte(k))
		if err != nil {
			return err
		}
		val, err = mc.Merge([]byte(k), val)
		if err != nil {
			return err
		}
		if val == nil {
			err := i.store.deletelocked([]byte(k))
			if err != nil {
				return err
			}
		} else {
			err := i.store.setlocked([]byte(k), val)
			if err != nil {
				return err
			}
		}
	}

	for index, key := range i.keys {
		val := i.vals[index]
		if val == nil {
			err := i.store.deletelocked(key)
			if err != nil {
				return err
			}
		} else {
			err := i.store.setlocked(key, val)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (i *Batch) Close() error {
	return nil
}
