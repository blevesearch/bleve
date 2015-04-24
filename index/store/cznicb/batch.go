//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// +build go1.4

package cznicb

import ()

type op struct {
	k []byte
	v []byte
}

type Batch struct {
	s      *Store
	ops    []op
	merges map[string][][]byte
}

func (b *Batch) Set(k, v []byte) {
	b.ops = append(b.ops, op{k, v})
}

func (b *Batch) Delete(k []byte) {
	b.ops = append(b.ops, op{k, nil})
}

func (b *Batch) Merge(key, val []byte) {
	ops, ok := b.merges[string(key)]
	if ok && len(ops) > 0 {
		last := ops[len(ops)-1]
		mergedVal, partialMergeOk := b.s.mo.PartialMerge(key, last, val)
		if partialMergeOk {
			// replace last entry with the result of the merge
			ops[len(ops)-1] = mergedVal
		} else {
			// could not partial merge, append this to the end
			ops = append(ops, val)
		}
	} else {
		ops = [][]byte{val}
	}
	b.merges[string(key)] = ops
}

func (b *Batch) Execute() (err error) {
	b.s.m.Lock()
	defer b.s.m.Unlock()

	t := b.s.t
	for key, mergeOps := range b.merges {
		k := []byte(key)
		t.Put(k, func(oldV interface{}, exists bool) (newV interface{}, write bool) {
			ob := []byte(nil)
			if exists && oldV != nil {
				ob = oldV.([]byte)
			}
			mergedVal, fullMergeOk := b.s.mo.FullMerge(k, ob, mergeOps)
			if !fullMergeOk {
				return nil, false
			}
			return mergedVal, true
		})
	}

	for _, op := range b.ops {
		if op.v != nil {
			t.Set(op.k, op.v)
		} else {
			t.Delete(op.k)
		}
	}

	return nil
}

func (b *Batch) Close() error {
	return nil
}
