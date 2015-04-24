//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package store

type op struct {
	K []byte
	V []byte
}

type EmulatedBatch struct {
	w     KVWriter
	ops   []*op
	merge *EmulatedMerge
}

func NewEmulatedBatch(w KVWriter, mo MergeOperator) *EmulatedBatch {
	return &EmulatedBatch{
		w:     w,
		ops:   make([]*op, 0, 1000),
		merge: NewEmulatedMerge(mo),
	}
}

func (b *EmulatedBatch) Set(key, val []byte) {
	b.ops = append(b.ops, &op{key, val})
}

func (b *EmulatedBatch) Delete(key []byte) {
	b.ops = append(b.ops, &op{key, nil})
}

func (b *EmulatedBatch) Merge(key, val []byte) {
	b.merge.Merge(key, val)
}

func (b *EmulatedBatch) Execute() error {
	// first process merges
	err := b.merge.Execute(b.w)
	if err != nil {
		return err
	}

	// now apply all the ops
	for _, op := range b.ops {
		if op.V != nil {
			err := b.w.Set(op.K, op.V)
			if err != nil {
				return err
			}
		} else {
			err := b.w.Delete(op.K)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (b *EmulatedBatch) Close() error {
	return nil
}
