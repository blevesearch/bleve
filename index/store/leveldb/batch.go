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
	"github.com/blevesearch/bleve/index/store"
	"github.com/jmhodges/levigo"
)

type Batch struct {
	w     *Writer
	merge *store.EmulatedMerge
	batch *levigo.WriteBatch
}

func (b *Batch) Set(key, val []byte) {
	b.batch.Put(key, val)
}

func (b *Batch) Delete(key []byte) {
	b.batch.Delete(key)
}

func (b *Batch) Merge(key, val []byte) {
	b.merge.Merge(key, val)
}

func (b *Batch) Execute() error {
	// first process merges
	ops, err := b.merge.ExecuteDeferred(b.w)
	if err != nil {
		return err
	}
	for _, op := range ops {
		b.batch.Put(op.K, op.V)
	}

	wopts := defaultWriteOptions()
	defer wopts.Close()
	err = b.w.store.db.Write(wopts, b.batch)
	return err
}

func (b *Batch) Close() error {
	return nil
}
