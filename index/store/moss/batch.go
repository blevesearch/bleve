//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

package moss

import (
	"github.com/couchbase/moss"

	"github.com/blevesearch/bleve/index/store"
)

type Batch struct {
	store   *Store
	merge   *store.EmulatedMerge
	batch   moss.Batch
	alloced bool
}

func (b *Batch) Set(key, val []byte) {
	if b.alloced {
		b.batch.AllocSet(key, val)
	} else {
		b.batch.Set(key, val)
	}
}

func (b *Batch) Delete(key []byte) {
	if b.alloced {
		b.batch.AllocDel(key)
	} else {
		b.batch.Del(key)
	}
}

func (b *Batch) Merge(key, val []byte) {
	b.merge.Merge(key, val)
}

func (b *Batch) Reset() {
	b.Close()

	batch, err := b.store.ms.NewBatch(0, 0)
	if err == nil {
		b.batch = batch
		b.merge = store.NewEmulatedMerge(b.store.mo)
	}
}

func (b *Batch) Close() error {
	b.merge = nil
	b.batch.Close()
	b.batch = nil
	return nil
}
