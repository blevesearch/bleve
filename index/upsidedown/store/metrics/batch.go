//  Copyright (c) 2015 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package metrics

import store "github.com/blevesearch/upsidedown_store_api"

type Batch struct {
	s *Store
	o store.KVBatch
}

func (b *Batch) Set(key, val []byte) {
	b.o.Set(key, val)
}

func (b *Batch) Delete(key []byte) {
	b.o.Delete(key)
}

func (b *Batch) Merge(key, val []byte) {
	b.s.timerBatchMerge.Time(func() {
		b.o.Merge(key, val)
	})
}

func (b *Batch) Reset() {
	b.o.Reset()
}

func (b *Batch) Close() error {
	err := b.o.Close()
	b.o = nil
	return err
}
