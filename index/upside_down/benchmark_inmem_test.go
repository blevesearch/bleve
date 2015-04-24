//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package upside_down

import (
	"testing"

	"github.com/blevesearch/bleve/index/store"
	"github.com/blevesearch/bleve/index/store/inmem"
)

func CreateInMem() (store.KVStore, error) {
	return inmem.New()
}

func DestroyInMem() error {
	return nil
}

func BenchmarkInMemIndexing1Workers(b *testing.B) {
	CommonBenchmarkIndex(b, CreateInMem, DestroyInMem, 1)
}

func BenchmarkInMemIndexing2Workers(b *testing.B) {
	CommonBenchmarkIndex(b, CreateInMem, DestroyInMem, 2)
}

func BenchmarkInMemIndexing4Workers(b *testing.B) {
	CommonBenchmarkIndex(b, CreateInMem, DestroyInMem, 4)
}

// batches

func BenchmarkInMemIndexing1Workers10Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, CreateInMem, DestroyInMem, 1, 10)
}

func BenchmarkInMemIndexing2Workers10Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, CreateInMem, DestroyInMem, 2, 10)
}

func BenchmarkInMemIndexing4Workers10Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, CreateInMem, DestroyInMem, 4, 10)
}

func BenchmarkInMemIndexing1Workers100Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, CreateInMem, DestroyInMem, 1, 100)
}

func BenchmarkInMemIndexing2Workers100Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, CreateInMem, DestroyInMem, 2, 100)
}

func BenchmarkInMemIndexing4Workers100Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, CreateInMem, DestroyInMem, 4, 100)
}

func BenchmarkInMemIndexing1Workers1000Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, CreateInMem, DestroyInMem, 1, 1000)
}

func BenchmarkInMemIndexing2Workers1000Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, CreateInMem, DestroyInMem, 2, 1000)
}

func BenchmarkInMemIndexing4Workers1000Batch(b *testing.B) {
	CommonBenchmarkIndexBatch(b, CreateInMem, DestroyInMem, 4, 1000)
}
