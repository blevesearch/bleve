//  Copyright (c) 2018 Couchbase, Inc.
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

package zap

import (
	"testing"

	"github.com/RoaringBitmap/roaring"
)

func BenchmarkRoaringAdd1(b *testing.B) {
	benchmarkRoaringAdd(b, 1)
}

func BenchmarkRoaringAdd10(b *testing.B) {
	benchmarkRoaringAdd(b, 10)
}

func BenchmarkRoaringAdd1K(b *testing.B) {
	benchmarkRoaringAdd(b, 1000)
}

func BenchmarkRoaringAdd1M(b *testing.B) {
	benchmarkRoaringAdd(b, 1000000)
}

func BenchmarkRoaringAdd10M(b *testing.B) {
	benchmarkRoaringAdd(b, 10000000)
}

func benchmarkRoaringAdd(b *testing.B, n uint32) {
	r := roaring.New()
	for i := 0; i < b.N; i++ {
		for j := uint32(0); j < n; j++ {
			r.Add(j)
		}
	}
}

func BenchmarkRoaringAddRange1(b *testing.B) {
	benchmarkRoaringAddRange(b, 1)
}

func BenchmarkRoaringAddRange10(b *testing.B) {
	benchmarkRoaringAddRange(b, 10)
}

func BenchmarkRoaringAddRange1K(b *testing.B) {
	benchmarkRoaringAddRange(b, 1000)
}

func BenchmarkRoaringAddRange1M(b *testing.B) {
	benchmarkRoaringAddRange(b, 1000000)
}

func BenchmarkRoaringAddRange10M(b *testing.B) {
	benchmarkRoaringAddRange(b, 10000000)
}

func benchmarkRoaringAddRange(b *testing.B, n uint64) {
	r := roaring.New()
	for i := 0; i < b.N; i++ {
		r.AddRange(0, n)
	}
}
