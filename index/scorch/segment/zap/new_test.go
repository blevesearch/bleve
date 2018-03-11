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
	benchmarkRoaringAdd(b, 1, 1, 0)
}

func BenchmarkRoaringAdd10(b *testing.B) {
	benchmarkRoaringAdd(b, 10, 1, 0)
}

func BenchmarkRoaringAdd1K(b *testing.B) {
	benchmarkRoaringAdd(b, 1000, 1, 0)
}

func BenchmarkRoaringAdd1M(b *testing.B) {
	benchmarkRoaringAdd(b, 1000000, 1, 0)
}

func BenchmarkRoaringAdd10M(b *testing.B) {
	benchmarkRoaringAdd(b, 10000000, 1, 0)
}

func benchmarkRoaringAdd(b *testing.B, n uint32,
	runSize uint32, gapBetweenRuns uint32) {
	for i := 0; i < b.N; i++ {
		r := roaring.New()
		for j := uint32(0); j < n; j++ {
			runEnd := j + runSize
			for j < runEnd && j < n {
				r.Add(j)
				j += 1
			}
			j += gapBetweenRuns
		}
	}
}

// ----------------------------------------

func BenchmarkRoaringAddRange1(b *testing.B) {
	benchmarkRoaringAddRange(b, 1, 1, 0)
}

func BenchmarkRoaringAddRange10(b *testing.B) {
	benchmarkRoaringAddRange(b, 10, 1, 0)
}

func BenchmarkRoaringAddRange1K(b *testing.B) {
	benchmarkRoaringAddRange(b, 1000, 1, 0)
}

func BenchmarkRoaringAddRange1M(b *testing.B) {
	benchmarkRoaringAddRange(b, 1000000, 1, 0)
}

func BenchmarkRoaringAddRange10M(b *testing.B) {
	benchmarkRoaringAddRange(b, 10000000, 1, 0)
}

func benchmarkRoaringAddRange(b *testing.B, n uint64,
	runSize uint64, gapBetweenRuns uint64) {
	for i := 0; i < b.N; i++ {
		r := roaring.New()
		for j := uint64(0); j < n; j++ {
			runEnd := j + runSize
			r.AddRange(j, runEnd)
			j += runEnd + gapBetweenRuns
		}
	}
}

// ----------------------------------------

func BenchmarkInterimDocNums1(b *testing.B) {
	benchmarkInterimDocNums(b, 1, 1, 0)
}

func BenchmarkInterimDocNums10(b *testing.B) {
	benchmarkInterimDocNums(b, 10, 1, 0)
}

func BenchmarkInterimDocNums1K(b *testing.B) {
	benchmarkInterimDocNums(b, 1000, 1, 0)
}

func BenchmarkInterimDocNums1M(b *testing.B) {
	benchmarkInterimDocNums(b, 1000000, 1, 0)
}

func BenchmarkInterimDocNums10M(b *testing.B) {
	benchmarkInterimDocNums(b, 10000000, 1, 0)
}

func benchmarkInterimDocNums(b *testing.B, n uint64,
	runSize uint64, gapBetweenRuns uint64) {
	for i := 0; i < b.N; i++ {
		r := &interimDocNums{}
		for j := uint64(0); j < n; j++ {
			runEnd := j + runSize
			for j < runEnd && j < n {
				r.add(j)
				j += 1
			}
			j += gapBetweenRuns
		}
		r.incorporateLastRange()
	}
}

// ----------------------------------------

func BenchmarkRun1Skip1RoaringAdd1M(b *testing.B) {
	benchmarkRoaringAdd(b, 1000000, 1, 1)
}

func BenchmarkRun1Skip1RoaringAddRange1M(b *testing.B) {
	benchmarkRoaringAddRange(b, 1000000, 1, 1)
}

func BenchmarkRun1Skip1InterimDocNums1M(b *testing.B) {
	benchmarkInterimDocNums(b, 1000000, 1, 1)
}

func BenchmarkRun2Skip1RoaringAdd1M(b *testing.B) {
	benchmarkRoaringAdd(b, 1000000, 2, 1)
}

func BenchmarkRun2Skip1RoaringAddRange1M(b *testing.B) {
	benchmarkRoaringAddRange(b, 1000000, 2, 1)
}

func BenchmarkRun2Skip1InterimDocNums1M(b *testing.B) {
	benchmarkInterimDocNums(b, 1000000, 2, 1)
}

func BenchmarkRun4Skip16RoaringAdd1M(b *testing.B) {
	benchmarkRoaringAdd(b, 1000000, 4, 16)
}

func BenchmarkRun4Skip16RoaringAddRange1M(b *testing.B) {
	benchmarkRoaringAddRange(b, 1000000, 4, 16)
}

func BenchmarkRun4Skip16InterimDocNums1M(b *testing.B) {
	benchmarkInterimDocNums(b, 1000000, 4, 16)
}

// ----------------------------------------

func BenchmarkRoaringClear(b *testing.B) {
	r := roaring.New()
	b.ResetTimer()
    for i := 0; i < b.N; i++ {
		r.Clear()
	}
}
