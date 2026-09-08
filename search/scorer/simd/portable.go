//  Copyright (c) 2026 Couchbase, Inc.
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

package simd

import "math"

// The portable kernels. They evaluate every step in the same order and
// grouping the vector kernels do (see the package doc comment), so they are
// not just a correctness fallback for architectures without a hand-written
// kernel -- they are also the reference TestMatchesScalar checks the real
// kernels against, and on amd64/arm64 without the SIMD build they are what
// actually runs.
//
// math.Sqrt is used here rather than scorer.SqrtCache deliberately: the two
// are bit-identical for every input (see the package doc comment), and using
// the same sqrt call the vector kernels' hardware instruction matches keeps
// this file architecture-agnostic instead of importing the table just to
// mirror it.

func bm25Portable(freqs []uint64, norms []float64, idf, k1, oneMinusB, b, invAvgDocLength, queryWeight float64, out []float64, n int) {
	for i := 0; i < n; i++ {
		tf := math.Sqrt(float64(freqs[i]))
		sq := norms[i] * norms[i]
		fieldLength := 1 / sq
		t1 := b * fieldLength
		t2 := t1 * invAvgDocLength
		inner := oneMinusB + t2
		t3 := k1 * inner
		denom := tf + t3
		t4 := tf * k1
		numer := idf * t4
		score := numer / denom
		out[i] = score * queryWeight
	}
}

func tfidfPortable(freqs []uint64, norms []float64, idf, queryWeight float64, out []float64, n int) {
	for i := 0; i < n; i++ {
		tf := math.Sqrt(float64(freqs[i]))
		score := tf * norms[i] * idf
		out[i] = score * queryWeight
	}
}
