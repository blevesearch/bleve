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

//go:build (!amd64 && !arm64) || purego

package simd

// Architectures without an assembly implementation, and any build that asks
// for purego, run the portable kernels directly.

const hasAsm = false

func bm25(freqs []uint64, norms []float64, idf, k1, oneMinusB, b, invAvgDocLength, queryWeight float64, out []float64, n int) {
	bm25Portable(freqs, norms, idf, k1, oneMinusB, b, invAvgDocLength, queryWeight, out, n)
}

func tfidf(freqs []uint64, norms []float64, idf, queryWeight float64, out []float64, n int) {
	tfidfPortable(freqs, norms, idf, queryWeight, out, n)
}
