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

// Package simd vectorizes the arithmetic TermQueryScorer.ScoreBulk runs once
// per document in a block: term-frequency square-rooting followed by BM25 (or
// plain tf-idf) applied to the result and a field-norm value.
//
// This is ordinary floating point, not the bit-shuffling bitpack.BlockLen
// vectorizes in zapx -- there is no shared instruction schedule between the
// amd64 and arm64 kernels here, because there is nothing architecture-
// specific to share: both are the same straight-line sequence of packed
// convert/sqrt/multiply/add/divide, expressed directly in each backend's own
// generator (see gen/avo and gen/goat).
//
// Both kernels compute two documents' scores per vector op (SSE2's XMM and
// NEON's 128-bit register both hold two float64 lanes), and, critically,
// evaluate every step in the exact same left-to-right order the scalar
// formula in scorer_term.go's docScore does -- b*fieldLength computed first,
// *then* multiplied by invAvgDocLength, never a fused b*invAvgDocLength
// constant, even though that would be mathematically equal. IEEE 754 packed
// arithmetic is exact per lane -- there is no reduced-precision fast-math
// mode for ADDPD/MULPD/DIVPD or NEON's FADD/FMUL/FDIV -- but a different
// *grouping* of the same multiplications can round differently in the last
// bit. Preserving both the order and the grouping is what makes every path
// that scores a (freq, norm) pair -- the scalar Score(), MaxScore()'s
// block-max upper bound, and this bulk path -- produce bit-identical
// results, which block-max WAND depends on: MaxScore's bound has to be a
// real upper bound on whatever ScoreBulk goes on to compute for the same
// inputs, not just approximately one. TestMatchesScalar in simd_test.go
// checks the identity directly, and is the guard against a future edit
// changing one side's grouping but not the other's.
//
// The freq-to-tf step is a hardware sqrt (SQRTPD / FSQRT), not a table
// lookup: IEEE 754 requires sqrt specifically -- unlike +, -, *, / -- to be
// correctly rounded, so it reproduces scorer.SqrtCache's table bit-for-bit
// for every input, not just the cached range. That's what lets this kernel
// take raw freqs directly instead of a precomputed tf slice, dropping a
// scalar gather loop (table lookups don't vectorize without a gather
// instruction, which SSE2 and NEON both lack) with no separate bit-exactness
// argument to maintain for it.
//
// amd64's uint64-to-double conversion goes through CVTSI2SD, a *signed*
// convert -- there is no unsigned 64-bit int-to-double instruction before
// AVX-512. This is exact for any freq under 2^63, which every real term
// frequency is by an astronomical margin (a document would need over 4
// billion occurrences of one term to reach it). NEON's UCVTF converts
// unsigned natively, so the arm64 kernel has no such bound at all.
//
// No runtime CPU detection, matching bitpack's own policy: SSE2 is baseline
// on every amd64 Go target, NEON on every arm64 one.
package simd

// BM25 computes n scores -- idf*(tf[i]*k1) / (tf[i] + k1*(oneMinusB +
// b*(1/(norms[i]*norms[i]))*invAvgDocLength)) * queryWeight, where
// tf[i] = sqrt(float64(freqs[i])) -- into out[:n].
//
// n must be even; ScoreBulk handles the odd tail element itself (see its own
// doc comment) so this never has to branch around a half-empty vector lane.
// freqs, norms and out must each have length >= n.
func BM25(freqs []uint64, norms []float64, idf, k1, oneMinusB, b, invAvgDocLength, queryWeight float64, out []float64, n int) {
	bm25(freqs, norms, idf, k1, oneMinusB, b, invAvgDocLength, queryWeight, out, n)
}

// TFIDF computes n scores -- sqrt(float64(freqs[i]))*norms[i]*idf*queryWeight
// -- into out[:n]. Same evenness/length contract as BM25.
func TFIDF(freqs []uint64, norms []float64, idf, queryWeight float64, out []float64, n int) {
	tfidf(freqs, norms, idf, queryWeight, out, n)
}
