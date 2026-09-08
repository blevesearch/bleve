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

import (
	"math"
	"math/rand"
	"testing"
)

// genInputs builds n pairs of (freq, norm) values covering the shapes that
// matter for this arithmetic: realistic small term frequencies, the
// saturated/extreme ends, and random values in between. norms is never zero
// -- fieldLength divides by norm*norm, and a real segment's fieldnorm column
// never stores exactly zero either.
func genInputs(n int, seed int64) (freqs []uint64, norms []float64) {
	rng := rand.New(rand.NewSource(seed))
	freqs = make([]uint64, n)
	norms = make([]float64, n)
	for i := 0; i < n; i++ {
		switch i % 4 {
		case 0:
			freqs[i] = uint64(rng.Intn(20))
		case 1:
			freqs[i] = uint64(rng.Intn(5000))
		case 2:
			freqs[i] = 0 // a real freq can be this small
		default:
			freqs[i] = uint64(1 + rng.Intn(1<<20))
		}
		norms[i] = 0.05 + rng.Float64()*4 // a real fieldnorm factor's plausible range
	}
	return freqs, norms
}

func TestBM25MatchesPortable(t *testing.T) {
	if !hasAsm {
		t.Skip("no assembly kernel on this architecture")
	}
	for _, n := range []int{0, 2, 8, 64, 128, 254} {
		freqs, norms := genInputs(n, int64(n)*7+1)
		idf, k1, oneMinusB, b, invAvgDocLength, queryWeight := 1.732, 1.2, 0.25, 0.75, 1.0/812.4, 3.14

		got := make([]float64, n)
		want := make([]float64, n)
		bm25(freqs, norms, idf, k1, oneMinusB, b, invAvgDocLength, queryWeight, got, n)
		bm25Portable(freqs, norms, idf, k1, oneMinusB, b, invAvgDocLength, queryWeight, want, n)

		for i := 0; i < n; i++ {
			if got[i] != want[i] {
				t.Fatalf("n=%d i=%d: asm %v != portable %v (freq=%d norm=%v)",
					n, i, got[i], want[i], freqs[i], norms[i])
			}
		}
	}
}

func TestTFIDFMatchesPortable(t *testing.T) {
	if !hasAsm {
		t.Skip("no assembly kernel on this architecture")
	}
	for _, n := range []int{0, 2, 8, 64, 128, 254} {
		freqs, norms := genInputs(n, int64(n)*13+3)
		idf, queryWeight := 2.5, 0.9

		got := make([]float64, n)
		want := make([]float64, n)
		tfidf(freqs, norms, idf, queryWeight, got, n)
		tfidfPortable(freqs, norms, idf, queryWeight, want, n)

		for i := 0; i < n; i++ {
			if got[i] != want[i] {
				t.Fatalf("n=%d i=%d: asm %v != portable %v (freq=%d norm=%v)",
					n, i, got[i], want[i], freqs[i], norms[i])
			}
		}
	}
}

// FuzzBM25 checks the same asm-vs-portable identity against whatever inputs
// go-fuzz's corpus mutation finds, not just the hand-picked cases above.
func FuzzBM25(f *testing.F) {
	f.Add(uint64(2), 0.75, int64(11))
	f.Add(uint64(0), 3.9, int64(2))
	f.Add(uint64(1000), 0.05, int64(128))
	f.Fuzz(func(t *testing.T, freqSeed uint64, normSeed float64, nSeed int64) {
		if !hasAsm {
			t.Skip("no assembly kernel on this architecture")
		}
		n := int(nSeed % 130)
		if n < 0 {
			n = -n
		}
		n &^= 1
		if math.IsNaN(normSeed) || math.IsInf(normSeed, 0) || normSeed == 0 {
			t.Skip("not a representable norm")
		}
		freqs := make([]uint64, n)
		norms := make([]float64, n)
		for i := range freqs {
			freqs[i] = freqSeed + uint64(i)
			norms[i] = math.Abs(normSeed) + 0.01
		}
		got := make([]float64, n)
		want := make([]float64, n)
		bm25(freqs, norms, 1.5, 1.2, 0.25, 0.75, 0.001, 1.0, got, n)
		bm25Portable(freqs, norms, 1.5, 1.2, 0.25, 0.75, 0.001, 1.0, want, n)
		for i := 0; i < n; i++ {
			if got[i] != want[i] && !(math.IsNaN(got[i]) && math.IsNaN(want[i])) {
				t.Fatalf("i=%d: asm %v != portable %v", i, got[i], want[i])
			}
		}
	})
}
