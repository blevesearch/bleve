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

package scorer

import (
	"math"
	"math/rand"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// the scalar Score has to agree with TermQueryScorer, whose docScore the
// regular path uses, for every freq and norm, with both scoring models; and
// ScoreBlock, which is float32 SIMD, with them to within float32 precision.
func TestPerSegmentTermScorerMatchesTermQueryScorer(t *testing.T) {
	freqs := []uint32{1, 2, 3, 7, 63, 64, 65, 1000, 1 << 20}
	norms := []float32{float32(math.Inf(1)), 1, 0.7071068, 0.5, 0.31622776, 0.1, 0.01}

	for _, tc := range []struct {
		name         string
		avgDocLength float64
	}{{"tfidf", 0}, {"bm25", 12.0}} {
		for _, boost := range []float64{1.0, 2.5} {
			for _, qnorm := range []float64{-1, 0.5} { // -1: not set
				old := NewTermQueryScorer([]byte("t"), "f", boost, 1000, 37, tc.avgDocLength, search.SearcherOptions{})
				ps := NewPerSegmentTermScorer(boost, 1000, 37, tc.avgDocLength)
				if old.Weight() != ps.Weight() {
					t.Fatalf("%s: weight %v != %v", tc.name, ps.Weight(), old.Weight())
				}
				if qnorm >= 0 {
					old.SetQueryNorm(qnorm)
					ps.SetQueryNorm(qnorm)
				}

				var fr [segment.PostingsBlockLen]uint32
				var nr [segment.PostingsBlockLen]float32
				var sc [segment.PostingsBlockLen]float32
				n := 0
				for _, f := range freqs {
					for _, nm := range norms {
						fr[n], nr[n] = f, nm
						n++
					}
				}
				blockMax := ps.ScoreBlock(&fr, &nr, n, &sc)
				wantMax := math.Inf(-1)

				for i := 0; i < n; i++ {
					// what the regular path does with a TermFieldDoc
					want, _ := old.docScore(oldTF(fr[i]), float64(nr[i]))
					if old.queryWeight != 1.0 {
						want *= old.queryWeight
					}
					if got := ps.Score(fr[i], nr[i]); !sameFloat(got, want) {
						t.Fatalf("%s boost=%v qnorm=%v freq=%d norm=%v: Score %v, want %v",
							tc.name, boost, qnorm, fr[i], nr[i], got, want)
					}
					if !closeFloat32(sc[i], want) {
						t.Fatalf("%s boost=%v qnorm=%v freq=%d norm=%v: ScoreBlock %v, want %v",
							tc.name, boost, qnorm, fr[i], nr[i], sc[i], want)
					}
					if v := float64(sc[i]); v > wantMax {
						wantMax = v
					}
				}
				if float64(blockMax) != wantMax {
					t.Fatalf("%s: block max %v, want %v", tc.name, blockMax, wantMax)
				}
			}
		}
	}
}

func oldTF(freq uint32) float64 {
	if freq < MaxSqrtCache {
		return SqrtCache[int(freq)]
	}
	return math.Sqrt(float64(freq))
}

func sameFloat(a, b float64) bool {
	return a == b || (math.IsNaN(a) && math.IsNaN(b))
}

// within float32 precision (a handful of ulps over a chain of operations)
func closeFloat32(got float32, want float64) bool {
	g := float64(got)
	if math.IsInf(want, 0) || math.IsNaN(want) {
		return g == want || (math.IsNaN(g) && math.IsNaN(want))
	}
	return math.Abs(g-want) <= 1e-5*math.Abs(want)
}

// ScoreOne is the score ScoreBlock gives, to the bit
func TestPerSegmentTermScorerScoreOneMatchesScoreBlock(t *testing.T) {
	for _, avg := range []float64{0, 9.5} {
		for _, qnorm := range []float64{-1, 0.37} {
			ps := NewPerSegmentTermScorer(1.5, 5000, 120, avg)
			if qnorm >= 0 {
				ps.SetQueryNorm(qnorm)
			}
			var fr [segment.PostingsBlockLen]uint32
			var nr [segment.PostingsBlockLen]float32
			var sc [segment.PostingsBlockLen]float32
			for i := range fr {
				fr[i] = uint32(1 + i*i%900)
				nr[i] = float32(1.0 / math.Sqrt(float64(1+i%40)))
			}
			nr[5] = float32(math.Inf(1))
			ps.ScoreBlock(&fr, &nr, segment.PostingsBlockLen, &sc)
			for i := range fr {
				if one := ps.ScoreOne(fr[i], nr[i]); math.Float32bits(one) != math.Float32bits(sc[i]) {
					t.Fatalf("avg=%v i=%d: ScoreOne %v, ScoreBlock %v", avg, i, one, sc[i])
				}
			}
		}
	}
}

// UpperBound has to be at least the score of every posting it bounds: that's
// what pruning stands on.
func TestPerSegmentTermScorerUpperBoundBounds(t *testing.T) {
	rnd := rand.New(rand.NewSource(8))
	for _, avg := range []float64{0, 3, 12, 60} {
		for _, qnorm := range []float64{-1, 0.2, 1.7} {
			for _, boost := range []float64{1, 3.5} {
				ps := NewPerSegmentTermScorer(boost, 100000, uint64(1+rnd.Intn(50000)), avg)
				if qnorm >= 0 {
					ps.SetQueryNorm(qnorm)
				}
				for trial := 0; trial < 300; trial++ {
					// a block: bounds from its max freq and max norm
					n := 1 + rnd.Intn(128)
					var maxFreq uint32
					var maxNorm float32
					type posting struct {
						freq uint32
						norm float32
					}
					ps0 := make([]posting, n)
					for i := range ps0 {
						f := uint32(1 + rnd.Intn(200))
						if rnd.Intn(30) == 0 {
							f = uint32(1 + rnd.Intn(1<<20))
						}
						norm := float32(1 / math.Sqrt(float64(1+rnd.Intn(5000))))
						ps0[i] = posting{f, norm}
						maxFreq = max(maxFreq, f)
						maxNorm = max(maxNorm, norm)
					}
					for _, bounded := range []bool{true, false} {
						ub := ps.UpperBound(maxFreq, maxNorm, bounded)
						for _, p := range ps0 {
							if sc := ps.ScoreOne(p.freq, p.norm); sc > ub {
								t.Fatalf("avg=%v bounded=%v: score %v of (%d,%v) above its bound %v (max %d,%v)",
									avg, bounded, sc, p.freq, p.norm, ub, maxFreq, maxNorm)
							}
						}
					}
				}
			}
		}
	}
}

func TestPerSegmentTermScorerUpperBoundUnbounded(t *testing.T) {
	if ub := NewPerSegmentTermScorer(1, 1000, 10, 0).UpperBound(255, 1, false); !math.IsInf(float64(ub), 1) {
		t.Fatalf("tf-idf with an unbounded frequency can't be bounded, got %v", ub)
	}
	if ub := NewPerSegmentTermScorer(1, 1000, 10, 7).UpperBound(255, 1, false); math.IsInf(float64(ub), 0) || ub <= 0 {
		t.Fatalf("bm25 saturates, got %v", ub)
	}
	// nothing in the block: nothing to score
	if ub := NewPerSegmentTermScorer(1, 1000, 10, 7).UpperBound(0, 0, true); ub != 0 {
		t.Fatalf("empty block bound %v", ub)
	}
}

// a term that "occurs" in more documents than there are has a negative idf, as
// the count of its postings doesn't discount deletions: nothing about its
// scores can be bounded
func TestPerSegmentTermScorerNotPrunableWithNegativeIDF(t *testing.T) {
	ps := NewPerSegmentTermScorer(1, 100, 1000, 7) // bm25: df >> N
	if ps.Prunable() {
		t.Fatal("negative idf must not be prunable")
	}
	if ub := ps.UpperBound(3, 1, true); !math.IsInf(float64(ub), 1) {
		t.Fatalf("bound %v", ub)
	}
	if !NewPerSegmentTermScorer(1, 1000, 10, 7).Prunable() || !NewPerSegmentTermScorer(1, 1000, 10, 0).Prunable() {
		t.Fatal("an ordinary term is prunable")
	}
}
