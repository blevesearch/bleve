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

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/freeway/simd"
)

// PerSegmentBlockLen is the number of postings the per segment scorer scores
// in one go.
const PerSegmentBlockLen = search.PerSegmentBlockLen

// PerSegmentTermScorer scores the postings of a single term, given nothing
// but a frequency and a norm: there's no TermFieldDoc to read from and no
// DocumentMatch to write to. It computes what TermQueryScorer does for a
// scoring (non explaining) search, with either the tf-idf or the bm25 model.
//
// It doesn't build explanations while scoring: Explain does, for a posting that
// is already known to be a hit, after the fact. Term vectors are not supported;
// those requests are to be served by TermQueryScorer.
type PerSegmentTermScorer struct {
	queryBoost   float64
	idf          float64
	avgDocLength float64 // > 0 only for bm25 scoring
	queryWeight  float64

	// what an explanation of a score shows besides the score
	docTotal  uint64
	docTerm   uint64
	queryNorm float64

	// the float32 parameters of the SIMD kernels, derived from the above
	kMul, kIdf, kK1, kOneMinusB, kB, kInvAvg, kQueryWeight float32
}

// NewPerSegmentTermScorer takes the same statistics NewTermQueryScorer does:
// docTotal is the total number of documents in the index, docTerm the number
// of documents containing the term, and avgDocLength the average document
// length (bm25 scoring only, 0 otherwise).
func NewPerSegmentTermScorer(queryBoost float64, docTotal, docTerm uint64,
	avgDocLength float64) *PerSegmentTermScorer {
	rv := &PerSegmentTermScorer{
		queryBoost:   queryBoost,
		avgDocLength: avgDocLength,
		queryWeight:  1.0,
		docTotal:     docTotal,
		docTerm:      docTerm,
	}
	// the very function the regular scorer uses, so the two can't drift
	rv.idf = (&TermQueryScorer{}).computeIDF(avgDocLength, docTotal, docTerm)
	rv.refreshKernelParams()
	return rv
}

// Weight is the term's contribution to the query norm.
func (s *PerSegmentTermScorer) Weight() float64 {
	sum := s.queryBoost * s.idf
	return sum * sum
}

// SetQueryNorm sets the query norm, which scales every score by the query
// weight.
func (s *PerSegmentTermScorer) SetQueryNorm(qnorm float64) {
	s.queryNorm = qnorm
	s.queryWeight = s.queryBoost * s.idf * qnorm
	s.refreshKernelParams()
}

func (s *PerSegmentTermScorer) refreshKernelParams() {
	s.kMul = float32(s.idf * s.queryWeight) // tf * norm * idf * queryWeight
	s.kIdf = float32(s.idf)
	s.kK1 = float32(search.BM25_k1)
	s.kOneMinusB = float32(1 - search.BM25_b)
	s.kB = float32(search.BM25_b)
	if s.avgDocLength > 0 {
		s.kInvAvg = float32(1 / s.avgDocLength)
	}
	s.kQueryWeight = float32(s.queryWeight)
}

// Score returns the score of one posting, in float64, the way TermQueryScorer
// computes it. It's the reference the SIMD ScoreBlock is checked against.
func (s *PerSegmentTermScorer) Score(freq uint32, norm float32) float64 {
	var tf float64
	if freq < MaxSqrtCache {
		tf = SqrtCache[int(freq)]
	} else {
		tf = math.Sqrt(float64(freq))
	}
	n := float64(norm)

	var score float64
	if s.avgDocLength > 0 {
		// bm25: the norm gives back the field length of the doc
		fieldLength := 1 / (n * n)
		score = s.idf * (tf * search.BM25_k1) /
			(tf + search.BM25_k1*(1-search.BM25_b+(search.BM25_b*fieldLength/s.avgDocLength)))
	} else {
		score = tf * n * s.idf
	}

	if s.queryWeight != 1.0 {
		score = score * s.queryWeight
	}
	return score
}

// ScoreBlock scores the first n postings of a block into scores, and returns
// the highest of the scores (simd.NegInf32 if n is 0). The model and the query
// weight are decided once per block, and the arithmetic is done by freeway's
// SIMD kernels, four postings at a time, in float32.
//
// The scores are therefore not bit-identical to Score's, whose float64
// arithmetic is that of TermQueryScorer; they are within float32 precision of
// them.
func (s *PerSegmentTermScorer) ScoreBlock(freqs *[PerSegmentBlockLen]uint32,
	norms *[PerSegmentBlockLen]float32, n int, scores *[PerSegmentBlockLen]float32) float32 {
	if s.avgDocLength > 0 {
		return simd.BM25_32(freqs[:], norms[:], s.kIdf, s.kK1, s.kOneMinusB, s.kB, s.kInvAvg,
			s.kQueryWeight, scores[:], n)
	}
	return simd.TFIDF32(freqs[:], norms[:], s.kMul, scores[:], n)
}

// ScoreOne scores a single posting, with the arithmetic of ScoreBlock: the
// score is the very one that ScoreBlock gives the posting.
func (s *PerSegmentTermScorer) ScoreOne(freq uint32, norm float32) float32 {
	if s.avgDocLength > 0 {
		return simd.BM25_32One(freq, norm, s.kIdf, s.kK1, s.kOneMinusB, s.kB, s.kInvAvg, s.kQueryWeight)
	}
	return simd.TFIDF32One(freq, norm, s.kMul)
}

// Prunable reports whether the scores of the term are all non negative, which
// is what pruning needs: the more terms match, the higher a score. They are
// unless the term's statistics are off. They can be: the number of documents
// having the term doesn't discount deletions, so it can top the number of
// documents there are, and then the idf is negative.
func (s *PerSegmentTermScorer) Prunable() bool {
	return s.idf > 0 && s.queryWeight > 0
}

// boundSlack is what an upper bound is inflated by, so that float32 rounding
// in the chain of operations that makes a score can't leave a real score a hair
// above its bound.
const boundSlack = 1e-5

// UpperBound is a bound on the score of any posting whose frequency is at most
// maxFreq and whose norm factor is at most maxNorm. A score rises with both, so
// that of the pair is one.
//
// If the frequency isn't bounded, the bound is that of an infinite
// frequency: the saturation limit of bm25, nothing finite for tf-idf (which
// grows without bound with the frequency).
func (s *PerSegmentTermScorer) UpperBound(maxFreq uint32, maxNorm float32, freqBounded bool) float32 {
	if !s.Prunable() {
		// a score that can go down as the frequency goes up has no bound of
		// this kind
		return float32(math.Inf(1))
	}
	var bound float32
	switch {
	case freqBounded:
		bound = s.ScoreOne(maxFreq, maxNorm)
	case s.avgDocLength > 0:
		// idf * k1 * tf / (tf + k1*(...)) tends to idf * k1 as tf grows
		bound = s.kIdf * s.kK1 * s.kQueryWeight
	default:
		return float32(math.Inf(1))
	}
	if math.IsInf(float64(bound), 0) || bound != bound {
		return float32(math.Inf(1))
	}
	return bound * (1 + boundSlack)
}
