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
	"fmt"
	"math"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// Explain describes the score of a posting of the term: the explanation that
// TermQueryScorer builds, with the same structure and messages, for the posting
// of the doc id that has the frequency and the norm.
//
// It is for the hits that a search returns, once they are known: scoring itself
// never builds an explanation. The score at the root is the very one that
// ScoreOne gives the posting, and so the one the hit was ranked by, to the bit;
// the parts below it are worked out in float64 the way TermQueryScorer does, as
// the SIMD kernels don't have such parts to show.
func (s *PerSegmentTermScorer) Explain(field, term string, id index.IndexInternalID,
	freq uint32, norm float32) *search.Explanation {
	var tf float64
	if freq < MaxSqrtCache {
		tf = SqrtCache[int(freq)]
	} else {
		tf = math.Sqrt(float64(freq))
	}
	n := float64(norm)

	idfExplanation := &search.Explanation{
		Value:   s.idf,
		Message: fmt.Sprintf("idf(docFreq=%d, maxDocs=%d)", s.docTerm, s.docTotal),
	}

	var score float64
	var model string
	var children []*search.Explanation
	if s.avgDocLength > 0 {
		fieldLength := 1 / (n * n)
		fieldNormVal := 1 - search.BM25_b + (search.BM25_b * fieldLength / s.avgDocLength)
		score = s.idf * (tf * search.BM25_k1) / (tf + search.BM25_k1*fieldNormVal)
		model = index.BM25Scoring

		fieldNormalizeExplanation := &search.Explanation{
			Value: fieldNormVal,
			Message: fmt.Sprintf("fieldNorm(field=%s), b=%f, fieldLength=%f, avgFieldLength=%f)",
				field, search.BM25_b, fieldLength, s.avgDocLength),
		}
		saturationExplanation := &search.Explanation{
			Value: search.BM25_k1 / (tf + search.BM25_k1*fieldNormVal),
			Message: fmt.Sprintf("saturation(term:%s), k1=%f/(tf=%f + k1*fieldNorm=%f))",
				term, search.BM25_k1, tf, fieldNormVal),
			Children: []*search.Explanation{fieldNormalizeExplanation},
		}
		children = []*search.Explanation{
			{Value: tf, Message: fmt.Sprintf("tf(termFreq(%s:%s)=%d", field, term, freq)},
			saturationExplanation,
			idfExplanation,
		}
	} else {
		score = tf * n * s.idf
		model = index.DefaultScoringModel
		children = []*search.Explanation{
			{Value: tf, Message: fmt.Sprintf("tf(termFreq(%s:%s)=%d", field, term, freq)},
			{Value: n, Message: fmt.Sprintf("fieldNorm(field=%s, doc=%s)", field, id)},
			idfExplanation,
		}
	}

	// the score the posting was ranked by
	final := float64(s.ScoreOne(freq, norm))

	fieldWeight := &search.Explanation{
		Value: score,
		Message: fmt.Sprintf("fieldWeight(%s:%s in %s), as per %s model, "+
			"product of:", field, term, id, model),
		Children: children,
	}
	if s.queryWeight == 1.0 {
		fieldWeight.Value = final
		return fieldWeight
	}

	queryWeight := &search.Explanation{
		Value:   s.queryWeight,
		Message: fmt.Sprintf("queryWeight(%s:%s^%f), product of:", field, term, s.queryBoost),
		Children: []*search.Explanation{
			{Value: s.queryBoost, Message: "boost"},
			idfExplanation,
			{Value: s.queryNorm, Message: "queryNorm"},
		},
	}
	return &search.Explanation{
		Value:    final,
		Message:  fmt.Sprintf("weight(%s:%s^%f in %s), product of:", field, term, s.queryBoost, id),
		Children: []*search.Explanation{queryWeight, fieldWeight},
	}
}
