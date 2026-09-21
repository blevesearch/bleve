//  Copyright (c) 2013 Couchbase, Inc.
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
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

func TestTermScorer(t *testing.T) {

	var docTotal uint64 = 100
	var docTerm uint64 = 9
	var queryTerm = []byte("beer")
	var queryField = "desc"
	var queryBoost = 1.0
	scorer := NewTermQueryScorer(queryTerm, queryField, queryBoost, docTotal, docTerm, 0, search.SearcherOptions{Explain: true})
	idf := 1.0 + math.Log(float64(docTotal)/float64(docTerm+1.0))

	tests := []struct {
		termMatch *index.TermFieldDoc
		result    *search.DocumentMatch
	}{
		// test some simple math
		{
			termMatch: &index.TermFieldDoc{
				ID:   index.IndexInternalID("one"),
				Freq: 1,
				Norm: 1.0,
				Vectors: []*index.TermFieldVector{
					{
						Field: "desc",
						Pos:   1,
						Start: 0,
						End:   4,
					},
				},
			},
			result: &search.DocumentMatch{
				IndexInternalID: index.IndexInternalID("one"),
				Score:           math.Sqrt(1.0) * idf,
				Sort:            []string{},
				Expl: &search.Explanation{
					Value:   math.Sqrt(1.0) * idf,
					Message: "fieldWeight(desc:beer in one), as per tf-idf model, product of:",
					Children: []*search.Explanation{
						{
							Value:   1,
							Message: "tf(termFreq(desc:beer)=1",
						},
						{
							Value:   1,
							Message: "fieldNorm(field=desc, doc=one)",
						},
						{
							Value:   idf,
							Message: "idf(docFreq=9, maxDocs=100)",
						},
					},
				},
				Locations: search.FieldTermLocationMap{
					"desc": search.TermLocationMap{
						"beer": []*search.Location{
							{
								Pos:   1,
								Start: 0,
								End:   4,
							},
						},
					},
				},
			},
		},
		// test the same thing again (score should be cached this time)
		{
			termMatch: &index.TermFieldDoc{
				ID:   index.IndexInternalID("one"),
				Freq: 1,
				Norm: 1.0,
			},
			result: &search.DocumentMatch{
				IndexInternalID: index.IndexInternalID("one"),
				Score:           math.Sqrt(1.0) * idf,
				Sort:            []string{},
				Expl: &search.Explanation{
					Value:   math.Sqrt(1.0) * idf,
					Message: "fieldWeight(desc:beer in one), as per tf-idf model, product of:",
					Children: []*search.Explanation{
						{
							Value:   1,
							Message: "tf(termFreq(desc:beer)=1",
						},
						{
							Value:   1,
							Message: "fieldNorm(field=desc, doc=one)",
						},
						{
							Value:   idf,
							Message: "idf(docFreq=9, maxDocs=100)",
						},
					},
				},
			},
		},
		// test a case where the sqrt isn't precalculated
		{
			termMatch: &index.TermFieldDoc{
				ID:   index.IndexInternalID("one"),
				Freq: 65,
				Norm: 1.0,
			},
			result: &search.DocumentMatch{
				IndexInternalID: index.IndexInternalID("one"),
				Score:           math.Sqrt(65) * idf,
				Sort:            []string{},
				Expl: &search.Explanation{
					Value:   math.Sqrt(65) * idf,
					Message: "fieldWeight(desc:beer in one), as per tf-idf model, product of:",
					Children: []*search.Explanation{
						{
							Value:   math.Sqrt(65),
							Message: "tf(termFreq(desc:beer)=65",
						},
						{
							Value:   1,
							Message: "fieldNorm(field=desc, doc=one)",
						},
						{
							Value:   idf,
							Message: "idf(docFreq=9, maxDocs=100)",
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		ctx := &search.SearchContext{
			DocumentMatchPool: search.NewDocumentMatchPool(1, 0),
		}
		actual := scorer.Score(ctx, test.termMatch)
		actual.Complete(nil)
		if len(actual.FieldTermLocations) == 0 {
			actual.FieldTermLocations = nil
		}

		if !reflect.DeepEqual(actual, test.result) {
			t.Errorf("expected %#v got %#v for %#v", test.result, actual, test.termMatch)
		}
	}

}

func TestTermScorerWithQueryNorm(t *testing.T) {

	var docTotal uint64 = 100
	var docTerm uint64 = 9
	var queryTerm = []byte("beer")
	var queryField = "desc"
	var queryBoost = 3.0
	scorer := NewTermQueryScorer(queryTerm, queryField, queryBoost, docTotal, docTerm, 0, search.SearcherOptions{Explain: true})
	idf := 1.0 + math.Log(float64(docTotal)/float64(docTerm+1.0))

	scorer.SetQueryNorm(2.0)

	expectedQueryWeight := 3 * idf * 3 * idf
	actualQueryWeight := scorer.Weight()
	if expectedQueryWeight != actualQueryWeight {
		t.Errorf("expected query weight %f, got %f", expectedQueryWeight, actualQueryWeight)
	}

	tests := []struct {
		termMatch *index.TermFieldDoc
		result    *search.DocumentMatch
	}{
		{
			termMatch: &index.TermFieldDoc{
				ID:   index.IndexInternalID("one"),
				Freq: 1,
				Norm: 1.0,
			},
			result: &search.DocumentMatch{
				IndexInternalID: index.IndexInternalID("one"),
				Score:           math.Sqrt(1.0) * idf * 3.0 * idf * 2.0,
				Sort:            []string{},
				Expl: &search.Explanation{
					Value:   math.Sqrt(1.0) * idf * 3.0 * idf * 2.0,
					Message: "weight(desc:beer^3.000000 in one), product of:",
					Children: []*search.Explanation{
						{
							Value:   2.0 * idf * 3.0,
							Message: "queryWeight(desc:beer^3.000000), product of:",
							Children: []*search.Explanation{
								{
									Value:   3,
									Message: "boost",
								},
								{
									Value:   idf,
									Message: "idf(docFreq=9, maxDocs=100)",
								},
								{
									Value:   2,
									Message: "queryNorm",
								},
							},
						},
						{
							Value:   math.Sqrt(1.0) * idf,
							Message: "fieldWeight(desc:beer in one), as per tf-idf model, product of:",
							Children: []*search.Explanation{
								{
									Value:   1,
									Message: "tf(termFreq(desc:beer)=1",
								},
								{
									Value:   1,
									Message: "fieldNorm(field=desc, doc=one)",
								},
								{
									Value:   idf,
									Message: "idf(docFreq=9, maxDocs=100)",
								},
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		ctx := &search.SearchContext{
			DocumentMatchPool: search.NewDocumentMatchPool(1, 0),
		}
		actual := scorer.Score(ctx, test.termMatch)

		if !reflect.DeepEqual(actual, test.result) {
			t.Errorf("expected %#v got %#v for %#v", test.result, actual, test.termMatch)
		}
	}

}

// TestScoreBulkMatchesDocScore is the end-to-end guarantee the simd package's
// own bit-exactness only proxies for: block-max WAND's MaxScore() bound has
// to hold against whatever ScoreBulk actually computes, so ScoreBulk's
// vectorized output must match scoring the same documents one at a time
// through docScore/Score exactly, not just approximately. Covers both BM25
// (avgDocLength > 0) and plain tf-idf (avgDocLength == 0), and both even and
// odd document counts (the odd tail goes through docScore directly, so an
// off-by-one there would otherwise slip past the simd package's own tests).
func TestScoreBulkMatchesDocScore(t *testing.T) {
	var docTotal uint64 = 5000
	var docTerm uint64 = 137
	queryTerm := []byte("beer")
	queryField := "desc"

	for _, avgDocLength := range []float64{0, 812.4} {
		mode := "tf-idf"
		if avgDocLength > 0 {
			mode = "bm25"
		}
		t.Run(mode, func(t *testing.T) {
			scorer := NewTermQueryScorer(queryTerm, queryField, 1.75, docTotal, docTerm, avgDocLength, search.SearcherOptions{})
			scorer.SetQueryNorm(1.3)

			for _, n := range []int{0, 1, 2, 3, 8, 9, 64, 65} {
				freqs := make([]uint64, n)
				norms := make([]float64, n)
				for i := 0; i < n; i++ {
					switch i % 4 {
					case 0:
						freqs[i] = uint64(i % 20)
					case 1:
						freqs[i] = uint64(5000 + i)
					case 2:
						freqs[i] = 0
					default:
						freqs[i] = uint64(1 + i*7)
					}
					norms[i] = 0.05 + float64(i%37)*0.1
				}

				got := make([]float64, n)
				scorer.ScoreBulk(freqs, norms, got)

				for i := 0; i < n; i++ {
					var tf float64
					if freqs[i] < MaxSqrtCache {
						tf = SqrtCache[int(freqs[i])]
					} else {
						tf = math.Sqrt(float64(freqs[i]))
					}
					score, _ := scorer.docScore(tf, norms[i])
					want := score * scorer.queryWeight
					if got[i] != want {
						t.Fatalf("n=%d i=%d: ScoreBulk %v != docScore*queryWeight %v (freq=%d norm=%v)",
							n, i, got[i], want, freqs[i], norms[i])
					}
				}
			}
		})
	}
}
