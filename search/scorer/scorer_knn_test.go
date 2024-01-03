//  Copyright (c) 2023 Couchbase, Inc.
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

//go:build vectors
// +build vectors

package scorer

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

func TestKNNScorerExplanation(t *testing.T) {
	var queryVector []float32
	// arbitrary vector of dims: 64
	for i := 0; i < 64; i++ {
		queryVector = append(queryVector, float32(i))
	}

	var resVector []float32
	// arbitrary res vector.
	for i := 0; i < 64; i++ {
		resVector = append(resVector, float32(i))
	}

	tests := []struct {
		vectorMatch *index.VectorDoc
		scorer      *KNNQueryScorer
		norm        float64
		result      *search.DocumentMatch
	}{
		{
			vectorMatch: &index.VectorDoc{
				ID:     index.IndexInternalID("one"),
				Score:  0.5,
				Vector: resVector,
			},
			norm: 1.0,
			scorer: NewKNNQueryScorer(queryVector, "desc", 1.0,
				search.SearcherOptions{Explain: true}, index.EuclideanDistance),
			// Specifically testing EuclideanDistance since that involves score inversion.
			result: &search.DocumentMatch{
				IndexInternalID: index.IndexInternalID("one"),
				Score:           0.5,
				Expl: &search.Explanation{
					Value:   1 / 0.5,
					Message: "fieldWeight(desc in doc one), score of:",
					Children: []*search.Explanation{
						{
							Value:   1 / 0.5,
							Message: "vector(field(desc:one) with similarity_metric(l2_norm)=2.000000e+00",
						},
					},
				},
			},
		},
		{
			vectorMatch: &index.VectorDoc{
				ID:    index.IndexInternalID("one"),
				Score: 0.0,
				// Result vector is an exact match of an existing vector.
				Vector: queryVector,
			},
			norm: 1.0,
			scorer: NewKNNQueryScorer(queryVector, "desc", 1.0,
				search.SearcherOptions{Explain: true}, index.EuclideanDistance),
			// Specifically testing EuclideanDistance with 0 score.
			result: &search.DocumentMatch{
				IndexInternalID: index.IndexInternalID("one"),
				Score:           0.0,
				Expl: &search.Explanation{
					Value:   maxKNNScore,
					Message: "fieldWeight(desc in doc one), score of:",
					Children: []*search.Explanation{
						{
							Value:   maxKNNScore,
							Message: "vector(field(desc:one) with similarity_metric(l2_norm)=1.797693e+308",
						},
					},
				},
			},
		},
		{
			vectorMatch: &index.VectorDoc{
				ID:     index.IndexInternalID("one"),
				Score:  0.5,
				Vector: resVector,
			},
			norm: 1.0,
			scorer: NewKNNQueryScorer(queryVector, "desc", 1.0,
				search.SearcherOptions{Explain: true}, index.CosineSimilarity),
			result: &search.DocumentMatch{
				IndexInternalID: index.IndexInternalID("one"),
				Score:           0.5,
				Expl: &search.Explanation{
					Value:   0.5,
					Message: "fieldWeight(desc in doc one), score of:",
					Children: []*search.Explanation{
						{
							Value:   0.5,
							Message: "vector(field(desc:one) with similarity_metric(dot_product)=5.000000e-01",
						},
					},
				},
			},
		},
		{
			vectorMatch: &index.VectorDoc{
				ID:     index.IndexInternalID("one"),
				Score:  0.25,
				Vector: resVector,
			},
			norm: 0.5,
			scorer: NewKNNQueryScorer(queryVector, "desc", 1.0,
				search.SearcherOptions{Explain: true}, index.CosineSimilarity),
			result: &search.DocumentMatch{
				IndexInternalID: index.IndexInternalID("one"),
				Score:           0.25,
				Expl: &search.Explanation{
					Value:   0.125,
					Message: "weight(desc:query Vector^1.000000 in one), product of:",
					Children: []*search.Explanation{
						{
							Value:   0.5,
							Message: "queryWeight(desc:query Vector^1.000000), product of:",
							Children: []*search.Explanation{
								{
									Value:   1,
									Message: "boost",
								},
								{
									Value:   0.5,
									Message: "queryNorm",
								},
							},
						},
						{
							Value:   0.25,
							Message: "fieldWeight(desc in doc one), score of:",
							Children: []*search.Explanation{
								{
									Value:   0.25,
									Message: "vector(field(desc:one) with similarity_metric(dot_product)=2.500000e-01",
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
		test.scorer.SetQueryNorm(test.norm)
		actual := test.scorer.Score(ctx, test.vectorMatch)
		actual.Complete(nil)

		if !reflect.DeepEqual(actual.Expl, test.result.Expl) {
			t.Errorf("expected %#v got %#v for %#v", test.result.Expl,
				actual.Expl, test.vectorMatch)
		}
	}
}
