//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package scorers

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
)

func TestConstantScorer(t *testing.T) {

	scorer := NewConstantScorer(1, 1, true)

	tests := []struct {
		termMatch *index.TermFieldDoc
		result    *search.DocumentMatch
	}{
		// test some simple math
		{
			termMatch: &index.TermFieldDoc{
				ID:   []byte("one"),
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
				ID:    "one",
				Score: 1.0,
				Expl: &search.Explanation{
					Value:   1.0,
					Message: "ConstantScore()",
				},
			},
		},
	}

	for _, test := range tests {
		actual := scorer.Score(string(test.termMatch.ID))

		if !reflect.DeepEqual(actual, test.result) {
			t.Errorf("expected %#v got %#v for %#v", test.result, actual, test.termMatch)
		}
	}

}

func TestConstantScorerWithQueryNorm(t *testing.T) {

	scorer := NewConstantScorer(1, 1, true)
	scorer.SetQueryNorm(2.0)

	tests := []struct {
		termMatch *index.TermFieldDoc
		result    *search.DocumentMatch
	}{
		{
			termMatch: &index.TermFieldDoc{
				ID:   []byte("one"),
				Freq: 1,
				Norm: 1.0,
			},
			result: &search.DocumentMatch{
				ID:    "one",
				Score: 2.0,
				Expl: &search.Explanation{
					Value:   2.0,
					Message: "weight(^1.000000), product of:",
					Children: []*search.Explanation{
						{
							Value:   2.0,
							Message: "ConstantScore()^1.000000, product of:",
							Children: []*search.Explanation{
								{
									Value:   1,
									Message: "boost",
								},
								{
									Value:   2,
									Message: "queryNorm",
								},
							},
						},
						{
							Value:   1.0,
							Message: "ConstantScore()",
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		actual := scorer.Score(string(test.termMatch.ID))

		if !reflect.DeepEqual(actual, test.result) {
			t.Errorf("expected %#v got %#v for %#v", test.result, actual, test.termMatch)
		}
	}

}
