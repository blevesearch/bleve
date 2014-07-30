//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import (
	"testing"
)

func TestSimpleFragmentScorer(t *testing.T) {

	tests := []struct {
		fragment *Fragment
		tlm      TermLocationMap
		score    float64
	}{
		{
			fragment: &Fragment{
				orig:  []byte("cat in the hat"),
				start: 0,
				end:   14,
			},
			tlm: TermLocationMap{
				"cat": Locations{
					&Location{
						Pos:   0,
						Start: 0,
						End:   3,
					},
				},
			},
			score: 1,
		},
		{
			fragment: &Fragment{
				orig:  []byte("cat in the hat"),
				start: 0,
				end:   14,
			},
			tlm: TermLocationMap{
				"cat": Locations{
					&Location{
						Pos:   1,
						Start: 0,
						End:   3,
					},
				},
				"hat": Locations{
					&Location{
						Pos:   4,
						Start: 11,
						End:   14,
					},
				},
			},
			score: 2,
		},
	}

	for _, test := range tests {
		scorer := NewSimpleFragmentScorer(test.tlm)
		scorer.Score(test.fragment)
		if test.fragment.score != test.score {
			t.Errorf("expected score %f, got %f", test.score, test.fragment.score)
		}
	}

}
