//  Copyright (c) 2022 Couchbase, Inc.
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

package plain

import (
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/highlight"
)

func TestPlainFragmentFormatter(t *testing.T) {
	tests := []struct {
		fragment *highlight.Fragment
		tlm      search.TermLocationMap
		output   string
		start    string
		end      string
	}{
		{
			fragment: &highlight.Fragment{
				Orig:  []byte("the quick brown fox"),
				Start: 0,
				End:   19,
			},
			tlm: search.TermLocationMap{
				"quick": []*search.Location{
					{
						Pos:   2,
						Start: 4,
						End:   9,
					},
				},
			},
			output: "the <b>quick</b> brown fox",
			start:  "<b>",
			end:    "</b>",
		},
		{
			fragment: &highlight.Fragment{
				Orig:  []byte("the quick brown fox"),
				Start: 0,
				End:   19,
			},
			tlm: search.TermLocationMap{
				"quick": []*search.Location{
					{
						Pos:   2,
						Start: 4,
						End:   9,
					},
				},
			},
			output: "the <em>quick</em> brown fox",
			start:  "<em>",
			end:    "</em>",
		},
	}

	for _, test := range tests {
		plainFormatter := NewFragmentFormatter(test.start, test.end)
		otl := highlight.OrderTermLocations(test.tlm)
		result := plainFormatter.Format(test.fragment, otl)
		if result != test.output {
			t.Errorf("expected `%s`, got `%s`", test.output, result)
		}
	}
}
