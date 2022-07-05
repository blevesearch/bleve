//  Copyright (c) 2014 Couchbase, Inc.
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

package html

import (
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/highlight"
)

func TestHTMLFragmentFormatter(t *testing.T) {
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
		// test html escaping
		{
			fragment: &highlight.Fragment{
				Orig:  []byte("<the> quick brown & fox"),
				Start: 0,
				End:   23,
			},
			tlm: search.TermLocationMap{
				"quick": []*search.Location{
					{
						Pos:   2,
						Start: 6,
						End:   11,
					},
				},
			},
			output: "&lt;the&gt; <em>quick</em> brown &amp; fox",
			start:  "<em>",
			end:    "</em>",
		},
		// test html escaping inside search term
		{
			fragment: &highlight.Fragment{
				Orig:  []byte("<the> qu&ick brown & fox"),
				Start: 0,
				End:   24,
			},
			tlm: search.TermLocationMap{
				"qu&ick": []*search.Location{
					{
						Pos:   2,
						Start: 6,
						End:   12,
					},
				},
			},
			output: "&lt;the&gt; <em>qu&amp;ick</em> brown &amp; fox",
			start:  "<em>",
			end:    "</em>",
		},
	}

	for _, test := range tests {
		emHTMLFormatter := NewFragmentFormatter(test.start, test.end)
		otl := highlight.OrderTermLocations(test.tlm)
		result := emHTMLFormatter.Format(test.fragment, otl)
		if result != test.output {
			t.Errorf("expected `%s`, got `%s`", test.output, result)
		}
	}
}
