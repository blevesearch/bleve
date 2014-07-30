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

func TestHTMLFragmentFormatterDefault(t *testing.T) {
	tests := []struct {
		fragment *Fragment
		tlm      TermLocationMap
		output   string
	}{
		{
			fragment: &Fragment{
				orig:  []byte("the quick brown fox"),
				start: 0,
				end:   19,
			},
			tlm: TermLocationMap{
				"quick": Locations{
					&Location{
						Pos:   2,
						Start: 4,
						End:   9,
					},
				},
			},
			output: "the <b>quick</b> brown fox",
		},
	}

	emHtmlFormatter := NewHTMLFragmentFormatter()
	for _, test := range tests {
		result := emHtmlFormatter.Format(test.fragment, test.tlm)
		if result != test.output {
			t.Errorf("expected `%s`, got `%s`", test.output, result)
		}
	}
}

func TestHTMLFragmentFormatterCustom(t *testing.T) {
	tests := []struct {
		fragment *Fragment
		tlm      TermLocationMap
		output   string
	}{
		{
			fragment: &Fragment{
				orig:  []byte("the quick brown fox"),
				start: 0,
				end:   19,
			},
			tlm: TermLocationMap{
				"quick": Locations{
					&Location{
						Pos:   2,
						Start: 4,
						End:   9,
					},
				},
			},
			output: "the <em>quick</em> brown fox",
		},
	}

	emHtmlFormatter := NewHTMLFragmentFormatterCustom("<em>", "</em>")
	for _, test := range tests {
		result := emHtmlFormatter.Format(test.fragment, test.tlm)
		if result != test.output {
			t.Errorf("expected `%s`, got `%s`", test.output, result)
		}
	}
}
