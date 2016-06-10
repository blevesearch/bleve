//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package camelcase_filter

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/analysis"
)

func TestCamelCaseFilter(t *testing.T) {

	tests := []struct {
		input  analysis.TokenStream
		output analysis.TokenStream
	}{
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte(""),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte(""),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("a"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("a"),
				},
			},
		},

		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("...aMACMac123macILoveGolang"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("..."),
				},
				&analysis.Token{
					Term: []byte("a"),
				},
				&analysis.Token{
					Term: []byte("MAC"),
				},
				&analysis.Token{
					Term: []byte("Mac"),
				},
				&analysis.Token{
					Term: []byte("123"),
				},
				&analysis.Token{
					Term: []byte("mac"),
				},
				&analysis.Token{
					Term: []byte("I"),
				},
				&analysis.Token{
					Term: []byte("Love"),
				},
				&analysis.Token{
					Term: []byte("Golang"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Lang"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Lang"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("GLang"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("G"),
				},
				&analysis.Token{
					Term: []byte("Lang"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("GOLang"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("GO"),
				},
				&analysis.Token{
					Term: []byte("Lang"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("GOOLang"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("GOO"),
				},
				&analysis.Token{
					Term: []byte("Lang"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("1234"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("1234"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("starbucks"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("starbucks"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Starbucks TVSamsungIsGREAT000"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Starbucks"),
				},
				&analysis.Token{
					Term: []byte(" "),
				},
				&analysis.Token{
					Term: []byte("TV"),
				},
				&analysis.Token{
					Term: []byte("Samsung"),
				},
				&analysis.Token{
					Term: []byte("Is"),
				},
				&analysis.Token{
					Term: []byte("GREAT"),
				},
				&analysis.Token{
					Term: []byte("000"),
				},
			},
		},
	}

	for _, test := range tests {
		ccFilter := NewCamelCaseFilter()
		actual := ccFilter.Filter(test.input)
		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("expected %s \n\n got %s", test.output, actual)
		}
	}
}
