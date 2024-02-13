//  Copyright (c) 2017 Couchbase, Inc.
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

package es

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/analysis"
)

func TestSpanishNormalizeFilter(t *testing.T) {
	tests := []struct {
		input  analysis.TokenStream
		output analysis.TokenStream
	}{
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Guía"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Guia"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Belcebú"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Belcebu"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Limón"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Limon"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("agüero"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("aguero"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("laúd"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("laud"),
				},
			},
		},
		// empty
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
	}

	spanishNormalizeFilter := NewSpanishNormalizeFilter()
	for _, test := range tests {
		actual := spanishNormalizeFilter.Filter(test.input)
		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("expected %#v, got %#v", test.output, actual)
			t.Errorf("expected %s(% x), got %s(% x)", test.output[0].Term, test.output[0].Term, actual[0].Term, actual[0].Term)
		}
	}
}
