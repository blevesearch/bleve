//  Copyright (c) 2018 Couchbase, Inc.
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

package pl

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

func TestPolishAnalyzer(t *testing.T) {
	tests := []struct {
		input  []byte
		output analysis.TokenStream
	}{
		// stemming
		{
			input: []byte("śmiało"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("śmieć"),
				},
			},
		},
		{
			input: []byte("przypadku"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("przypadek"),
				},
			},
		},
		// stop word
		{
			input:  []byte("według"),
			output: analysis.TokenStream{},
		},
		// digits safe
		{
			input: []byte("text 1000"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("text"),
				},
				&analysis.Token{
					Term: []byte("1000"),
				},
			},
		},
		{
			input: []byte("badawczego było opracowanie kompendium które przystępny sposób prezentowało niespecjalistom zakresu kryptografii kwantowej wykorzystanie technik kwantowych do bezpiecznego przesyłu przetwarzania informacji"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("badawczy"),
				},
				&analysis.Token{
					Term: []byte("opracować"),
				},
				&analysis.Token{
					Term: []byte("kompendium"),
				},
				&analysis.Token{
					Term: []byte("przystyć"),
				},
				&analysis.Token{
					Term: []byte("prezentować"),
				},
				&analysis.Token{
					Term: []byte("niespecjalista"),
				},
				&analysis.Token{
					Term: []byte("zakres"),
				},
				&analysis.Token{
					Term: []byte("kryptografia"),
				},
				&analysis.Token{
					Term: []byte("kwantowy"),
				},
				&analysis.Token{
					Term: []byte("wykorzyseć"),
				},
				&analysis.Token{
					Term: []byte("technika"),
				},
				&analysis.Token{
					Term: []byte("kwantowy"),
				},
				&analysis.Token{
					Term: []byte("bezpieczny"),
				},
				&analysis.Token{
					Term: []byte("przesył"),
				},
				&analysis.Token{
					Term: []byte("przetwarzać"),
				},
				&analysis.Token{
					Term: []byte("informacja"),
				},
			},
		},
		{
			input: []byte("Ale ta wiedza była utrzymywana w tajemnicy"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("wiedza"),
				},
				&analysis.Token{
					Term: []byte("utrzymywać"),
				},
				&analysis.Token{
					Term: []byte("tajemnik"),
				},
			},
		},
	}

	cache := registry.NewCache()
	analyzer, err := cache.AnalyzerNamed(AnalyzerName)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		actual := analyzer.Analyze(test.input)
		if len(actual) != len(test.output) {
			t.Fatalf("expected length: %d, got %d", len(test.output), len(actual))
		}
		for i, tok := range actual {
			if !reflect.DeepEqual(tok.Term, test.output[i].Term) {
				t.Errorf("expected term %s (% x) got %s (% x)", test.output[i].Term, test.output[i].Term, tok.Term, tok.Term)
			}
		}
	}
}
