//  Copyright (c) 2019 Couchbase, Inc.
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

package lt

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

func TestLithuanianAnalyzer(t *testing.T) {
	tests := []struct {
		input  []byte
		output analysis.TokenStream
	}{
		// stemming
		{
			input: []byte("kavytė"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("kav"),
				},
			},
		},
		{
			input: []byte("kavinukas"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("kavinuk"),
				},
			},
		},
		// stop word
		{
			input:  []byte("į"),
			output: analysis.TokenStream{},
		},
		// digits safe
		{
			input: []byte("Šeši nuliai - 1000000"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("šeš"),
				},
				&analysis.Token{
					Term: []byte("nul"),
				},
				&analysis.Token{
					Term: []byte("1000000"),
				},
			},
		},
		{
			input: []byte("Tiek savaitgalį, tiek per šventes laukia rudeniški orai: sniego tikėtis neverta"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("savaitgal"),
				},
				&analysis.Token{
					Term: []byte("švent"),
				},
				&analysis.Token{
					Term: []byte("lauk"),
				},
				&analysis.Token{
					Term: []byte("rudeniš"),
				},
				&analysis.Token{
					Term: []byte("or"),
				},
				&analysis.Token{
					Term: []byte("snieg"),
				},
				&analysis.Token{
					Term: []byte("tik"),
				},
				&analysis.Token{
					Term: []byte("nevert"),
				},
			},
		},
		{
			input: []byte("Visą savaitę prognozuojami klastingi reiškiniai"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("vis"),
				},
				&analysis.Token{
					Term: []byte("savait"),
				},
				&analysis.Token{
					// verb. "prognozuo-ti"
					Term: []byte("prognozuo"),
				},
				&analysis.Token{
					Term: []byte("klast"),
				},
				&analysis.Token{
					Term: []byte("reiškin"),
				},
			},
		},
		{
			input: []byte("Susirgęs Arūnas gyvenimui pasirinko šalį, kurioje įteisinta eutanazija: silpsta visos jo organizmo funkcijos"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("susirg"),
				},
				&analysis.Token{
					Term: []byte("arūn"),
				},
				&analysis.Token{
					Term: []byte("gyvenim"),
				},
				&analysis.Token{
					Term: []byte("pasirink"),
				},
				&analysis.Token{
					Term: []byte("šal"),
				},
				&analysis.Token{
					Term: []byte("kur"),
				},
				&analysis.Token{
					Term: []byte("įteis"),
				},
				&analysis.Token{
					Term: []byte("eutanazij"),
				},
				&analysis.Token{
					Term: []byte("silpst"),
				},
				&analysis.Token{
					Term: []byte("vis"),
				},
				&analysis.Token{
					Term: []byte("organizm"),
				},
				&analysis.Token{
					Term: []byte("funkcij"),
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
