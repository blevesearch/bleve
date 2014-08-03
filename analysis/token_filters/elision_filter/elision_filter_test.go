//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package elision_filter

import (
	"reflect"
	"testing"

	"github.com/couchbaselabs/bleve/analysis"
)

func TestElisionFilter(t *testing.T) {

	frenchArticlesMap := analysis.NewWordMap()
	err := frenchArticlesMap.LoadBytes(FrenchArticles)
	if err != nil {
		t.Fatal(err)
	}

	italianArticlesMap := analysis.NewWordMap()
	err = italianArticlesMap.LoadBytes(ItalianArticles)
	if err != nil {
		t.Fatal(err)
	}

	catalanArticlesMap := analysis.NewWordMap()
	err = catalanArticlesMap.LoadBytes(CatalanArticles)
	if err != nil {
		t.Fatal(err)
	}

	irishArticlesMap := analysis.NewWordMap()
	err = irishArticlesMap.LoadBytes(IrishArticles)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		articleMap analysis.WordMap
		input      analysis.TokenStream
		output     analysis.TokenStream
	}{
		{
			articleMap: frenchArticlesMap,
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("l'avion"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("avion"),
				},
			},
		},
		{
			articleMap: italianArticlesMap,
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("dell'Italia"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Italia"),
				},
			},
		},
		{
			articleMap: catalanArticlesMap,
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("l'Institut"),
				},
				&analysis.Token{
					Term: []byte("d'Estudis"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("Institut"),
				},
				&analysis.Token{
					Term: []byte("Estudis"),
				},
			},
		},
		{
			articleMap: irishArticlesMap,
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("b'fhearr"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("fhearr"),
				},
			},
		},
	}

	for _, test := range tests {
		elisionFilter := NewElisionFilter(test.articleMap)
		actual := elisionFilter.Filter(test.input)
		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("expected %s, got %s", test.output[0].Term, actual[0].Term)
		}
	}
}
