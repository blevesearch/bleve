//  Copyright (c) 2025 Couchbase, Inc.
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

package tr

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

func TestSnowballTurkishStemmer(t *testing.T) {
	tests := []struct {
		input  analysis.TokenStream
		output analysis.TokenStream
	}{
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("kimsesizler"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("kimsesiz"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("kitaplar"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("kitap"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("arabanın"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("araba"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("bardaklar"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("bardak"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("kediye"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("kedi"),
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("yazdım"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("yaz"),
				},
			},
		},
	}

	cache := registry.NewCache()
	filter, err := cache.TokenFilterNamed(SnowballStemmerName)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range tests {
		actual := filter.Filter(test.input)
		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("expected %s, got %s", test.output[0].Term, actual[0].Term)
		}
	}
}
