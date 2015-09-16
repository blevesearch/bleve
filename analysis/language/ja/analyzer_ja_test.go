//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package ja

import (
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

func TestJaAnalyzer(t *testing.T) {
	tests := []struct {
		input  []byte
		output analysis.TokenStream
	}{
		{
			input: []byte("こんにちは世界"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("こんにちは"),
					Type:     analysis.Ideographic,
					Position: 1,
					Start:    0,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("世界"),
					Type:     analysis.Ideographic,
					Position: 2,
					Start:    15,
					End:      21,
				},
			},
		},
		{
			input: []byte("ｶﾀｶﾅ"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("カタカナ"),
					Type:     analysis.Ideographic,
					Position: 1,
					Start:    0,
					End:      12,
				},
			},
		},
	}

	cache := registry.NewCache()
	for _, test := range tests {
		analyzer, err := cache.AnalyzerNamed(AnalyzerName)
		if err != nil {
			t.Fatal(err)
		}
		actual := analyzer.Analyze(test.input)
		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("expected %v, got %v", test.output, actual)
		}
	}
}
