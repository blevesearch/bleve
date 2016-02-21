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
)

func TestKagome(t *testing.T) {

	tests := []struct {
		input  []byte
		output analysis.TokenStream
	}{
		{
			[]byte("こんにちは世界"),
			analysis.TokenStream{
				{
					Start:    0,
					End:      15,
					Term:     []byte("こんにちは"),
					Position: 1,
					Type:     analysis.Ideographic,
				},
				{
					Start:    15,
					End:      21,
					Term:     []byte("世界"),
					Position: 2,
					Type:     analysis.Ideographic,
				},
			},
		},
		{
			[]byte("関西国際空港"),
			analysis.TokenStream{
				{
					Start:    0,
					End:      6,
					Term:     []byte("関西"),
					Position: 1,
					Type:     analysis.Ideographic,
				},
				{
					Start:    6,
					End:      12,
					Term:     []byte("国際"),
					Position: 2,
					Type:     analysis.Ideographic,
				},
				{
					Start:    12,
					End:      18,
					Term:     []byte("空港"),
					Position: 3,
					Type:     analysis.Ideographic,
				},
			},
		},
	}

	tokenizer := NewKagomeMorphTokenizer()
	for _, test := range tests {
		actuals := tokenizer.Tokenize(test.input)

		if !reflect.DeepEqual(actuals, test.output) {
			t.Errorf("Expected %v, got %v for %s", test.output, actuals, string(test.input))
		}
	}
}
