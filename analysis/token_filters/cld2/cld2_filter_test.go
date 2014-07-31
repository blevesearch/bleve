//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package cld2

import (
	"reflect"
	"testing"

	"github.com/couchbaselabs/bleve/analysis"
)

func TestCld2Filter(t *testing.T) {
	tests := []struct {
		input  analysis.TokenStream
		output analysis.TokenStream
	}{
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("the quick brown fox"),
					Start:    0,
					End:      19,
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("en"),
					Start:    0,
					End:      2,
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("こんにちは世界"),
					Start:    0,
					End:      21,
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("ja"),
					Start:    0,
					End:      2,
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("แยกคำภาษาไทยก็ทำได้นะจ้ะ"),
					Start:    0,
					End:      72,
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("th"),
					Start:    0,
					End:      2,
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
		{
			input: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("مرحبا، العالم!"),
					Start:    0,
					End:      26,
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("ar"),
					Start:    0,
					End:      2,
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
	}

	filter := NewCld2Filter()
	for _, test := range tests {
		res := filter.Filter(test.input)
		if !reflect.DeepEqual(res, test.output) {
			t.Errorf("expected:")
			for _, token := range test.output {
				t.Errorf("%#v - %s", token, token.Term)
			}
			t.Errorf("got:")
			for _, token := range res {
				t.Errorf("%#v - %s", token, token.Term)
			}
		}
	}

}
