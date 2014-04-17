//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package single_token

import (
	"reflect"
	"testing"

	"github.com/couchbaselabs/bleve/analysis"
)

func TestSingleTokenTokenizer(t *testing.T) {

	tests := []struct {
		input  []byte
		output analysis.TokenStream
	}{
		{
			[]byte("Hello World"),
			analysis.TokenStream{
				{
					0,
					11,
					[]byte("Hello World"),
					1,
				},
			},
		},
		{
			[]byte("こんにちは世界"),
			analysis.TokenStream{
				{
					0,
					21,
					[]byte("こんにちは世界"),
					1,
				},
			},
		},
		{
			[]byte("แยกคำภาษาไทยก็ทำได้นะจ้ะ"),
			analysis.TokenStream{
				{
					0,
					72,
					[]byte("แยกคำภาษาไทยก็ทำได้นะจ้ะ"),
					1,
				},
			},
		},
	}

	for _, test := range tests {
		tokenizer := NewSingleTokenTokenizer()
		actual := tokenizer.Tokenize(test.input)

		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("Expected %v, got %v for %s", test.output, actual, string(test.input))
		}
	}
}
