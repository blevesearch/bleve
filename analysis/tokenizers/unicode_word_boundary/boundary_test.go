//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package unicode_word_boundary

import (
	"reflect"
	"testing"

	"github.com/couchbaselabs/bleve/analysis"
)

func TestBoundary(t *testing.T) {

	tests := []struct {
		input  []byte
		locale string
		output analysis.TokenStream
	}{
		{
			[]byte("Hello World"),
			"en_US",
			analysis.TokenStream{
				{
					0,
					5,
					[]byte("Hello"),
					1,
				},
				{
					6,
					11,
					[]byte("World"),
					2,
				},
			},
		},
		{
			[]byte("こんにちは世界"),
			"en_US",
			analysis.TokenStream{
				{
					0,
					15,
					[]byte("こんにちは"),
					1,
				},
				{
					15,
					21,
					[]byte("世界"),
					2,
				},
			},
		},
		{
			[]byte("แยกคำภาษาไทยก็ทำได้นะจ้ะ"),
			"th_TH",
			analysis.TokenStream{
				{
					0,
					9,
					[]byte("แยก"),
					1,
				},
				{
					9,
					15,
					[]byte("คำ"),
					2,
				},
				{
					15,
					27,
					[]byte("ภาษา"),
					3,
				},
				{
					27,
					36,
					[]byte("ไทย"),
					4,
				},
				{
					36,
					42,
					[]byte("ก็"),
					5,
				},
				{
					42,
					57,
					[]byte("ทำได้"),
					6,
				},
				{
					57,
					63,
					[]byte("นะ"),
					7,
				},
				{
					63,
					72,
					[]byte("จ้ะ"),
					8,
				},
			},
		},
	}

	for _, test := range tests {
		tokenizer := NewUnicodeWordBoundaryCustomLocaleTokenizer(test.locale)
		actual := tokenizer.Tokenize(test.input)

		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("Expected %v, got %v for %s", test.output, actual, string(test.input))
		}
	}
}
