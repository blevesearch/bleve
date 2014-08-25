//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build icu full

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
					Start:    0,
					End:      5,
					Term:     []byte("Hello"),
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    6,
					End:      11,
					Term:     []byte("World"),
					Position: 2,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
		{
			[]byte("steven's"),
			"en_US",
			analysis.TokenStream{
				{
					Start:    0,
					End:      8,
					Term:     []byte("steven's"),
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
		{
			[]byte("こんにちは世界"),
			"en_US",
			analysis.TokenStream{
				{
					Start:    0,
					End:      15,
					Term:     []byte("こんにちは"),
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    15,
					End:      21,
					Term:     []byte("世界"),
					Position: 2,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
		{
			[]byte("แยกคำภาษาไทยก็ทำได้นะจ้ะ"),
			"th_TH",
			analysis.TokenStream{
				{
					Start:    0,
					End:      9,
					Term:     []byte("แยก"),
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    9,
					End:      15,
					Term:     []byte("คำ"),
					Position: 2,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    15,
					End:      27,
					Term:     []byte("ภาษา"),
					Position: 3,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    27,
					End:      36,
					Term:     []byte("ไทย"),
					Position: 4,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    36,
					End:      42,
					Term:     []byte("ก็"),
					Position: 5,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    42,
					End:      57,
					Term:     []byte("ทำได้"),
					Position: 6,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    57,
					End:      63,
					Term:     []byte("นะ"),
					Position: 7,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    63,
					End:      72,
					Term:     []byte("จ้ะ"),
					Position: 8,
					Type:     analysis.AlphaNumeric,
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
