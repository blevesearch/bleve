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

package asciifolding

import (
	"fmt"
	"reflect"
	"testing"
)

func TestAsciiFoldingFilter(t *testing.T) {

	tests := []struct {
		input  []byte
		output []byte
	}{
		{
			// empty input passes
			input:  []byte(``),
			output: []byte(``),
		},
		{
			// no modification for plain ASCII
			input:  []byte(`The quick brown fox jumps over the lazy dog`),
			output: []byte(`The quick brown fox jumps over the lazy dog`),
		},
		{
			// Umlauts are folded to plain ASCII
			input:  []byte(`The quick bröwn fox jümps over the läzy dog`),
			output: []byte(`The quick brown fox jumps over the lazy dog`),
		}, {
			// composite unicode runes are folded to more than one ASCII rune
			input:  []byte(`ÆꜴ`),
			output: []byte(`AEAO`),
		}, {
			// apples from https://issues.couchbase.com/browse/MB-33486
			input:  []byte(`Ápple Àpple Äpple Âpple Ãpple Åpple`),
			output: []byte(`Apple Apple Apple Apple Apple Apple`),
		}, {
			// Fix ASCII folding of \u24A2
			input:  []byte(`⒢`),
			output: []byte(`(g)`),
		},
	}

	for _, test := range tests {
		filter := New()
		t.Run(fmt.Sprintf("on %s", test.input), func(t *testing.T) {
			output := filter.Filter(test.input)
			if !reflect.DeepEqual(output, test.output) {
				t.Errorf("\nExpected:\n`%s`\ngot:\n`%s`\n", string(test.output), string(output))
			}
		})
	}
}
