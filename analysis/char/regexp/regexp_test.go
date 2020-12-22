//  Copyright (c) 2014 Couchbase, Inc.
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

package regexp

import (
	"fmt"
	"reflect"
	"regexp"
	"testing"
)

func TestRegexpCharFilter(t *testing.T) {

	tests := []struct {
		regexStr string
		replace  []byte
		input    []byte
		output   []byte
	}{
		{
			regexStr: `</?[!\w]+((\s+\w+(\s*=\s*(?:".*?"|'.*?'|[^'">\s]+))?)+\s*|\s*)/?>`,
			replace:  []byte{' '},
			input:    []byte(`<html>test</html>`),
			output:   []byte(` test `),
		},
		{
			regexStr: `\x{200C}`,
			replace:  []byte{' '},
			input:    []byte("water\u200Cunder\u200Cthe\u200Cbridge"),
			output:   []byte("water under the bridge"),
		},
		{
			regexStr: `([a-z])\s+(\d)`,
			replace:  []byte(`$1-$2`),
			input:    []byte(`temp 1`),
			output:   []byte(`temp-1`),
		},
		{
			regexStr: `foo.?`,
			replace:  []byte(`X`),
			input:    []byte(`seafood, fool`),
			output:   []byte(`seaX, X`),
		},
		{
			regexStr: `def`,
			replace:  []byte(`_`),
			input:    []byte(`abcdefghi`),
			output:   []byte(`abc_ghi`),
		},
		{
			regexStr: `456`,
			replace:  []byte(`000000`),
			input:    []byte(`123456789`),
			output:   []byte(`123000000789`),
		},
		{
			regexStr: `“|”`,
			replace:  []byte(`"`),
			input:    []byte(`“hello”`),
			output:   []byte(`"hello"`),
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("match %s replace %s", test.regexStr, string(test.replace)), func(t *testing.T) {
			regex := regexp.MustCompile(test.regexStr)
			filter := New(regex, test.replace)

			output := filter.Filter(test.input)
			if !reflect.DeepEqual(test.output, output) {
				t.Errorf("Expected: `%s`, Got: `%s`\n", string(test.output), string(output))
			}
		})

	}
}
