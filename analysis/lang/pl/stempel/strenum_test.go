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

package stempel

import (
	"fmt"
	"io"
	"reflect"
	"testing"
)

func TestStrenumNext(t *testing.T) {

	tests := []struct {
		in     []rune
		up     bool
		expect []rune
	}{
		{
			in:     []rune{'h', 'e', 'l', 'l', 'o'},
			up:     true,
			expect: []rune{'h', 'e', 'l', 'l', 'o'},
		},
		{
			in:     []rune{'h', 'e', 'l', 'l', 'o'},
			up:     false,
			expect: []rune{'o', 'l', 'l', 'e', 'h'},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s-up-%t", string(test.in), test.up), func(t *testing.T) {
			strenum := newStrEnum(test.in, test.up)
			var got []rune
			next, err := strenum.next()
			for err == nil {
				got = append(got, next)
				next, err = strenum.next()
			}
			if err != io.EOF {
				t.Errorf("next got err: %v", err)
			}
			if !reflect.DeepEqual(got, test.expect) {
				t.Errorf("expected %v, got %v", test.expect, got)
			}
		})
	}

}
