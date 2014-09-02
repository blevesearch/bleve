//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package analysis

import (
	"reflect"
	"testing"
)

func TestDeleteRune(t *testing.T) {
	tests := []struct {
		in     []rune
		delPos int
		out    []rune
	}{
		{
			in:     []rune{'a', 'b', 'c'},
			delPos: 1,
			out:    []rune{'a', 'c'},
		},
	}

	for _, test := range tests {
		actual := DeleteRune(test.in, test.delPos)
		if !reflect.DeepEqual(actual, test.out) {
			t.Errorf("expected %#v, got %#v", test.out, actual)
		}
	}
}

func TestInsertRune(t *testing.T) {
	tests := []struct {
		in      []rune
		insPos  int
		insRune rune
		out     []rune
	}{
		{
			in:      []rune{'a', 'b', 'c'},
			insPos:  1,
			insRune: 'x',
			out:     []rune{'a', 'x', 'b', 'c'},
		},
		{
			in:      []rune{'a', 'b', 'c'},
			insPos:  0,
			insRune: 'x',
			out:     []rune{'x', 'a', 'b', 'c'},
		},
		{
			in:      []rune{'a', 'b', 'c'},
			insPos:  3,
			insRune: 'x',
			out:     []rune{'a', 'b', 'c', 'x'},
		},
	}

	for _, test := range tests {
		actual := InsertRune(test.in, test.insPos, test.insRune)
		if !reflect.DeepEqual(actual, test.out) {
			t.Errorf("expected %#v, got %#v", test.out, actual)
		}
	}
}
