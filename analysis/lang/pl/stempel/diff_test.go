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
	"reflect"
	"testing"
)

func TestDiff(t *testing.T) {
	tests := []struct {
		in  []rune
		cmd []rune
		out []rune
	}{
		// test delete, this command deletes N chars backwards from the current pos
		// the current pos starts at the end of the string
		// if you try to delete a negative number of chars or more chars than there
		// are, you will get the buffer at that time
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  delete 1
			cmd: []rune{'D', 'a'},
			out: []rune{'h', 'e', 'l', 'l'},
		},
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  delete 2
			cmd: []rune{'D', 'a' + 1},
			out: []rune{'h', 'e', 'l'},
		},
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  delete 3
			cmd: []rune{'D', 'a' + 2},
			out: []rune{'h', 'e'},
		},
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  delete 4
			cmd: []rune{'D', 'a' + 3},
			out: []rune{'h'},
		},
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  delete 5
			cmd: []rune{'D', 'a' + 4},
			out: []rune{},
		},
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  delete 6 (invalid, return buffer at that point)
			cmd: []rune{'D', 'a' + 5},
			out: []rune{'h', 'e', 'l', 'l', 'o'},
		},
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  delete -1
			cmd: []rune{'D', 'a' - 1},
			out: []rune{'h', 'e', 'l', 'l', 'o'},
		},
		// delete one char twice
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  delete 1, delete 1
			cmd: []rune{'D', 'a', 'D', 'a'},
			out: []rune{'h', 'e', 'l'},
		},
		// test insert
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  insert 'p'
			cmd: []rune{'I', 'p'},
			out: []rune{'h', 'e', 'l', 'l', 'o', 'p'},
		},
		// insert twice
		{
			in: []rune{'h'},
			//  insert 'l', insert 'e'
			// NOTE how the cursor moves backwards, so we have to insert in reverse
			cmd: []rune{'I', 'l', 'I', 'e'},
			out: []rune{'h', 'e', 'l'},
		},
		// test replace
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  replace with 'y'
			cmd: []rune{'R', 'y'},
			out: []rune{'h', 'e', 'l', 'l', 'y'},
		},
		// test replace again
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  replace with 'y', then replace with 'x'
			// NOTE how the cursor moves backwards as we replace
			cmd: []rune{'R', 'y', 'R', 'x'},
			out: []rune{'h', 'e', 'l', 'x', 'y'},
		},
		// test skip, then replace
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  skip 1, then replace with 'y'
			cmd: []rune{'-', 'a', 'R', 'y'},
			out: []rune{'h', 'e', 'l', 'y', 'o'},
		},
		// test skip 2, then replace
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  skip 1, then replace with 'y'
			cmd: []rune{'-', 'a' + 1, 'R', 'y'},
			out: []rune{'h', 'e', 'y', 'l', 'o'},
		},
		// test skip 2, then replace
		{
			in: []rune{'h', 'e', 'l', 'l', 'o'},
			//  skip 5 (too far), then replace with 'y'
			//  get original
			cmd: []rune{'-', 'a' + 4, 'R', 'y'},
			out: []rune{'h', 'e', 'l', 'l', 'o'},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s-'%s'", string(test.in), string(test.cmd)), func(t *testing.T) {
			got := Diff(test.in, test.cmd)
			if !reflect.DeepEqual(test.out, got) {
				t.Errorf("expected %v, got %v", test.out, got)
			}
		})
	}
}
