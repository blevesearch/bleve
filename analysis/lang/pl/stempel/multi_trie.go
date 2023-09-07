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

	"github.com/blevesearch/stempel/javadata"
)

// multiTrie represents a trie of tries.  When using the multiTrie, each trie
// is consulted consecutively to find commands to perform on the input.  Thus
// a multiTrie with seven tries might have up to seven groups of commands to
// perform on the input.
type multiTrie struct {
	tries   []*trie
	by      int32
	forward bool
}

func newMultiTrie(r *javadata.Reader) (rv *multiTrie, err error) {
	rv = &multiTrie{}
	rv.forward, err = r.ReadBool()
	if err != nil {
		return nil, err
	}
	rv.by, err = r.ReadInt32()
	if err != nil {
		return nil, err
	}
	nTries, err := r.ReadInt32()
	if err != nil {
		return nil, err
	}
	for nTries > 0 {
		trie, err := newTrie(r)
		if err != nil {
			return nil, err
		}
		rv.tries = append(rv.tries, trie)
		nTries--
	}
	return rv, nil
}

const eom = rune('*')

func (t *multiTrie) GetLastOnPath(key []rune) []rune {
	var rv []rune
	lastKey := key
	p := make([][]rune, len(t.tries))
	lastR := ' '
	for i := 0; i < len(t.tries); i++ {
		r := t.tries[i].GetLastOnPath(lastKey)
		if len(r) == 0 || len(r) == 1 && r[0] == eom {
			return rv
		}
		if cannotFollow(lastR, r[0]) {
			return rv
		}
		lastR = r[len(r)-2]
		p[i] = r
		if p[i][0] == '-' {
			if i > 0 {
				var err error
				key, err = t.skip(key, lengthPP(p[i-1]))
				if err != nil {
					return rv
				}
			}
			var err error
			key, err = t.skip(key, lengthPP(p[i]))
			if err != nil {
				return rv
			}
		}
		rv = append(rv, r...)
		if len(key) != 0 {
			lastKey = key
		}
	}
	return rv
}

func cannotFollow(after, goes rune) bool {
	switch after {
	case '-', 'D':
		return after == goes
	}
	return false
}

var errIndexOutOfBounds = fmt.Errorf("index out of bounds")

func (t *multiTrie) skip(in []rune, count int) ([]rune, error) {
	if count > len(in) {
		return nil, errIndexOutOfBounds
	}
	if t.forward {
		return in[count:], nil
	}
	return in[0 : len(in)-count], nil
}

func lengthPP(cmd []rune) int {
	rv := 0
	for i := 0; i < len(cmd); i++ {
		switch cmd[i] {
		case '-', 'D':
			i++
			rv += int(cmd[i] - rune('a') + 1)
		case 'R':
			i++
			rv++
			fallthrough
		case 'I':
		}
	}
	return rv
}

func (t *multiTrie) String() string {
	rv := ""
	for i, trie := range t.tries {
		rv += fmt.Sprintf("trie %d\n\n %v\n--------\n", i, trie)
	}
	return rv
}
