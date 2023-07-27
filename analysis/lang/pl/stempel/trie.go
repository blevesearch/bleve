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

// trie represents the internal trie structure
type trie struct {
	rows    []*row
	cmds    []string
	root    int32
	forward bool
}

func newTrie(r *javadata.Reader) (rv *trie, err error) {
	rv = &trie{}
	rv.forward, err = r.ReadBool()
	if err != nil {
		return nil, fmt.Errorf("error reading trie forward: %v", err)
	}
	rv.root, err = r.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("error reading trie root: %v", err)
	}

	// commands
	nCommands, err := r.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("error reading trie num commands: %v", err)
	}
	for nCommands > 0 {
		utfCommand, nerr := r.ReadUTF()
		if nerr != nil {
			return nil, fmt.Errorf("error reading trie command utf: %v", nerr)
		}
		rv.cmds = append(rv.cmds, utfCommand)
		nCommands--
	}

	// rows
	nRows, err := r.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("error reading trie num rows: %v", err)
	}
	for nRows > 0 {
		row, err := newRow(r)
		if err != nil {
			return nil, fmt.Errorf("error reading trie row: %v", err)
		}
		rv.rows = append(rv.rows, row)
		nRows--
	}

	return rv, nil
}

func (t *trie) getRow(i int) *row {
	if i < 0 || i >= len(t.rows) {
		return nil
	}
	return t.rows[i]
}

func (t *trie) GetLastOnPath(key []rune) []rune {
	now := t.getRow(int(t.root))
	var last []rune
	var w int32
	e := newStrEnum(key, t.forward)

	// walk over each rune
	// if rune has row in the table, note the cmd (as last)
	// if rune has row in table, see if it transitions to another row
	// if it does, move to that row and next char on next loop itr
	// if it does not, return the last cmd
	// if you get to end of string and there is command in row use it
	// or return last
	for i := 0; i < len(key)-1; i++ {
		r, err := e.next()
		if err != nil {
			return last
		}
		w = now.getCmd(r)
		if w >= 0 {
			last = []rune(t.cmds[w])
		}
		w = now.getRef(r)
		if w >= 0 {
			now = t.getRow(int(w))
		} else {
			return last
		}
	}
	r, err := e.next()
	if err != nil {
		return last
	}
	w = now.getCmd(r)
	if err != nil {
		return last
	}
	if w >= 0 {
		return []rune(t.cmds[w])
	}
	return last
}

func (t *trie) String() string {
	rv := ""
	for _, cmd := range t.cmds {
		rv += fmt.Sprintf("cmd: %s\n", string(cmd))
	}
	for _, row := range t.rows {
		rv += fmt.Sprintf("row: %v\n", row)
	}
	return rv
}
