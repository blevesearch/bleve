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

type row struct {
	cells map[rune]*cell
}

func (r *row) String() string {
	rv := ""
	for k, v := range r.cells {
		rv += fmt.Sprintf("[%s:%v]\n", string(k), v)
	}
	return rv
}

func newRow(r *javadata.Reader) (*row, error) {
	rv := &row{
		cells: make(map[rune]*cell),
	}

	nCells, err := r.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("error reading num cells: %v", err)
	}

	for nCells > 0 {

		c, err := r.ReadCharAsRune()
		if err != nil {
			return nil, fmt.Errorf("error reading cell char: %v", err)
		}
		cell, err := newCell(r)
		if err != nil {
			return nil, fmt.Errorf("error reading cell: %v", err)
		}

		rv.cells[c] = cell
		nCells--
	}
	return rv, nil
}

func (r *row) getCmd(way rune) int32 {
	c := r.at(way)
	if c != nil {
		return c.cmd
	}
	return -1
}

func (r *row) getRef(way rune) int32 {
	c := r.at(way)
	if c != nil {
		return c.ref
	}
	return -1
}

func (r *row) at(c rune) *cell {
	return r.cells[c]
}
