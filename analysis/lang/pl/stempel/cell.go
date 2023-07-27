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

type cell struct {
	ref int32
	cmd int32
}

func (c *cell) String() string {
	return fmt.Sprintf("ref(%d) cmd(%d)", c.ref, c.cmd)
}

func newCell(r *javadata.Reader) (*cell, error) {
	cmd, err := r.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("error reading cell cmd: %v", err)
	}
	_, err = r.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("error reading cell cnt: %v", err)
	}
	ref, err := r.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("error reading cell ref: %v", err)
	}
	_, err = r.ReadInt32()
	if err != nil {
		return nil, fmt.Errorf("error reading cell skip: %v", err)
	}
	return &cell{
		cmd: cmd,
		ref: ref,
	}, nil
}
