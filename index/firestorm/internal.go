//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package firestorm

var InternalKeyPrefix = []byte{'d'}

type InternalRow struct {
	key []byte
	val []byte
}

func NewInternalRow(key, val []byte) *InternalRow {
	rv := InternalRow{
		key: key,
		val: val,
	}
	return &rv
}

func NewInternalRowKV(key, value []byte) (*InternalRow, error) {
	rv := InternalRow{}
	rv.key = key[1:]
	rv.val = value
	return &rv, nil
}

func (ir *InternalRow) Key() []byte {
	buf := make([]byte, len(ir.key)+1)
	buf[0] = 'i'
	copy(buf[1:], ir.key)
	return buf
}

func (ir *InternalRow) Value() []byte {
	return ir.val
}
