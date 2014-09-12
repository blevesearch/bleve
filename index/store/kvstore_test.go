//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package store

import (
	"encoding/binary"
	"testing"
)

type addUint64Operator struct {
	offset uint64
}

func newAddUint64Operator(offset uint64) *addUint64Operator {
	return &addUint64Operator{offset: offset}
}

func (a *addUint64Operator) Merge(key, existing []byte) ([]byte, error) {
	var existingUint64 uint64
	if len(existing) > 0 {
		existingUint64, _ = binary.Uvarint(existing)
	}
	existingUint64 += a.offset
	result := make([]byte, 8)
	binary.PutUvarint(result, existingUint64)
	return result, nil
}

func TestAssociativeMerge(t *testing.T) {

	// simulate original lookup of value
	existingValue := make([]byte, 8)
	binary.PutUvarint(existingValue, 27)

	mergeChain := make(AssociativeMergeChain, 0)
	mergeChain = append(mergeChain, newAddUint64Operator(6))
	mergeChain = append(mergeChain, newAddUint64Operator(3))
	mergeChain = append(mergeChain, newAddUint64Operator(25))
	mergeChain = append(mergeChain, newAddUint64Operator(1))

	newValueBytes, err := mergeChain.Merge([]byte("key"), existingValue)
	if err != nil {
		t.Fatal(err)
	}
	newValue, _ := binary.Uvarint(newValueBytes)
	if newValue != 62 {
		t.Errorf("expected 62, got %d", newValue)
	}
}
