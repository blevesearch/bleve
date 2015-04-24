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
	"fmt"
)

// At the moment this happens to be the same interface as described by
// RocksDB, but this may not always be the case.

type MergeOperator interface {

	// FullMerge the full sequence of operands on top of the existingValue
	// if no value currently exists, existingValue is nil
	// return the merged value, and success/failure
	FullMerge(key, existingValue []byte, operands [][]byte) ([]byte, bool)

	// Partially merge these two operands.
	// If partial merge cannot be done, return nil,false, which will defer
	// all processing until the FullMerge is done.
	PartialMerge(key, leftOperand, rightOperand []byte) ([]byte, bool)

	// Name returns an identifier for the operator
	Name() string
}

// EmulatedMergeSingle removes some duplicated code across
// KV stores which do not support merge operations
// on their own.  It is up to the caller to ensure
// that an appropriate lock has been acquired in
// order for this behavior to be valid
func EmulatedMergeSingle(writer KVWriter, mo MergeOperator, key []byte, operand []byte) error {
	existingValue, err := writer.Get(key)
	if err != nil {
		return err
	}
	newValue, ok := mo.FullMerge(key, existingValue, [][]byte{operand})
	if !ok {
		return fmt.Errorf("merge operator returned failure")
	}
	err = writer.Set(key, newValue)
	if err != nil {
		return err
	}
	return nil
}

type EmulatedMerge struct {
	merges map[string][][]byte
	mo     MergeOperator
}

func NewEmulatedMerge(mo MergeOperator) *EmulatedMerge {
	return &EmulatedMerge{
		merges: make(map[string][][]byte),
		mo:     mo,
	}
}

func (m *EmulatedMerge) Merge(key, val []byte) {
	ops, ok := m.merges[string(key)]
	if ok && len(ops) > 0 {
		last := ops[len(ops)-1]
		mergedVal, partialMergeOk := m.mo.PartialMerge(key, last, val)
		if partialMergeOk {
			// replace last entry with the result of the merge
			ops[len(ops)-1] = mergedVal
		} else {
			// could not partial merge, append this to the end
			ops = append(ops, val)
		}
	} else {
		ops = [][]byte{val}
	}
	m.merges[string(key)] = ops
}

func (m *EmulatedMerge) Execute(w KVWriter) error {
	for k, mergeOps := range m.merges {
		kb := []byte(k)
		existingVal, err := w.Get(kb)
		if err != nil {
			return err
		}
		mergedVal, fullMergeOk := m.mo.FullMerge(kb, existingVal, mergeOps)
		if !fullMergeOk {
			return fmt.Errorf("merge operator returned failure")
		}
		err = w.Set(kb, mergedVal)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *EmulatedMerge) ExecuteDeferred(w KVWriter) ([]*op, error) {
	rv := make([]*op, 0, 1000)
	for k, mergeOps := range m.merges {
		kb := []byte(k)
		existingVal, err := w.Get(kb)
		if err != nil {
			return nil, err
		}
		mergedVal, fullMergeOk := m.mo.FullMerge(kb, existingVal, mergeOps)
		if !fullMergeOk {
			return nil, fmt.Errorf("merge operator returned failure")
		}
		rv = append(rv, &op{kb, mergedVal})
	}
	return rv, nil
}
