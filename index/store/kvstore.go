//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package store

import ()

type KVBatch interface {
	Set(key, val []byte)
	Delete(key []byte)
	Merge(key []byte, oper AssociativeMerge)
	Execute() error
	Close() error
}

type KVIterator interface {
	SeekFirst()
	Seek([]byte)
	Next()

	Current() ([]byte, []byte, bool)
	Key() []byte
	Value() []byte
	Valid() bool

	Close()
}

type KVStore interface {
	Writer() KVWriter
	Reader() KVReader
	Close() error
}

type KVWriter interface {
	KVReader
	Set(key, val []byte) error
	Delete(key []byte) error
	NewBatch() KVBatch
}

type KVReader interface {
	Get(key []byte) ([]byte, error)
	Iterator(key []byte) KVIterator
	Close() error
}

type AssociativeMerge interface {
	Merge(key, existing []byte) ([]byte, error)
}

type AssociativeMergeChain []AssociativeMerge

func (a AssociativeMergeChain) Merge(key, orig []byte) ([]byte, error) {
	curr := orig
	for _, m := range a {
		var err error
		curr, err = m.Merge(key, curr)
		if err != nil {
			return nil, err
		}
	}
	return curr, nil
}
