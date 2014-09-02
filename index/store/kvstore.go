//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package store

type KVBatch interface {
	Set(key, val []byte)
	Delete(key []byte)
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
	Get(key []byte) ([]byte, error)
	Set(key, val []byte) error
	Delete(key []byte) error
	Commit() error
	Close() error

	Iterator(key []byte) KVIterator
	NewBatch() KVBatch
}
