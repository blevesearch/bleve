//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the
//  License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing,
//  software distributed under the License is distributed on an "AS
//  IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
//  express or implied. See the License for the specific language
//  governing permissions and limitations under the License.

// +build go1.4

package cznicb

import (
	"github.com/blevesearch/bleve/index/store"
)

type Reader struct {
	s *Store
}

func newReader(s *Store) (*Reader, error) {
	return &Reader{
		s: s,
	}, nil
}

func (r *Reader) BytesSafeAfterClose() bool {
	return false
}

func (r *Reader) Get(key []byte) ([]byte, error) {
	return r.s.get(key)
}

func (r *Reader) Iterator(key []byte) store.KVIterator {
	return r.s.iterator(key)
}

func (r *Reader) Close() error {
	return nil
}
