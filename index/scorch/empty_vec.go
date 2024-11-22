//  Copyright (c) 2024 Couchbase, Inc.
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

//go:build vectors
// +build vectors

package scorch

import segment "github.com/blevesearch/scorch_segment_api/v2"

type emptyVecPostingsIterator struct{}

func (e *emptyVecPostingsIterator) Next() (segment.VecPosting, error) {
	return nil, nil
}

func (e *emptyVecPostingsIterator) Advance(uint64) (segment.VecPosting, error) {
	return nil, nil
}

func (e *emptyVecPostingsIterator) Size() int {
	return 0
}

func (e *emptyVecPostingsIterator) BytesRead() uint64 {
	return 0
}

func (e *emptyVecPostingsIterator) ResetBytesRead(uint64) {}

func (e *emptyVecPostingsIterator) BytesWritten() uint64 { return 0 }

var anemptyVecPostingsIterator = &emptyVecPostingsIterator{}
