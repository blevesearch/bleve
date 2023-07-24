//  Copyright (c) 2023 Couchbase, Inc.
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

//go:build densevector
// +build densevector

package scorch

import (
	"context"
	"fmt"

	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

func (is *IndexSnapshot) VectorReader(ctx context.Context, vector []float32, field string) (
	index.VectorReader, error) {

	fmt.Printf("first \n")
	rv := &IndexSnapshotVectorReader{
		vector:   vector,
		field:    field,
		snapshot: is,
	}

	if rv.postings == nil {
		rv.postings = make([]segment.VecPostingsList, len(is.segment))
	}
	if rv.iterators == nil {
		rv.iterators = make([]segment.VecPostingsIterator, len(is.segment))
	}

	fmt.Printf("segment length: %d, %d \n", len(is.segment), len(rv.postings))
	for i, seg := range is.segment {
		// add k to index snapshot from query TODO
		// TODO Get the roaring bitmap stuff?
		// fmt.Printf("is seg.seg nil: %v\n", seg.segment == nil) -> false
		if sv, ok := seg.segment.(segment.VectorSegment); ok {
			fmt.Printf("passing check")
			pl, err := sv.SimilarVectors(field, vector, 10, seg.deleted)
			if err != nil {
				return nil, err
			}
			fmt.Printf("is pl nil: %v\n", pl == nil)
			rv.postings[i] = pl
			rv.iterators[i] = pl.Iterator(rv.iterators[i])
		}
	}

	return rv, nil
}
