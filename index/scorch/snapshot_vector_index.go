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

//go:build vectors
// +build vectors

package scorch

import (
	"context"
	"encoding/json"

	index "github.com/blevesearch/bleve_index_api"
	segment_api "github.com/blevesearch/scorch_segment_api/v2"
)

func (is *IndexSnapshot) VectorReader(ctx context.Context, vector []float32,
	field string, k int64, searchParams json.RawMessage) (
	index.VectorReader, error) {

	rv := &IndexSnapshotVectorReader{
		vector:       vector,
		field:        field,
		k:            k,
		snapshot:     is,
		searchParams: searchParams,
	}

	if rv.postings == nil {
		rv.postings = make([]segment_api.VecPostingsList, len(is.segment))
	}
	if rv.iterators == nil {
		rv.iterators = make([]segment_api.VecPostingsIterator, len(is.segment))
	}

	// initialize postings and iterators within the OptimizeVR's Finish()

	return rv, nil
}
