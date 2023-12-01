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
	"fmt"

	index "github.com/blevesearch/bleve_index_api"
	segment_api "github.com/blevesearch/scorch_segment_api/v2"
)

type OptimizeVR struct {
	snapshot *IndexSnapshot

	// maps field to vector readers
	vrs map[string][]*IndexSnapshotVectorReader
}

func (o *OptimizeVR) Finish() error {
	// for each field, get the vector index --> invoke the zap func.
	// for each VR, populate postings list and iterators
	// by passing the obtained vector index and getting similar vectors.
	// defer close index - just once.

	for i, seg := range o.snapshot.segment {
		// for each field, get the vector index --> invoke the zap func.
		for field, vrs := range o.vrs {
			// for each VR belonging to that field
			if sv, ok := seg.segment.(segment_api.VectorSegment); ok {
				// reading just once per field per segment.
				searchVectorIndex, closeVectorIndex, err := sv.InterpretVectorIndex(field)
				if err != nil {
					return err
				}

				for _, vr := range vrs {
					// for each VR, populate postings list and iterators
					// by passing the obtained vector index and getting similar vectors.
					pl, err := searchVectorIndex(vr.field, vr.vector, vr.k, seg.deleted)
					if err != nil {
						go closeVectorIndex()
						return err
					}
					vr.postings[i] = pl
					vr.iterators[i] = pl.Iterator(vr.iterators[i])
				}

				go closeVectorIndex()
			}
		}
	}

	return nil
}

func (s *IndexSnapshotVectorReader) VectorOptimize(
	octx index.VectorOptimizableContext) (index.VectorOptimizableContext, error) {

	if octx == nil {
		octx = &OptimizeVR{snapshot: s.snapshot,
			vrs: make(map[string][]*IndexSnapshotVectorReader),
		}
	}

	o, ok := octx.(*OptimizeVR)
	if !ok {
		return octx, nil
	}

	if o.snapshot != s.snapshot {
		return nil, fmt.Errorf("tried to optimize KNN across different snapshots")
	}

	o.vrs[s.field] = append(o.vrs[s.field], s)

	return o, nil
}
