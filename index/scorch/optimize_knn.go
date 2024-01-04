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
	"sync"

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
	var errors []error
	var mu sync.Mutex
	wg := sync.WaitGroup{}
	// Launch goroutines to get vector index for each segment
	for i, seg := range o.snapshot.segment {
		if sv, ok := seg.segment.(segment_api.VectorSegment); ok {
			wg.Add(1)
			go func(index int, segment segment_api.VectorSegment, origSeg *SegmentSnapshot) {
				defer wg.Done()
				for field, vrs := range o.vrs {
					searchVectorIndex, closeVectorIndex, err := segment.InterpretVectorIndex(field)
					if err != nil {
						mu.Lock()
						errors = append(errors, err)
						mu.Unlock()
						return
					}
					for _, vr := range vrs {
						// for each VR, populate postings list and iterators
						// by passing the obtained vector index and getting similar vectors.
						pl, err := searchVectorIndex(vr.field, vr.vector, vr.k, origSeg.deleted)
						if err != nil {
							mu.Lock()
							errors = append(errors, err)
							mu.Unlock()
							go closeVectorIndex()
							return
						}

						// postings and iterators are already alloc'ed when
						// IndexSnapshotVectorReader is created
						vr.postings[index] = pl
						vr.iterators[index] = pl.Iterator(vr.iterators[index])
					}
					go closeVectorIndex()
				}
			}(i, sv, seg)
		}
	}
	wg.Wait()
	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}

func (s *IndexSnapshotVectorReader) VectorOptimize(
	octx index.VectorOptimizableContext) (index.VectorOptimizableContext, error) {

	if s.snapshot.parent.segPlugin.Version() < VectorSearchSupportedSegmentVersion {
		return nil, fmt.Errorf("vector search not supported for this index, "+
			"index's segment version %v, supported segment version for vector search %v",
			s.snapshot.parent.segPlugin.Version(), VectorSearchSupportedSegmentVersion)
	}

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
