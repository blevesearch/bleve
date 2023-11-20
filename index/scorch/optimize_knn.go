//go:build vectors
// +build vectors

package scorch

import (
	"fmt"

	index "github.com/blevesearch/bleve_index_api"
	segment_api "github.com/blevesearch/scorch_segment_api/v2"
)

type OptimizeVRDisjunction struct {
	snapshot *IndexSnapshot

	// map of the same field --> vrs
	vrs map[string][]*IndexSnapshotVectorReader
}

func (o *OptimizeVRDisjunction) Finish() (index.Optimized, error) {

	// for each field, get the faiss index --> invoke the zap func.

	// for each VR, populate postings list and iterators
	// by passing the obtained FAISS index and getting similar vectors.

	// defer close index - just once.

	for i, seg := range o.snapshot.segment {
		// for each field, get the faiss index --> invoke the zap func.
		for field, vrs := range o.vrs {
			// for each VR belonging to that field
			if sv, ok := seg.segment.(segment_api.VectorSegment); ok {
				// reading just once per field per segment.
				faissIndex, err := sv.ReadVectorIndex(field)
				if err != nil {
					return nil, err
				}
				defer faissIndex.Close()

				for _, vr := range vrs {
					// for each VR, populate postings list and iterators
					// by passing the obtained FAISS index and getting similar vectors.
					pl, err := sv.SearchSimilarVectors(faissIndex, vr.field,
						vr.queryVector, vr.k, seg.deleted)
					if err != nil {
						return nil, err
					}
					vr.postings[i] = pl
					vr.iterators[i] = pl.Iterator(vr.iterators[i])
				}
			}
		}
	}

	return nil, nil
}

func (s *IndexSnapshotVectorReader) Optimize(kind string,
	octx index.OptimizableContext) (index.OptimizableContext, error) {

	if octx == nil {
		octx = &OptimizeVRDisjunction{snapshot: s.snapshot,
			vrs: make(map[string][]*IndexSnapshotVectorReader),
		}
	}

	o, ok := octx.(*OptimizeVRDisjunction)
	if !ok {
		return octx, nil
	}

	if o.snapshot != s.snapshot {
		return nil, fmt.Errorf("tried to optimize KNN across different snapshots")
	}

	o.vrs[s.field] = append(o.vrs[s.field], s)

	return o, nil
}
