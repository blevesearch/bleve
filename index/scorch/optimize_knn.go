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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
	segment_api "github.com/blevesearch/scorch_segment_api/v2"
)

type OptimizeVR struct {
	ctx       context.Context
	snapshot  *IndexSnapshot
	totalCost uint64
	// maps field to vector readers
	vrs map[string][]*IndexSnapshotVectorReader

	// preassigned holds the phase one centroid ranking for each vector reader
	// that has one. Populated once by rankCentroids before any segment is
	// searched, and read only afterwards, so the per-segment goroutines can
	// share it without synchronisation.
	preassigned map[*IndexSnapshotVectorReader]*segment_api.PreassignedCentroids
}

// rankCentroids runs phase one of the two phase kNN search: for every query
// vector, one coarse quantizer search against the trained index, ranking its
// centroids by distance from that vector.
//
// Phase two then hands the ranking to each segment, which probes exactly those
// inverted lists instead of working out which clusters to probe itself. That is
// sound because every segment written by the fast merge path carries a clone of
// the trained index's coarse quantizer, so cluster numbering is shared; each
// segment still verifies this before trusting the ranking, and the ones that
// cannot - typically small segments that never went through fast merge - simply
// search as before.
//
// This is best effort. Anything that goes wrong here leaves the reader without
// a ranking, which costs recall nothing: that reader's segments each fall back
// to their own coarse quantizer search.
func (o *OptimizeVR) rankCentroids() {
	if o.snapshot == nil || o.snapshot.parent == nil {
		return
	}
	ranker, ok := o.snapshot.parent.trainer.(centroidRanker)
	if !ok {
		// no trainer, or one with no trained index to rank against.
		return
	}
	for field, vrs := range o.vrs {
		// skip fields whose index data is gone; the per-segment loop below
		// skips them too.
		if info, ok := o.snapshot.updatedFields[field]; ok && (info.Deleted || info.Index) {
			continue
		}
		for _, vr := range vrs {
			pre, err := ranker.searchCentroids(field, vr.vector)
			if err != nil || !pre.Valid() {
				continue
			}
			if o.preassigned == nil {
				o.preassigned = make(map[*IndexSnapshotVectorReader]*segment_api.PreassignedCentroids)
			}
			o.preassigned[vr] = pre
			atomic.AddUint64(&o.snapshot.parent.stats.TotKNNCentroidRankings, 1)
		}
	}
}

// searchSegment runs vr's kNN search against one segment's vector index, taking
// the pre-assigned centroid path when phase one produced a ranking and the
// index can use it.
func (o *OptimizeVR) searchSegment(vecIndex segment_api.VectorIndex, segID int,
	vr *IndexSnapshotVectorReader) (segment_api.VecPostingsList, error) {
	// check if the vector reader is configured to use a pre-filter to filter
	// out ineligible documents before performing kNN search.
	var eligible index.EligibleDocumentList
	if vr.eligibleSelector != nil {
		eligible = vr.eligibleSelector.SegmentEligibleDocuments(segID)
	}

	pre := o.preassigned[vr]
	pvi, canPreassign := vecIndex.(segment_api.PreassignedVectorIndex)
	if pre != nil && canPreassign {
		// The Preassigned calls below verify the layout themselves and fall
		// back silently when it does not match, so asking first is not needed
		// for correctness. We ask anyway so the counters record which path was
		// actually taken; the answer is memoized per cached index, so this
		// costs a map lookup after the first query against a segment.
		shared, err := pvi.SharesCentroidLayout(pre)
		if err != nil {
			return nil, err
		}
		if shared {
			atomic.AddUint64(&o.snapshot.parent.stats.TotKNNPreassignedSegmentSearches, 1)
			if eligible != nil {
				return pvi.SearchWithFilterPreassigned(vr.vector, vr.k, eligible, pre, vr.searchParams)
			}
			return pvi.SearchPreassigned(vr.vector, vr.k, pre, vr.searchParams)
		}
	}

	atomic.AddUint64(&o.snapshot.parent.stats.TotKNNUnassignedSegmentSearches, 1)
	if eligible != nil {
		return vecIndex.SearchWithFilter(vr.vector, vr.k, eligible, vr.searchParams)
	}
	return vecIndex.Search(vr.vector, vr.k, vr.searchParams)
}

func (o *OptimizeVR) invokeSearcherEndCallback() {
	if o.ctx != nil {
		if cb := o.ctx.Value(search.SearcherEndCallbackKey); cb != nil {
			if cbF, ok := cb.(search.SearcherEndCallbackFn); ok {
				if o.totalCost > 0 {
					// notify the callback that the searcher creation etc. is finished
					// and report back the total cost for it to track and take actions
					// appropriately.
					_ = cbF(o.totalCost)
				}
			}
		}
	}
}

// search runs the configured kNN searches for every vector
// reader against a single segment, populating the per-segment postings and
// iterators on each reader.
func (o *OptimizeVR) search(segID int) error {
	snapshot := o.snapshot.segment[segID]
	vecSeg, ok := snapshot.segment.(segment_api.VectorSegment)
	if !ok {
		return nil
	}
	meta := snapshot.cachedMeta
	// for each field, get the vector index --> invoke the zap func.
	// for each VR, populate postings list and iterators
	// by passing the obtained vector index and getting similar vectors.
	for field, vrs := range o.vrs {
		// Early exit if the field is supposed to be completely deleted or
		// if it's index data has been deleted
		if info, ok := o.snapshot.updatedFields[field]; ok && (info.Deleted || info.Index) {
			continue
		}
		vecIndex, err := vecSeg.InterpretVectorIndex(field, snapshot.deleted)
		if err != nil {
			return err
		}
		// update the vector index size as a meta value in the segment snapshot
		// if not already present
		if !meta.contains(field) {
			meta.store(field, vecIndex.Size())
		}
		for _, vr := range vrs {
			// for each VR, populate postings list and iterators
			// by passing the obtained vector index and getting similar vectors.
			pl, err := o.searchSegment(vecIndex, segID, vr)
			if err != nil {
				vecIndex.Close()
				return err
			}
			// postings and iterators are already alloc'ed when
			// IndexSnapshotVectorReader is created, here we are just
			// populating them with the obtained postings list.
			vr.postings[segID] = pl
			vr.iterators[segID] = pl.Iterator(vr.iterators[segID])
		}
		go vecIndex.Close()
	}
	return nil
}

func (o *OptimizeVR) Finish() error {
	defer o.invokeSearcherEndCallback()

	// Phase one: rank the centroids once per query vector, against the trained
	// index, so the per-segment searches below do not each repeat it.
	o.rankCentroids()

	numSegments := len(o.snapshot.segment)

	var wg sync.WaitGroup
	wg.Add(numSegments)
	var errM sync.Mutex
	var searchErr error
	// launch goroutines to search the vector index for each segment
	for i := 0; i < numSegments; i++ {
		go func(segID int) {
			defer wg.Done()
			if err := o.search(segID); err != nil {
				errM.Lock()
				searchErr = err
				errM.Unlock()
			}
		}(i)
	}
	// wait until all the launched goroutines finish and collect errors if any
	wg.Wait()
	return searchErr
}

func (s *IndexSnapshotVectorReader) VectorOptimize(ctx context.Context,
	octx index.VectorOptimizableContext,
) (index.VectorOptimizableContext, error) {
	if s.snapshot.parent.segPlugin.Version() < VectorSearchSupportedSegmentVersion {
		return nil, fmt.Errorf("vector search not supported for this index, "+
			"index's segment version %v, supported segment version for vector search %v",
			s.snapshot.parent.segPlugin.Version(), VectorSearchSupportedSegmentVersion)
	}

	if octx == nil {
		octx = &OptimizeVR{
			snapshot: s.snapshot,
			vrs:      make(map[string][]*IndexSnapshotVectorReader),
		}
	}

	o, ok := octx.(*OptimizeVR)
	if !ok {
		return octx, nil
	}
	o.ctx = ctx

	if o.snapshot != s.snapshot {
		o.invokeSearcherEndCallback()
		return nil, fmt.Errorf("tried to optimize KNN across different snapshots")
	}

	// for every searcher creation, consult the segment snapshot to see
	// what's the vector index size and since you're anyways going
	// to use this vector index to perform the search etc. as part of the Finish()
	// perform a check as to whether we allow the searcher creation (the downstream)
	// Finish() logic to even occur or not.
	var sumVectorIndexSize uint64
	for _, seg := range o.snapshot.segment {
		if vecIndexSize, ok := seg.cachedMeta.load(s.field); ok {
			sumVectorIndexSize += vecIndexSize.(uint64)
		}
	}

	if o.ctx != nil {
		if cb := o.ctx.Value(search.SearcherStartCallbackKey); cb != nil {
			if cbF, ok := cb.(search.SearcherStartCallbackFn); ok {
				err := cbF(sumVectorIndexSize)
				if err != nil {
					// it's important to invoke the end callback at this point since
					// if the earlier searchers of this optimize struct were successful
					// the cost corresponding to it would be incremented and if the
					// current searcher fails the check then we end up erroring out
					// the overall optimized searcher creation, the cost needs to be
					// handled appropriately.
					o.invokeSearcherEndCallback()
					return nil, err
				}
			}
		}
	}

	// total cost is essentially the sum of the vector indexes' size across all the
	// searchers - all of them end up reading and maintaining a vector index.
	// misacconting this value would end up calling the "end" callback with a value
	// not equal to the value passed to "start" callback.
	o.totalCost += sumVectorIndexSize
	o.vrs[s.field] = append(o.vrs[s.field], s)
	return o, nil
}
