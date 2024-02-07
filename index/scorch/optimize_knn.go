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
}

// This setting _MUST_ only be changed during init and not after.
var BleveMaxKNNConcurrency = 10

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

func (o *OptimizeVR) Finish() error {
	// for each field, get the vector index --> invoke the zap func.
	// for each VR, populate postings list and iterators
	// by passing the obtained vector index and getting similar vectors.
	// defer close index - just once.
	var errorsM sync.Mutex
	var errors []error

	defer o.invokeSearcherEndCallback()

	wg := sync.WaitGroup{}
	// BleveMaxKNNConcurrency is the max number of concurrent go routines to launch
	// for creating the vector postings list. This is a package level variable
	// that can be changed by the user. The default value is 10.

	// A temporary buffer is required for searching the vector index and in order to
	// avoid allocating a new buffer for each segment being searched, we create a buffer pool,
	// which also acts as a semaphore to limit the number of concurrent goroutines.
	// The size of this pool would be min(BleveMaxKNNConcurrency, numSegments).
	// So if there are 5 segments (< BleveMaxKNNConcurrency),
	//		we will have a pool of size 5. -> No reuse of buffers since we tradeoff memory for concurrency.
	// and if there are 20 segments(> BleveMaxKNNConcurrency),
	// 		we will have a pool of size 10 (= BleveMaxKNNConcurrency). -> Reuse 10 buffers for all 20 segments,
	//											  						  tradeoff concurrency for reduced memory.
	// Thus the pool acts for a dual benefit of acting as a semaphore and limiting the number of concurrent goroutines
	// and also reusing buffers for searching the vector index.

	poolSize := BleveMaxKNNConcurrency
	if len(o.snapshot.segment) < BleveMaxKNNConcurrency {
		poolSize = len(o.snapshot.segment)
	}

	type buffers struct {
		distanceBuffer []float32
		labelBuffer    []int64
	}

	semaphore := make(chan map[int64]*buffers, poolSize)

	// get a set of all possible k values
	// reuse a single buffer for each k
	kSet := make(map[int64]struct{})
	for _, vrs := range o.vrs {
		for _, vr := range vrs {
			kSet[vr.k] = struct{}{}
		}
	}

	// populate the pool with buffers
	// each buffer is a map of k -> buffers
	// so for every K value, we have a single distance buffer
	// and a single label buffer
	for i := 0; i < poolSize; i++ {
		buffer := make(map[int64]*buffers)
		for k := range kSet {
			buffer[k] = &buffers{
				distanceBuffer: make([]float32, k),
				labelBuffer:    make([]int64, k),
			}
		}
		semaphore <- buffer
	}

	// Launch goroutines to get vector index for each segment
	for i, seg := range o.snapshot.segment {
		if sv, ok := seg.segment.(segment_api.VectorSegment); ok {
			wg.Add(1)
			buffersToUse := <-semaphore // Acquire the buffers to use from the pool // block if pool is empty -> semaphore block
			go func(index int, segment segment_api.VectorSegment, origSeg *SegmentSnapshot) {
				defer func() {
					semaphore <- buffersToUse // Release the semaphore slot -> add back the used buffer to the pool for reuse
					wg.Done()
				}()
				for field, vrs := range o.vrs {
					vecIndex, err := segment.InterpretVectorIndex(field)
					if err != nil {
						errorsM.Lock()
						errors = append(errors, err)
						errorsM.Unlock()
						return
					}

					// update the vector index size as a meta value in the segment snapshot
					vectorIndexSize := vecIndex.Size()
					origSeg.cachedMeta.updateMeta(field, vectorIndexSize)
					for _, vr := range vrs {
						// for each VR, populate postings list and iterators
						// by passing the obtained vector index and getting similar vectors.
						pl, err := vecIndex.Search(vr.vector, vr.k, origSeg.deleted, buffersToUse[vr.k].distanceBuffer, buffersToUse[vr.k].labelBuffer)
						if err != nil {
							errorsM.Lock()
							errors = append(errors, err)
							errorsM.Unlock()
							go vecIndex.Close()
							return
						}

						atomic.AddUint64(&o.snapshot.parent.stats.TotKNNSearches, uint64(1))

						// postings and iterators are already alloc'ed when
						// IndexSnapshotVectorReader is created
						vr.postings[index] = pl
						vr.iterators[index] = pl.Iterator(vr.iterators[index])
					}
					go vecIndex.Close()
				}
			}(i, sv, seg)
		}
	}
	wg.Wait()
	close(semaphore)
	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}

func (s *IndexSnapshotVectorReader) VectorOptimize(ctx context.Context,
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
		vecIndexSize := seg.cachedMeta.fetchMeta(s.field)
		if vecIndexSize != nil {
			sumVectorIndexSize += vecIndexSize.(uint64)
		}
	}

	if o.ctx != nil {
		if cb := o.ctx.Value(search.SearcherStartCallbackKey); cb != nil {
			if cbF, ok := cb.(search.SearcherStartCallbackFn); ok {
				err := cbF(sumVectorIndexSize)
				if err != nil {
					// it's important to invoke the end callback at this point since
					// if the earlier searchers of this optimze struct were successful
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
