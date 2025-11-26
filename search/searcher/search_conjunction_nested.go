//  Copyright (c) 2025 Couchbase, Inc.
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

package searcher

import (
	"container/heap"
	"context"
	"fmt"
	"math"
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeNestedConjunctionSearcher int

func init() {
	var ncs NestedConjunctionSearcher
	reflectStaticSizeNestedConjunctionSearcher = int(reflect.TypeOf(ncs).Size())
}

type NestedConjunctionSearcher struct {
	nestedReader  index.NestedReader
	searchers     []search.Searcher
	queryNorm     float64
	currs         []*search.DocumentMatch
	currAncestors [][]index.AncestorID
	currKeys      []index.AncestorID
	initialized   bool
	joinIdx       int
	options       search.SearcherOptions
	docQueue      *CoalesceQueue
}

func NewNestedConjunctionSearcher(ctx context.Context, indexReader index.IndexReader,
	searchers []search.Searcher, joinIdx int, options search.SearcherOptions) (search.Searcher, error) {

	var nr index.NestedReader
	var ok bool
	if nr, ok = indexReader.(index.NestedReader); !ok {
		return nil, fmt.Errorf("indexReader does not support nested documents")
	}

	// build our searcher
	rv := NestedConjunctionSearcher{
		nestedReader:  nr,
		options:       options,
		searchers:     searchers,
		currs:         make([]*search.DocumentMatch, len(searchers)),
		currAncestors: make([][]index.AncestorID, len(searchers)),
		currKeys:      make([]index.AncestorID, len(searchers)),
		joinIdx:       joinIdx,
		docQueue:      NewCoalesceQueue(),
	}
	rv.computeQueryNorm()

	return &rv, nil
}

func (s *NestedConjunctionSearcher) computeQueryNorm() {
	// first calculate sum of squared weights
	sumOfSquaredWeights := 0.0
	for _, searcher := range s.searchers {
		sumOfSquaredWeights += searcher.Weight()
	}
	// now compute query norm from this
	s.queryNorm = 1.0 / math.Sqrt(sumOfSquaredWeights)
	// finally tell all the downstream searchers the norm
	for _, searcher := range s.searchers {
		searcher.SetQueryNorm(s.queryNorm)
	}
}

func (s *NestedConjunctionSearcher) Size() int {
	sizeInBytes := reflectStaticSizeNestedConjunctionSearcher + size.SizeOfPtr

	for _, entry := range s.searchers {
		sizeInBytes += entry.Size()
	}

	for _, entry := range s.currs {
		if entry != nil {
			sizeInBytes += entry.Size()
		}
	}

	return sizeInBytes
}

func (s *NestedConjunctionSearcher) Weight() float64 {
	var rv float64
	for _, searcher := range s.searchers {
		rv += searcher.Weight()
	}
	return rv
}

func (s *NestedConjunctionSearcher) SetQueryNorm(qnorm float64) {
	for _, searcher := range s.searchers {
		searcher.SetQueryNorm(qnorm)
	}
}

func (s *NestedConjunctionSearcher) Count() uint64 {
	// for now return a worst case
	var sum uint64
	for _, searcher := range s.searchers {
		sum += searcher.Count()
	}
	return sum
}

func (s *NestedConjunctionSearcher) Close() (rv error) {
	for _, searcher := range s.searchers {
		err := searcher.Close()
		if err != nil && rv == nil {
			rv = err
		}
	}
	return rv
}

func (s *NestedConjunctionSearcher) Min() int {
	return 0
}

func (s *NestedConjunctionSearcher) DocumentMatchPoolSize() int {
	rv := len(s.currs)
	for _, s := range s.searchers {
		rv += s.DocumentMatchPoolSize()
	}
	return rv
}

func (s *NestedConjunctionSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	var err error
	// initialize on first call to Next, by getting first match
	// from each searcher and their ancestry chains
	if !s.initialized {
		for i, searcher := range s.searchers {
			if s.currs[i] != nil {
				ctx.DocumentMatchPool.Put(s.currs[i])
			}
			s.currs[i], err = searcher.Next(ctx)
			if err != nil {
				return nil, err
			}
			if s.currs[i] == nil {
				// one of the searchers is exhausted, so we are done
				return nil, nil
			}
			// get the ancestry chain for this match
			s.currAncestors[i], err = s.nestedReader.Ancestors(s.currs[i].IndexInternalID)
			if err != nil {
				return nil, err
			}
			// check if the ancestry chain is > joinIdx, if not we reset the joinIdx
			// to the minimum possible value across all searchers, ideally this will be
			// done in query construction time itself, by using the covering depth across
			// all sub-queries, but we do this here as a fallback
			if s.joinIdx >= len(s.currAncestors[i]) {
				s.joinIdx = len(s.currAncestors[i]) - 1
			}
		}
		// build currKeys for each searcher, do it here as we may have adjusted joinIdx
		for i := range s.searchers {
			s.currKeys[i] = s.getKeyForIdx(i)
		}
		s.initialized = true
	}
	// check if the docQueue has any buffered matches
	if s.docQueue.Len() > 0 {
		return s.docQueue.Dequeue()
	}
OUTER:
	for {
		// pick the pivot searcher with the highest key (ancestor at joinIdx level)
		if s.currs[0] == nil {
			return nil, nil
		}
		maxKey := s.currKeys[0]
		for i := 1; i < len(s.searchers); i++ {
			// currs[i] is nil means one of the searchers is exhausted
			if s.currs[i] == nil {
				return nil, nil
			}
			currKey := s.currKeys[i]
			if maxKey.Compare(currKey) < 0 {
				maxKey = currKey
			}
		}
		// now try to align all other searchers to the
		// we check if the a searchers key matches maxKey
		// if not, we advance the pivot searcher to maxKey
		// else do nothing and move to the next searcher
		for i := 0; i < len(s.searchers); i++ {
			if s.currKeys[i].Compare(maxKey) < 0 {
				// not aligned, so advance this searcher to maxKey
				var err error
				ctx.DocumentMatchPool.Put(s.currs[i])
				s.currs[i], err = s.searchers[i].Advance(ctx, maxKey.ToIndexInternalID())
				if err != nil {
					return nil, err
				}
				if s.currs[i] == nil {
					// one of the searchers is exhausted, so we are done
					return nil, nil
				}
				// recalc ancestors
				s.currAncestors[i], err = s.nestedReader.Ancestors(s.currs[i].IndexInternalID)
				if err != nil {
					return nil, err
				}
				// recalc key
				s.currKeys[i] = s.getKeyForIdx(i)
			}
		}
		// now check if all the searchers are aligned at the same maxKey
		// if they are not aligned, we need to restart the loop of picking
		// the pivot searcher with the highest key
		for i := 0; i < len(s.searchers); i++ {
			if !s.currKeys[i].Equals(maxKey) {
				// not aligned, so restart the outer loop
				continue OUTER
			}
		}
		// if we are here, all the searchers are aligned at maxKey
		// now we need to buffer all the intermediate matches for every
		// searcher at this key, until either the searcher's key changes
		// or the searcher is exhausted
		for i := 0; i < len(s.searchers); i++ {
			for {
				// buffer the current match
				recycle, err := s.docQueue.Enqueue(s.currs[i])
				if err != nil {
					return nil, err
				}
				if recycle != nil {
					// we got a match to recycle
					ctx.DocumentMatchPool.Put(recycle)
				}
				// advance to next match
				s.currs[i], err = s.searchers[i].Next(ctx)
				if err != nil {
					return nil, err
				}
				if s.currs[i] == nil {
					// searcher exhausted, break out
					break
				}
				// recalc ancestors
				s.currAncestors[i], err = s.nestedReader.Ancestors(s.currs[i].IndexInternalID)
				if err != nil {
					return nil, err
				}
				// recalc key
				s.currKeys[i] = s.getKeyForIdx(i)
				// check if key has changed
				if !s.currKeys[i].Equals(maxKey) {
					// key changed, break out
					break
				}
			}
		}
		// finally return the first buffered match
		return s.docQueue.Dequeue()
	}
}

func (s *NestedConjunctionSearcher) getKeyForIdx(i int) index.AncestorID {
	return s.currAncestors[i][len(s.currAncestors[i])-s.joinIdx-1]
}

func (s *NestedConjunctionSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	for {
		next, err := s.Next(ctx)
		if err != nil {
			return nil, err
		}
		if next == nil {
			return nil, nil
		}
		if next.IndexInternalID.Compare(ID) >= 0 {
			return next, nil
		}
		ctx.DocumentMatchPool.Put(next)
	}
}

// ------------------------------------------------------------------------------------------
type CoalesceQueue struct {
	order []*search.DocumentMatch          // queue of DocumentMatch
	items map[uint64]*search.DocumentMatch // map of ID to DocumentMatch
}

func NewCoalesceQueue() *CoalesceQueue {
	cq := &CoalesceQueue{
		order: make([]*search.DocumentMatch, 0),
		items: make(map[uint64]*search.DocumentMatch),
	}
	heap.Init(cq)
	return cq
}

func (cq *CoalesceQueue) Enqueue(it *search.DocumentMatch) (*search.DocumentMatch, error) {
	val, err := it.IndexInternalID.Value()
	if err != nil {
		// cannot coalesce without a valid uint64 ID
		return nil, err
	}

	if existing, ok := cq.items[val]; ok {
		// merge with current version
		existing.Score += it.Score
		existing.Expl = existing.Expl.MergeWith(it.Expl)
		existing.FieldTermLocations = search.MergeFieldTermLocations(
			existing.FieldTermLocations, []*search.DocumentMatch{it})
		// return it to caller for recycling
		return it, nil
	}

	// first time we see this ID — enqueue
	cq.items[val] = it
	heap.Push(cq, it)
	// no recycling needed as we added a new item
	return nil, nil
}

func (cq *CoalesceQueue) Dequeue() (*search.DocumentMatch, error) {
	if cq.Len() == 0 {
		return nil, nil
	}

	rv := heap.Pop(cq).(*search.DocumentMatch)

	val, err := rv.IndexInternalID.Value()
	if err != nil {
		return nil, err
	}

	delete(cq.items, val)
	return rv, nil
}

// heap implementation

func (cq *CoalesceQueue) Len() int {
	return len(cq.order)
}

func (cq *CoalesceQueue) Less(i, j int) bool {
	return cq.order[i].IndexInternalID.Compare(cq.order[j].IndexInternalID) < 0
}

func (cq *CoalesceQueue) Swap(i, j int) {
	cq.order[i], cq.order[j] = cq.order[j], cq.order[i]
}

func (cq *CoalesceQueue) Push(x any) {
	cq.order = append(cq.order, x.(*search.DocumentMatch))
}

func (cq *CoalesceQueue) Pop() any {
	old := cq.order
	n := len(old)
	x := old[n-1]
	cq.order = old[:n-1]
	return x
}
