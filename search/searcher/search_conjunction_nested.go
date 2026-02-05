//  Copyright (c) 2026 Couchbase, Inc.
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
	"context"
	"fmt"
	"math"
	"reflect"
	"slices"

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
	// reusable ID buffer for Advance() calls
	advanceID index.IndexInternalID
	// reusable buffer for Advance() calls
	ancestors []index.AncestorID
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

func (s *NestedConjunctionSearcher) initialize(ctx *search.SearchContext) (bool, error) {
	var err error
	for i, searcher := range s.searchers {
		if s.currs[i] != nil {
			ctx.DocumentMatchPool.Put(s.currs[i])
		}
		s.currs[i], err = searcher.Next(ctx)
		if err != nil {
			return false, err
		}
		if s.currs[i] == nil {
			// one of the searchers is exhausted, so we are done
			return true, nil
		}
		// get the ancestry chain for this match
		s.currAncestors[i], err = s.nestedReader.Ancestors(s.currs[i].IndexInternalID, s.currAncestors[i][:0])
		if err != nil {
			return false, err
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
		s.currKeys[i] = ancestorFromRoot(s.currAncestors[i], s.joinIdx)
	}
	s.initialized = true
	return false, nil
}

func (s *NestedConjunctionSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	// initialize on first call to Next, by getting first match
	// from each searcher and their ancestry chains
	if !s.initialized {
		done, err := s.initialize(ctx)
		if err != nil {
			return nil, err
		}
		if done {
			return nil, nil
		}
	}
	// check if the docQueue has any buffered matches
	if s.docQueue.Len() > 0 {
		return s.docQueue.Dequeue(ctx), nil
	}
	// now enter the main alignment loop
	n := len(s.searchers)
OUTER:
	for {
		// pick the pivot searcher with the highest key (ancestor at joinIdx level)
		if s.currs[0] == nil {
			return nil, nil
		}
		maxKey := s.currKeys[0]
		for i := 1; i < n; i++ {
			// currs[i] is nil means one of the searchers is exhausted
			if s.currs[i] == nil {
				return nil, nil
			}
			currKey := s.currKeys[i]
			if maxKey.Compare(currKey) < 0 {
				maxKey = currKey
			}
		}
		// store maxkey as advanceID only once only if needed
		var advanceID index.IndexInternalID
		// flag to track if all searchers are aligned
		var aligned bool = true
		// now try to align all other searchers to the
		// we check if the a searchers key matches maxKey
		// if not, we advance the pivot searcher to maxKey
		// else do nothing and move to the next searcher
		for i := 0; i < n; i++ {
			cmp := s.currKeys[i].Compare(maxKey)
			if cmp < 0 {
				// not aligned, so advance this searcher to maxKey
				// convert maxKey to advanceID only once
				if advanceID == nil {
					advanceID = s.toAdvanceID(maxKey)
				}
				var err error
				ctx.DocumentMatchPool.Put(s.currs[i])
				s.currs[i], err = s.searchers[i].Advance(ctx, advanceID)
				if err != nil {
					return nil, err
				}
				if s.currs[i] == nil {
					// one of the searchers is exhausted, so we are done
					return nil, nil
				}
				// recalc ancestors
				s.currAncestors[i], err = s.nestedReader.Ancestors(s.currs[i].IndexInternalID, s.currAncestors[i][:0])
				if err != nil {
					return nil, err
				}
				// recalc key
				s.currKeys[i] = ancestorFromRoot(s.currAncestors[i], s.joinIdx)
				// recalc cmp
				cmp = s.currKeys[i].Compare(maxKey)
			}
			if cmp != 0 {
				// not aligned
				aligned = false
			}
		}
		// now check if all the searchers are aligned at the same maxKey
		// if they are not aligned, we need to restart the loop of picking
		// the pivot searcher with the highest key
		if !aligned {
			continue OUTER
		}
		// if we are here, all the searchers are aligned at maxKey
		// now we need to buffer all the intermediate matches for every
		// searcher at this key, until either the searcher's key changes
		// or the searcher is exhausted
		var err error
		for i := 0; i < n; i++ {
			for {
				// buffer the current match
				s.docQueue.Enqueue(s.currs[i])
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
				s.currAncestors[i], err = s.nestedReader.Ancestors(s.currs[i].IndexInternalID, s.currAncestors[i][:0])
				if err != nil {
					return nil, err
				}
				// recalc key
				s.currKeys[i] = ancestorFromRoot(s.currAncestors[i], s.joinIdx)
				// check if key has changed
				if !s.currKeys[i].Equals(maxKey) {
					// key changed, break out
					break
				}
			}
		}
		// finalize the docQueue for dequeueing
		s.docQueue.Finalize()
		// finally return the first buffered match
		return s.docQueue.Dequeue(ctx), nil
	}
}

// ancestorFromRoot gets the AncestorID at the given position from the root
// if pos is 0, it returns the root AncestorID, and so on
func ancestorFromRoot(ancestors []index.AncestorID, pos int) index.AncestorID {
	return ancestors[len(ancestors)-pos-1]
}

// toAdvanceID converts an AncestorID to IndexInternalID, reusing the advanceID buffer.
// The returned ID is safe to pass to Advance() since Advance() never retains references.
func (s *NestedConjunctionSearcher) toAdvanceID(key index.AncestorID) index.IndexInternalID {
	// Reset length to 0 while preserving capacity for buffer reuse
	s.advanceID = s.advanceID[:0]
	// Convert key to IndexInternalID, reusing the underlying buffer
	s.advanceID = key.ToIndexInternalID(s.advanceID)
	return s.advanceID
}

func (s *NestedConjunctionSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	if !s.initialized {
		done, err := s.initialize(ctx)
		if err != nil {
			return nil, err
		}
		if done {
			return nil, nil
		}
	}
	// first check if the docQueue has any buffered matches
	// if so we first check if any of them can satisfy the Advance(ID)
	for s.docQueue.Len() > 0 {
		dm := s.docQueue.Dequeue(ctx)
		if dm.IndexInternalID.Compare(ID) >= 0 {
			return dm, nil
		}
		// otherwise recycle this match
		ctx.DocumentMatchPool.Put(dm)
	}
	var err error
	// now we first get the ancestry chain for the given ID
	s.ancestors, err = s.nestedReader.Ancestors(ID, s.ancestors[:0])
	if err != nil {
		return nil, err
	}
	// we now follow the the following logic for each searcher:
	// let S be the length of the ancestry chain for the searcher
	// let I be the length of the ancestry chain for the given ID
	// 1. if S > I:
	//    then we just Advance() the searcher to the given ID if required
	// 2. else if S <= I:
	//    then we get the AncestorID at position (S - 1) from the root of
	//    the given ID's ancestry chain, and Advance() the searcher to
	//    it if required
	for i, searcher := range s.searchers {
		if s.currs[i] == nil {
			return nil, nil // already exhausted, nothing to do
		}
		var targetID index.IndexInternalID
		S := len(s.currAncestors[i])
		I := len(s.ancestors)
		if S > I {
			// case 1: S > I
			targetID = ID
		} else {
			// case 2: S <= I
			targetID = s.toAdvanceID(ancestorFromRoot(s.ancestors, S-1))
		}
		if s.currs[i].IndexInternalID.Compare(targetID) < 0 {
			// need to advance this searcher
			ctx.DocumentMatchPool.Put(s.currs[i])
			s.currs[i], err = searcher.Advance(ctx, targetID)
			if err != nil {
				return nil, err
			}
			if s.currs[i] == nil {
				// one of the searchers is exhausted, so we are done
				return nil, nil
			}
			// recalc ancestors
			s.currAncestors[i], err = s.nestedReader.Ancestors(s.currs[i].IndexInternalID, s.currAncestors[i][:0])
			if err != nil {
				return nil, err
			}
			// recalc key
			s.currKeys[i] = ancestorFromRoot(s.currAncestors[i], s.joinIdx)
		}
	}
	// we need to call Next() in a loop until we reach or exceed the given ID
	// the Next() call basically gives us a match that is aligned correctly, but
	// if joinIdx < I, we can have multiple matches for the same joinIdx ancestor
	// and they may be < ID, so we need to loop
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
	order []*search.DocumentMatch // queue of DocumentMatch
}

func NewCoalesceQueue() *CoalesceQueue {
	cq := &CoalesceQueue{
		order: make([]*search.DocumentMatch, 0),
	}
	return cq
}

// Enqueue appends the given DocumentMatch to the queue. Coalescing of duplicates
// is deferred until Dequeue, after Finalize has sorted items by IndexInternalID.
func (cq *CoalesceQueue) Enqueue(it *search.DocumentMatch) {
	// append to order slice (this is a stack)
	cq.order = append(cq.order, it)
}

// Finalize prepares the queue for dequeue operations by sorting the items based on
// their IndexInternalID values. This MUST be called before any Dequeue operations,
// and after all Enqueue operations are complete. The sort is done in descending order
// so that dequeueing will basically be popping from the end of the slice, allowing for
// slice reuse.
func (cq *CoalesceQueue) Finalize() {
	slices.SortFunc(cq.order, func(a, b *search.DocumentMatch) int {
		return b.IndexInternalID.Compare(a.IndexInternalID)
	})
}

// Dequeue removes and returns the next DocumentMatch in sorted order, merging any
// consecutive duplicates. Merged items are recycled via ctx.DocumentMatchPool.
// Returns nil when the queue is empty.
func (cq *CoalesceQueue) Dequeue(ctx *search.SearchContext) *search.DocumentMatch {
	if cq.Len() == 0 {
		return nil
	}

	// pop from end of slice
	rv := cq.order[len(cq.order)-1]
	cq.order = cq.order[:len(cq.order)-1]

	// merge duplicates
	for cq.Len() > 0 {
		// peek at next item
		next := cq.order[len(cq.order)-1]
		if !rv.IndexInternalID.Equals(next.IndexInternalID) {
			// different ID, stop merging
			break
		}
		// pop the next item
		cq.order = cq.order[:len(cq.order)-1]
		// same ID, merge
		rv.Score += next.Score
		rv.Expl = rv.Expl.MergeWith(next.Expl)
		rv.FieldTermLocations = search.MergeFieldTermLocationsFromMatch(
			rv.FieldTermLocations, next)
		// recycle the merged item
		ctx.DocumentMatchPool.Put(next)
	}

	return rv
}

// Len returns the number of DocumentMatch items currently in the queue.
func (cq *CoalesceQueue) Len() int {
	return len(cq.order)
}
