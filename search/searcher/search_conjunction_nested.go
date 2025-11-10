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
	"context"
	"fmt"
	"math"
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/scorer"
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
	currAncestors [][]index.IndexInternalID
	pivotIDx      int
	scorer        *scorer.NestedConjunctionQueryScorer
	initialized   bool
	joinIdx       int
	options       search.SearcherOptions
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
		currAncestors: make([][]index.IndexInternalID, len(searchers)),
		scorer:        scorer.NewNestedConjunctionQueryScorer(options),
		joinIdx:       joinIdx,
	}
	rv.computeQueryNorm()

	return &rv, nil
}

// getTargetAncestor returns the appropriate ancestor ID for the given joinIdx
// if the ancestry chain is shallower than joinIdx, it returns the deepest ancestor
// otherwise it returns the ancestor at joinIdx level from the top-most ancestor
func getTargetAncestor(ancestors []index.IndexInternalID, joinIdx int) index.IndexInternalID {
	if len(ancestors) > joinIdx {
		return ancestors[len(ancestors)-joinIdx-1]
	}
	return ancestors[len(ancestors)-1]
}

func (s *NestedConjunctionSearcher) initSearchers(ctx *search.SearchContext) (bool, error) {
	var err error
	// get all searchers pointing at their first match
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
		s.currAncestors[i], err = s.nestedReader.Ancestors(s.currs[i].IndexInternalID)
		if err != nil {
			return false, err
		}
	}
	// scan the ancestry chains for all searchers to get the pivotIDx
	// the pivot will be the searcher with the longest ancestry chain
	// if there are multiple with the same length, pick the one with
	// the highest docID
	s.pivotIDx = 0
	pivotLength := len(s.currAncestors[0])
	for i := 1; i < len(s.searchers); i++ {
		if len(s.currAncestors[i]) > pivotLength {
			s.pivotIDx = i
			pivotLength = len(s.currAncestors[i])
		} else if len(s.currAncestors[i]) == pivotLength {
			// if same length, pick the one with the highest docID
			if s.currs[i].IndexInternalID.Compare(s.currs[s.pivotIDx].IndexInternalID) > 0 {
				s.pivotIDx = i
			}
		}
	}
	s.initialized = true
	return false, nil
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
	sizeInBytes := reflectStaticSizeNestedConjunctionSearcher + size.SizeOfPtr +
		s.scorer.Size()

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
	if !s.initialized {
		exhausted, err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
		if exhausted {
			return nil, nil
		}
	}
	// we have the pivot searcher, now try to align all the others to it, using the racecar algorithm,
	// basically - the idea is simple - we first check if the pivot searcher's indexInternalID
	// is behind any of the other searchers, and if so, we are sure that the pivot searcher
	// cannot be part of a match, so we advance it to the maximum of the other searchers.
	// Now once the pivot searcher is ahead of all the other searchers, we advance all the other
	// searchers to the corresponding ancestor of the pivot searcher, if all of them align on the correct
	// ancestor, we have a match, otherwise we repeat the process.
	for {
		pivotSearcher := s.searchers[s.pivotIDx]
		pivotDM := s.currs[s.pivotIDx]
		if pivotDM == nil {
			// one of the searchers is exhausted, so we are done
			return nil, nil
		}
		pivotAncestors := s.currAncestors[s.pivotIDx]
		pivotID := pivotDM.IndexInternalID
		// first, make sure the pivot is ahead of all the other searchers
		// we do this by getting the max of all the other searchers' IDs
		// at their respective target ancestors
		// and if the pivot is behind that, we advance it to that
		maxID := getTargetAncestor(pivotAncestors, s.joinIdx)
		for i := 0; i < len(s.searchers); i++ {
			if i == s.pivotIDx {
				// skip the pivot itself
				continue
			}
			curr := s.currs[i]
			if curr == nil {
				// one of the searchers is exhausted, so we are done
				return nil, nil
			}
			targetAncestor := getTargetAncestor(s.currAncestors[i], s.joinIdx)
			// now compare curr's target ancestor with maxID
			if targetAncestor.Compare(maxID) > 0 {
				maxID = targetAncestor
			}
		}
		if maxID.Compare(pivotID) > 0 {
			var err error
			// pivot is behind, so advance it
			ctx.DocumentMatchPool.Put(pivotDM)
			s.currs[s.pivotIDx], err = pivotSearcher.Advance(ctx, maxID)
			if err != nil {
				return nil, err
			}
			if s.currs[s.pivotIDx] == nil {
				// one of the searchers is exhausted, so we are done
				return nil, nil
			}
			// recalc ancestors
			s.currAncestors[s.pivotIDx], err = s.nestedReader.Ancestors(s.currs[s.pivotIDx].IndexInternalID)
			if err != nil {
				return nil, err
			}
			// now restart the whole process
			continue
		}
		// at this point, we know the pivot is ahead of all the other searchers
		// now try to align all the other searchers to the pivot's ancestry
		// we do this by advancing each searcher to the corresponding ancestor
		// of the pivot, with searchers with insufficient depth being advanced
		// to the corresponding document ID in the pivot's ancestry and
		// and the searchers with sufficient depth being advanced to the
		// ancestor at joinIdx level  once that is done we check if all the
		// searchers are aligned if they are, we have a match, otherwise we have a
		// scenario where one or more searchers have advanced beyond the pivot, so
		// we need to restart the whole process where we have to find the new maxID
		// and advance the pivot as done above
		allAligned := true
		for i := 0; i < len(s.searchers); i++ {
			if i == s.pivotIDx {
				// skip the pivot itself
				continue
			}
			curr := s.currs[i]
			if curr == nil {
				// one of the searchers is exhausted, so we are done
				return nil, nil
			}
			// try to align curr to the pivot's ancestry by advancing the
			// searcher to the corresponding ancestor of the pivot
			var targetAncestor index.IndexInternalID
			if len(s.currAncestors[i]) > s.joinIdx {
				// this searcher has sufficient depth, so use the pivot's ancestor at joinIdx
				targetAncestor = pivotAncestors[len(pivotAncestors)-s.joinIdx-1]
			} else {
				// this searcher does not have sufficient depth, so use the pivot's
				// ancestor at the searcher's max depth
				targetAncestor = pivotAncestors[len(s.currAncestors[i])-1]
			}
			if curr.IndexInternalID.Compare(targetAncestor) < 0 {
				var err error
				ctx.DocumentMatchPool.Put(curr)
				s.currs[i], err = s.searchers[i].Advance(ctx, targetAncestor)
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
			}
			// now check if we are aligned
			currID := getTargetAncestor(s.currAncestors[i], s.joinIdx)
			if currID.Compare(targetAncestor) != 0 {
				allAligned = false
			}
		}
		if allAligned {
			// we have a match, so we can build the resulting DocumentMatch
			// we do this by delegating to the scorer, which will pick the lowest
			// common ancestor (LCA) and merge all the constituents into it
			dm, err := s.scorer.Score(ctx, s.currs, s.currAncestors, s.joinIdx)
			if err != nil {
				return nil, err
			}
			// now advance the pivot searcher to get ready for the next call
			ctx.DocumentMatchPool.Put(pivotDM)
			s.currs[s.pivotIDx], err = pivotSearcher.Next(ctx)
			if err != nil {
				return nil, err
			}
			if s.currs[s.pivotIDx] != nil {
				s.currAncestors[s.pivotIDx], err = s.nestedReader.Ancestors(s.currs[s.pivotIDx].IndexInternalID)
				if err != nil {
					return nil, err
				}
			}
			// return the match we have
			return dm, nil
		}
	}
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
