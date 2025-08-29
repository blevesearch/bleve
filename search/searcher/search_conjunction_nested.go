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
	maxIDIdx      int
	scorer        *scorer.NestedConjunctionQueryScorer
	initialized   bool
	options       search.SearcherOptions
}

func NewNestedConjunctionSearcher(ctx context.Context, indexReader index.IndexReader,
	searchers []search.Searcher, options search.SearcherOptions) (search.Searcher, error) {

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
	}
	rv.computeQueryNorm()

	return &rv, nil
}

func (s *NestedConjunctionSearcher) initSearchers(ctx *search.SearchContext) error {
	var err error
	// get all searchers pointing at their first match
	for i, searcher := range s.searchers {
		if s.currs[i] != nil {
			ctx.DocumentMatchPool.Put(s.currs[i])
		}
		s.currs[i], err = searcher.Next(ctx)
		if err != nil {
			return err
		}
	}
	s.initialized = true

	return nil
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
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}

	var err error
	// pick maxID among currs
	s.maxIDIdx = 0
	for i := 0; i < len(s.currs); i++ {
		if s.currs[i] == nil {
			return nil, nil
		}
		s.currAncestors[i], err = s.nestedReader.Ancestors(s.currs[i].IndexInternalID)
		if err != nil {
			return nil, err
		}
		if s.currs[i].IndexInternalID.Compare(s.currs[s.maxIDIdx].IndexInternalID) > 0 {
			s.maxIDIdx = i
		}
	}
OUTER:
	for {
		// try to align others
		for i := 0; i < len(s.currs); i++ {
			if i == s.maxIDIdx {
				continue
			}
			for {
				// case 1: suffix already matches → exit with matched
				if s.suffixJoinOK(s.currAncestors[i], s.currAncestors[s.maxIDIdx]) {
					break
				}

				// case 2: docID is already > max → overshoot, stop and rewind
				if s.currs[i].IndexInternalID.Compare(s.currs[s.maxIDIdx].IndexInternalID) > 0 {
					s.maxIDIdx = i
					continue OUTER
				}

				// case 3: need to advance this searcher
				var err error
				if s.currs[i] != nil {
					ctx.DocumentMatchPool.Put(s.currs[i])
				}
				s.currs[i], err = s.searchers[i].Next(ctx)
				if err != nil {
					return nil, err
				}
				if s.currs[i] == nil {
					return nil, nil // exhausted
				}
				s.currAncestors[i], err = s.nestedReader.Ancestors(s.currs[i].IndexInternalID)
				if err != nil {
					return nil, err
				}
			}
			// now we have a guaranteed suffix match
			// move on to next searcher
		}
		// reaching this point means that we have reached a case where we have
		// all ancestry paths having the same suffix match, means we can emit out the
		// LCA which is the left most element of the smallest Ancestor path
		// and then we Next() all searchers except the one at maxIDIdx and return
		// the LCA as the document match

		rv := s.scorer.Score(ctx, s.currs, s.currAncestors)
		// advance all searchers except the one at maxIDIdx
		for i := 0; i < len(s.currs); i++ {
			if i == s.maxIDIdx {
				continue
			}
			if s.currs[i] != rv {
				ctx.DocumentMatchPool.Put(s.currs[i])
			}
			var err error
			s.currs[i], err = s.searchers[i].Next(ctx)
			if err != nil {
				return nil, err
			}
		}
		return rv, nil
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

// suffixJoinOK checks if one ancestry path is a suffix of the other
func (s *NestedConjunctionSearcher) suffixJoinOK(ancA, ancB []index.IndexInternalID) bool {
	lenA := len(ancA)
	lenB := len(ancB)
	if lenA == 0 || lenB == 0 {
		return false
	}

	// compare last minLen elements
	minLen := min(lenB, lenA)

	offsetA := lenA - minLen
	offsetB := lenB - minLen

	for i := range minLen {
		if !ancA[offsetA+i].Equals(ancB[offsetB+i]) {
			return false
		}
	}
	return true
}
