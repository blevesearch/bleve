//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package searchers

import (
	"fmt"
	"math"
	"sort"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/scorers"
)

// DisjunctionMaxClauseCount is a compile time setting that applications can
// adjust to non-zero value to cause the DisjunctionSearcher to return an
// error instead of exeucting searches when the size exceeds this value.
var DisjunctionMaxClauseCount = 0

type DisjunctionSearcher struct {
	indexReader  index.IndexReader
	searchers    OrderedSearcherList
	queryNorm    float64
	currs        []*search.DocumentMatch
	currentID    index.IndexInternalID
	scorer       *scorers.DisjunctionQueryScorer
	min          float64
	matching     []*search.DocumentMatch
	matchingIdxs []int
	initialized  bool
}

func tooManyClauses(count int) bool {
	if DisjunctionMaxClauseCount != 0 && count > DisjunctionMaxClauseCount {
		return true
	}
	return false
}

func tooManyClausesErr() error {
	return fmt.Errorf("TooManyClauses[maxClauseCount is set to %d]", DisjunctionMaxClauseCount)
}

func NewDisjunctionSearcher(indexReader index.IndexReader, qsearchers []search.Searcher, min float64, explain bool) (*DisjunctionSearcher, error) {
	if tooManyClauses(len(qsearchers)) {
		return nil, tooManyClausesErr()
	}
	// build the downstream searchers
	searchers := make(OrderedSearcherList, len(qsearchers))
	for i, searcher := range qsearchers {
		searchers[i] = searcher
	}
	// sort the searchers
	sort.Sort(sort.Reverse(searchers))
	// build our searcher
	rv := DisjunctionSearcher{
		indexReader:  indexReader,
		searchers:    searchers,
		currs:        make([]*search.DocumentMatch, len(searchers)),
		scorer:       scorers.NewDisjunctionQueryScorer(explain),
		min:          min,
		matching:     make([]*search.DocumentMatch, len(searchers)),
		matchingIdxs: make([]int, len(searchers)),
	}
	rv.computeQueryNorm()
	return &rv, nil
}

func (s *DisjunctionSearcher) computeQueryNorm() {
	// first calculate sum of squared weights
	sumOfSquaredWeights := 0.0
	for _, termSearcher := range s.searchers {
		sumOfSquaredWeights += termSearcher.Weight()
	}
	// now compute query norm from this
	s.queryNorm = 1.0 / math.Sqrt(sumOfSquaredWeights)
	// finally tell all the downstream searchers the norm
	for _, termSearcher := range s.searchers {
		termSearcher.SetQueryNorm(s.queryNorm)
	}
}

func (s *DisjunctionSearcher) initSearchers(ctx *search.SearchContext) error {
	var err error
	// get all searchers pointing at their first match
	for i, termSearcher := range s.searchers {
		if s.currs[i] != nil {
			ctx.DocumentMatchPool.Put(s.currs[i])
		}
		s.currs[i], err = termSearcher.Next(ctx)
		if err != nil {
			return err
		}
	}

	s.currentID = s.nextSmallestID()
	s.initialized = true
	return nil
}

func (s *DisjunctionSearcher) nextSmallestID() index.IndexInternalID {
	var rv index.IndexInternalID
	for _, curr := range s.currs {
		if curr != nil && (curr.IndexInternalID.Compare(rv) < 0 || rv == nil) {
			rv = curr.IndexInternalID
		}
	}
	return rv
}

func (s *DisjunctionSearcher) Weight() float64 {
	var rv float64
	for _, searcher := range s.searchers {
		rv += searcher.Weight()
	}
	return rv
}

func (s *DisjunctionSearcher) SetQueryNorm(qnorm float64) {
	for _, searcher := range s.searchers {
		searcher.SetQueryNorm(qnorm)
	}
}

func (s *DisjunctionSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	ctx.LowScoreFilter = 0

	if !s.initialized {
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}
	var err error
	var rv *search.DocumentMatch

	found := false
	for !found && s.currentID != nil {
		matching := s.matching[:0]
		matchingIdxs := s.matchingIdxs[:0]
		for i, curr := range s.currs {
			if curr != nil && curr.IndexInternalID.Equals(s.currentID) {
				matching = append(matching, curr)
				matchingIdxs = append(matchingIdxs, i)
			}
		}

		if len(matching) >= int(s.min) {
			found = true
			// score this match
			rv = s.scorer.Score(ctx, matching, len(matching), len(s.searchers))
		}

		// invoke next on all the matching searchers
		for _, i := range matchingIdxs {
			searcher := s.searchers[i]
			if s.currs[i] != rv {
				ctx.DocumentMatchPool.Put(s.currs[i])
			}
			s.currs[i], err = searcher.Next(ctx)
			if err != nil {
				return nil, err
			}
		}
		s.currentID = s.nextSmallestID()
	}
	return rv, nil
}

func (s *DisjunctionSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}
	// get all searchers pointing at their first match
	var err error
	for i, termSearcher := range s.searchers {
		if s.currs[i] != nil {
			ctx.DocumentMatchPool.Put(s.currs[i])
		}
		s.currs[i], err = termSearcher.Advance(ctx, ID)
		if err != nil {
			return nil, err
		}
	}

	s.currentID = s.nextSmallestID()

	return s.Next(ctx)
}

func (s *DisjunctionSearcher) Count() uint64 {
	// for now return a worst case
	var sum uint64
	for _, searcher := range s.searchers {
		sum += searcher.Count()
	}
	return sum
}

func (s *DisjunctionSearcher) Close() error {
	for _, searcher := range s.searchers {
		err := searcher.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *DisjunctionSearcher) Min() int {
	return int(s.min) // FIXME just make this an int
}

func (s *DisjunctionSearcher) DocumentMatchPoolSize() int {
	rv := len(s.currs)
	for _, s := range s.searchers {
		rv += s.DocumentMatchPoolSize()
	}
	return rv
}
