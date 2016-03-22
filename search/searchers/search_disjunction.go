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
	initialized bool
	indexReader index.IndexReader
	searchers   OrderedSearcherList
	queryNorm   float64
	currs       []*search.DocumentMatch
	currentID   string
	scorer      *scorers.DisjunctionQueryScorer
	min         float64
}

func NewDisjunctionSearcher(indexReader index.IndexReader, qsearchers []search.Searcher, min float64, explain bool) (*DisjunctionSearcher, error) {
	if DisjunctionMaxClauseCount != 0 && len(qsearchers) > DisjunctionMaxClauseCount {
		return nil, fmt.Errorf("TooManyClauses[maxClauseCount is set to %d]", DisjunctionMaxClauseCount)
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
		indexReader: indexReader,
		searchers:   searchers,
		currs:       make([]*search.DocumentMatch, len(searchers)),
		scorer:      scorers.NewDisjunctionQueryScorer(explain),
		min:         min,
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

func (s *DisjunctionSearcher) initSearchers() error {
	var err error
	// get all searchers pointing at their first match
	for i, termSearcher := range s.searchers {
		s.currs[i], err = termSearcher.Next()
		if err != nil {
			return err
		}
	}

	s.currentID = s.nextSmallestID()
	s.initialized = true
	return nil
}

func (s *DisjunctionSearcher) nextSmallestID() string {
	rv := ""
	for _, curr := range s.currs {
		if curr != nil && (curr.ID < rv || rv == "") {
			rv = curr.ID
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

func (s *DisjunctionSearcher) Next() (*search.DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers()
		if err != nil {
			return nil, err
		}
	}
	var err error
	var rv *search.DocumentMatch
	matching := make([]*search.DocumentMatch, 0, len(s.searchers))

	found := false
	for !found && s.currentID != "" {
		for _, curr := range s.currs {
			if curr != nil && curr.ID == s.currentID {
				matching = append(matching, curr)
			}
		}

		if len(matching) >= int(s.min) {
			found = true
			// score this match
			rv = s.scorer.Score(matching, len(matching), len(s.searchers))
		}

		// reset matching
		matching = make([]*search.DocumentMatch, 0)
		// invoke next on all the matching searchers
		for i, curr := range s.currs {
			if curr != nil && curr.ID == s.currentID {
				searcher := s.searchers[i]
				s.currs[i], err = searcher.Next()
				if err != nil {
					return nil, err
				}
			}
		}
		s.currentID = s.nextSmallestID()
	}
	return rv, nil
}

func (s *DisjunctionSearcher) Advance(ID string) (*search.DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers()
		if err != nil {
			return nil, err
		}
	}
	// get all searchers pointing at their first match
	var err error
	for i, termSearcher := range s.searchers {
		s.currs[i], err = termSearcher.Advance(ID)
		if err != nil {
			return nil, err
		}
	}

	s.currentID = s.nextSmallestID()

	return s.Next()
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
