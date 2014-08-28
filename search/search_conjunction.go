//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import (
	"math"
	"sort"

	"github.com/blevesearch/bleve/index"
)

type ConjunctionSearcher struct {
	initialized bool
	index       index.Index
	searchers   OrderedSearcherList
	explain     bool
	queryNorm   float64
	currs       []*DocumentMatch
	currentId   string
	scorer      *ConjunctionQueryScorer
}

func NewConjunctionSearcher(index index.Index, qsearchers []Searcher, explain bool) (*ConjunctionSearcher, error) {
	// build the downstream searchres
	searchers := make(OrderedSearcherList, len(qsearchers))
	for i, searcher := range qsearchers {
		searchers[i] = searcher
	}
	// sort the searchers
	sort.Sort(searchers)
	// build our searcher
	rv := ConjunctionSearcher{
		index:     index,
		explain:   explain,
		searchers: searchers,
		currs:     make([]*DocumentMatch, len(searchers)),
		scorer:    NewConjunctionQueryScorer(explain),
	}
	rv.computeQueryNorm()
	return &rv, nil
}

func (s *ConjunctionSearcher) computeQueryNorm() {
	// first calculate sum of squared weights
	sumOfSquaredWeights := 0.0
	for _, termSearcher := range s.searchers {
		sumOfSquaredWeights += termSearcher.Weight()
	}
	// now compute query norm from this
	s.queryNorm = 1.0 / math.Sqrt(sumOfSquaredWeights)
	// finally tell all the downsteam searchers the norm
	for _, termSearcher := range s.searchers {
		termSearcher.SetQueryNorm(s.queryNorm)
	}
}

func (s *ConjunctionSearcher) initSearchers() error {
	var err error
	// get all searchers pointing at their first match
	for i, termSearcher := range s.searchers {
		s.currs[i], err = termSearcher.Next()
		if err != nil {
			return err
		}
	}

	if len(s.currs) > 0 {
		if s.currs[0] != nil {
			s.currentId = s.currs[0].ID
		} else {
			s.currentId = ""
		}
	}

	s.initialized = true
	return nil
}

func (s *ConjunctionSearcher) Weight() float64 {
	var rv float64
	for _, searcher := range s.searchers {
		rv += searcher.Weight()
	}
	return rv
}

func (s *ConjunctionSearcher) SetQueryNorm(qnorm float64) {
	for _, searcher := range s.searchers {
		searcher.SetQueryNorm(qnorm)
	}
}

func (s *ConjunctionSearcher) Next() (*DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers()
		if err != nil {
			return nil, err
		}
	}
	var rv *DocumentMatch
	var err error
OUTER:
	for s.currentId != "" {
		for i, termSearcher := range s.searchers {
			if s.currs[i] != nil && s.currs[i].ID != s.currentId {
				// this reader doesn't have the currentId, try to advance
				s.currs[i], err = termSearcher.Advance(s.currentId)
				if err != nil {
					return nil, err
				}
				if s.currs[i] == nil {
					s.currentId = ""
					continue OUTER
				}
				if s.currs[i].ID != s.currentId {
					// we just advanced, so it doesn't match, it must be greater
					// no need to call next
					s.currentId = s.currs[i].ID
					continue OUTER
				}
			} else if s.currs[i] == nil {
				s.currentId = ""
				continue OUTER
			}
		}
		// if we get here, a doc matched all readers, sum the score and add it
		rv = s.scorer.Score(s.currs)

		// prepare for next entry
		s.currs[0], err = s.searchers[0].Next()
		if err != nil {
			return nil, err
		}
		if s.currs[0] == nil {
			s.currentId = ""
		} else {
			s.currentId = s.currs[0].ID
		}
		// don't continue now, wait for next call the Next()
		break
	}
	return rv, nil
}

func (s *ConjunctionSearcher) Advance(ID string) (*DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers()
		if err != nil {
			return nil, err
		}
	}
	var err error
	for i, searcher := range s.searchers {
		s.currs[i], err = searcher.Advance(ID)
		if err != nil {
			return nil, err
		}
	}
	s.currentId = ID
	return s.Next()
}

func (s *ConjunctionSearcher) Count() uint64 {
	// for now return a worst case
	var sum uint64 = 0
	for _, searcher := range s.searchers {
		sum += searcher.Count()
	}
	return sum
}

func (s *ConjunctionSearcher) Close() {
	for _, searcher := range s.searchers {
		searcher.Close()
	}
}
