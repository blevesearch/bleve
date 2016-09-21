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
	"math"
	"sort"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/scorers"
)

type ConjunctionSearcher struct {
	indexReader index.IndexReader
	searchers   OrderedSearcherList
	queryNorm   float64
	currs       []*search.DocumentMatch
	currentID   index.IndexInternalID
	scorer      *scorers.ConjunctionQueryScorer
	initialized bool
	explain     bool
}

func NewConjunctionSearcher(indexReader index.IndexReader, qsearchers []search.Searcher, explain bool) (*ConjunctionSearcher, error) {
	// build the downstream searchers
	searchers := make(OrderedSearcherList, len(qsearchers))
	for i, searcher := range qsearchers {
		searchers[i] = searcher
	}
	// sort the searchers
	sort.Sort(searchers)
	// build our searcher
	rv := ConjunctionSearcher{
		indexReader: indexReader,
		explain:     explain,
		searchers:   searchers,
		currs:       make([]*search.DocumentMatch, len(searchers)),
		scorer:      scorers.NewConjunctionQueryScorer(explain),
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
	// finally tell all the downstream searchers the norm
	for _, termSearcher := range s.searchers {
		termSearcher.SetQueryNorm(s.queryNorm)
	}
}

func (s *ConjunctionSearcher) initSearchers(ctx *search.SearchContext) error {
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

	if len(s.currs) > 0 {
		if s.currs[0] != nil {
			s.currentID = s.currs[0].IndexInternalID
		} else {
			s.currentID = nil
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

func (s *ConjunctionSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}
	var rv *search.DocumentMatch
	var err error
OUTER:
	for s.currentID != nil {
		for i, termSearcher := range s.searchers {
			if s.currs[i] == nil {
				s.currentID = nil
				continue OUTER
			}

			cmp := s.currentID.Compare(s.currs[i].IndexInternalID)
			if cmp != 0 {
				if cmp < 0 {
					s.currentID = s.currs[i].IndexInternalID
					continue OUTER
				}
				// this reader is less than the currentID, try to advance
				if s.currs[i] != nil {
					ctx.DocumentMatchPool.Put(s.currs[i])
				}
				s.currs[i], err = termSearcher.Advance(ctx, s.currentID)
				if err != nil {
					return nil, err
				}
				if s.currs[i] == nil {
					s.currentID = nil
					continue OUTER
				}
				if !s.currs[i].IndexInternalID.Equals(s.currentID) {
					// we just advanced, so it doesn't match, it must be greater
					// no need to call next
					s.currentID = s.currs[i].IndexInternalID
					continue OUTER
				}
			}
		}
		// if we get here, a doc matched all readers, sum the score and add it
		rv = s.scorer.Score(ctx, s.currs)

		// we know all the searchers are pointing at the same thing
		// so they all need to be advanced
		for i, termSearcher := range s.searchers {
			if s.currs[i] != rv {
				ctx.DocumentMatchPool.Put(s.currs[i])
			}
			s.currs[i], err = termSearcher.Next(ctx)
			if err != nil {
				return nil, err
			}
		}

		if s.currs[0] == nil {
			s.currentID = nil
		} else {
			s.currentID = s.currs[0].IndexInternalID
		}

		// don't continue now, wait for the next call to Next()
		break
	}
	return rv, nil
}

func (s *ConjunctionSearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}
	var err error
	for i, searcher := range s.searchers {
		if s.currs[i] != nil {
			ctx.DocumentMatchPool.Put(s.currs[i])
		}
		s.currs[i], err = searcher.Advance(ctx, ID)
		if err != nil {
			return nil, err
		}
	}
	s.currentID = ID
	return s.Next(ctx)
}

func (s *ConjunctionSearcher) Count() uint64 {
	// for now return a worst case
	var sum uint64
	for _, searcher := range s.searchers {
		sum += searcher.Count()
	}
	return sum
}

func (s *ConjunctionSearcher) Close() error {
	for _, searcher := range s.searchers {
		err := searcher.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *ConjunctionSearcher) Min() int {
	return 0
}

func (s *ConjunctionSearcher) DocumentMatchPoolSize() int {
	rv := len(s.currs)
	for _, s := range s.searchers {
		rv += s.DocumentMatchPoolSize()
	}
	return rv
}
