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

	"github.com/blevesearch/bleve/index"
)

type BooleanSearcher struct {
	initialized     bool
	index           index.Index
	mustSearcher    Searcher
	shouldSearcher  Searcher
	mustNotSearcher Searcher
	queryNorm       float64
	currMust        *DocumentMatch
	currShould      *DocumentMatch
	currMustNot     *DocumentMatch
	currentId       string
	min             uint64
	scorer          *ConjunctionQueryScorer
}

func NewBooleanSearcher(index index.Index, mustSearcher Searcher, shouldSearcher Searcher, mustNotSearcher Searcher, explain bool) (*BooleanSearcher, error) {
	// build our searcher
	rv := BooleanSearcher{
		index:           index,
		mustSearcher:    mustSearcher,
		shouldSearcher:  shouldSearcher,
		mustNotSearcher: mustNotSearcher,
		scorer:          NewConjunctionQueryScorer(explain),
	}
	rv.computeQueryNorm()
	return &rv, nil
}

func (s *BooleanSearcher) computeQueryNorm() {
	// first calculate sum of squared weights
	sumOfSquaredWeights := 0.0
	if s.mustSearcher != nil {
		sumOfSquaredWeights += s.mustSearcher.Weight()
	}
	if s.shouldSearcher != nil {
		sumOfSquaredWeights += s.shouldSearcher.Weight()
	}

	// now compute query norm from this
	s.queryNorm = 1.0 / math.Sqrt(sumOfSquaredWeights)
	// finally tell all the downsteam searchers the norm
	if s.mustSearcher != nil {
		s.mustSearcher.SetQueryNorm(s.queryNorm)
	}
	if s.shouldSearcher != nil {
		s.shouldSearcher.SetQueryNorm(s.queryNorm)
	}
}

func (s *BooleanSearcher) initSearchers() error {
	var err error
	// get all searchers pointing at their first match
	if s.mustSearcher != nil {
		s.currMust, err = s.mustSearcher.Next()
		if err != nil {
			return err
		}
	}

	if s.shouldSearcher != nil {
		s.currShould, err = s.shouldSearcher.Next()
		if err != nil {
			return err
		}
	}

	if s.mustNotSearcher != nil {
		s.currMustNot, err = s.mustNotSearcher.Next()
		if err != nil {
			return err
		}
	}

	if s.mustSearcher != nil && s.currMust != nil {
		s.currentId = s.currMust.ID
	} else if s.mustSearcher == nil && s.currShould != nil {
		s.currentId = s.currShould.ID
	} else {
		s.currentId = ""
	}

	s.initialized = true
	return nil
}

func (s *BooleanSearcher) advanceNextMust() error {
	var err error

	if s.mustSearcher != nil {
		s.currMust, err = s.mustSearcher.Next()
		if err != nil {
			return err
		}
	} else if s.mustSearcher == nil {
		s.currShould, err = s.shouldSearcher.Next()
		if err != nil {
			return err
		}
	}

	if s.mustSearcher != nil && s.currMust != nil {
		s.currentId = s.currMust.ID
	} else if s.mustSearcher == nil && s.currShould != nil {
		s.currentId = s.currShould.ID
	} else {
		s.currentId = ""
	}
	return nil
}

func (s *BooleanSearcher) Weight() float64 {
	var rv float64
	if s.mustSearcher != nil {
		rv += s.mustSearcher.Weight()
	}
	if s.shouldSearcher != nil {
		rv += s.shouldSearcher.Weight()
	}

	return rv
}

func (s *BooleanSearcher) SetQueryNorm(qnorm float64) {
	if s.mustSearcher != nil {
		s.mustSearcher.SetQueryNorm(qnorm)
	}
	if s.shouldSearcher != nil {
		s.shouldSearcher.SetQueryNorm(qnorm)
	}
}

func (s *BooleanSearcher) Next() (*DocumentMatch, error) {

	if !s.initialized {
		err := s.initSearchers()
		if err != nil {
			return nil, err
		}
	}

	var err error
	var rv *DocumentMatch

	for s.currentId != "" {
		if s.currMustNot != nil && s.currMustNot.ID < s.currentId {
			// advance must not searcher to our candidate entry
			s.currMustNot, err = s.mustNotSearcher.Advance(s.currentId)
			if err != nil {
				return nil, err
			}
			if s.currMustNot != nil && s.currMustNot.ID == s.currentId {
				// the candidate is excluded
				s.advanceNextMust()
				continue
			}
		} else if s.currMustNot != nil && s.currMustNot.ID == s.currentId {
			// the candidate is excluded
			s.advanceNextMust()
			continue
		}

		if s.currShould != nil && s.currShould.ID < s.currentId {
			// advance should searcher to our candidate entry
			s.currShould, err = s.shouldSearcher.Advance(s.currentId)
			if err != nil {
				return nil, err
			}
			if s.currShould != nil && s.currShould.ID == s.currentId {
				// score bonus matches should
				cons := []*DocumentMatch{}
				if s.currMust != nil {
					cons = append(cons, s.currMust)
				}
				cons = append(cons, s.currShould)
				rv = s.scorer.Score(cons)
				s.advanceNextMust()
				break
			} else if s.shouldSearcher.Min() == 0 {
				// match is OK anyway
				rv = s.scorer.Score([]*DocumentMatch{s.currMust})
				s.advanceNextMust()
				break
			}
		} else if s.currShould != nil && s.currShould.ID == s.currentId {
			// score bonus matches should
			cons := []*DocumentMatch{}
			if s.currMust != nil {
				cons = append(cons, s.currMust)
			}
			cons = append(cons, s.currShould)
			rv = s.scorer.Score(cons)
			s.advanceNextMust()
			break
		} else if s.shouldSearcher == nil || s.shouldSearcher.Min() == 0 {
			// match is OK anyway
			rv = s.scorer.Score([]*DocumentMatch{s.currMust})
			s.advanceNextMust()
			break
		}

		s.advanceNextMust()
	}
	return rv, nil
}

func (s *BooleanSearcher) Advance(ID string) (*DocumentMatch, error) {

	if !s.initialized {
		err := s.initSearchers()
		if err != nil {
			return nil, err
		}
	}

	var err error
	if s.mustSearcher != nil {
		s.currMust, err = s.mustSearcher.Advance(ID)
		if err != nil {
			return nil, err
		}
	}
	if s.shouldSearcher != nil {
		s.currShould, err = s.shouldSearcher.Advance(ID)
		if err != nil {
			return nil, err
		}
	}
	if s.mustNotSearcher != nil {
		s.currMustNot, err = s.mustNotSearcher.Advance(ID)
		if err != nil {
			return nil, err
		}
	}

	if s.mustSearcher != nil && s.currMust != nil {
		s.currentId = s.currMust.ID
	} else if s.mustSearcher == nil && s.currShould != nil {
		s.currentId = s.currShould.ID
	} else {
		s.currentId = ""
	}

	return s.Next()
}

func (s *BooleanSearcher) Count() uint64 {

	// for now return a worst case
	var sum uint64 = 0
	if s.mustSearcher != nil {
		sum += s.mustSearcher.Count()
	}
	if s.shouldSearcher != nil {
		sum += s.shouldSearcher.Count()
	}
	return sum
}

func (s *BooleanSearcher) Close() {
	if s.mustSearcher != nil {
		s.mustSearcher.Close()
	}
	if s.shouldSearcher != nil {
		s.shouldSearcher.Close()
	}
	if s.mustNotSearcher != nil {
		s.mustNotSearcher.Close()
	}
}

func (s *BooleanSearcher) Min() int {
	return 0
}
