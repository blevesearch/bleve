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

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/scorers"
)

type BooleanSearcher struct {
	initialized     bool
	indexReader     index.IndexReader
	mustSearcher    search.Searcher
	shouldSearcher  search.Searcher
	mustNotSearcher search.Searcher
	queryNorm       float64
	currMust        *search.DocumentMatch
	currShould      *search.DocumentMatch
	currMustNot     *search.DocumentMatch
	currentID       string
	min             uint64
	scorer          *scorers.ConjunctionQueryScorer
}

func NewBooleanSearcher(indexReader index.IndexReader, mustSearcher search.Searcher, shouldSearcher search.Searcher, mustNotSearcher search.Searcher, explain bool) (*BooleanSearcher, error) {
	// build our searcher
	rv := BooleanSearcher{
		indexReader:     indexReader,
		mustSearcher:    mustSearcher,
		shouldSearcher:  shouldSearcher,
		mustNotSearcher: mustNotSearcher,
		scorer:          scorers.NewConjunctionQueryScorer(explain),
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
	// finally tell all the downstream searchers the norm
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
		s.currentID = s.currMust.ID
	} else if s.mustSearcher == nil && s.currShould != nil {
		s.currentID = s.currShould.ID
	} else {
		s.currentID = ""
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
		s.currentID = s.currMust.ID
	} else if s.mustSearcher == nil && s.currShould != nil {
		s.currentID = s.currShould.ID
	} else {
		s.currentID = ""
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

func (s *BooleanSearcher) Next() (*search.DocumentMatch, error) {

	if !s.initialized {
		err := s.initSearchers()
		if err != nil {
			return nil, err
		}
	}

	var err error
	var rv *search.DocumentMatch

	for s.currentID != "" {
		if s.currMustNot != nil && s.currMustNot.ID < s.currentID {
			// advance must not searcher to our candidate entry
			s.currMustNot, err = s.mustNotSearcher.Advance(s.currentID)
			if err != nil {
				return nil, err
			}
			if s.currMustNot != nil && s.currMustNot.ID == s.currentID {
				// the candidate is excluded
				err = s.advanceNextMust()
				if err != nil {
					return nil, err
				}
				continue
			}
		} else if s.currMustNot != nil && s.currMustNot.ID == s.currentID {
			// the candidate is excluded
			err = s.advanceNextMust()
			if err != nil {
				return nil, err
			}
			continue
		}

		if s.currShould != nil && s.currShould.ID < s.currentID {
			// advance should searcher to our candidate entry
			s.currShould, err = s.shouldSearcher.Advance(s.currentID)
			if err != nil {
				return nil, err
			}
			if s.currShould != nil && s.currShould.ID == s.currentID {
				// score bonus matches should
				var cons []*search.DocumentMatch
				if s.currMust != nil {
					cons = []*search.DocumentMatch{
						s.currMust,
						s.currShould,
					}
				} else {
					cons = []*search.DocumentMatch{
						s.currShould,
					}
				}
				rv = s.scorer.Score(cons)
				err = s.advanceNextMust()
				if err != nil {
					return nil, err
				}
				break
			} else if s.shouldSearcher.Min() == 0 {
				// match is OK anyway
				rv = s.scorer.Score([]*search.DocumentMatch{s.currMust})
				err = s.advanceNextMust()
				if err != nil {
					return nil, err
				}
				break
			}
		} else if s.currShould != nil && s.currShould.ID == s.currentID {
			// score bonus matches should
			var cons []*search.DocumentMatch
			if s.currMust != nil {
				cons = []*search.DocumentMatch{
					s.currMust,
					s.currShould,
				}
			} else {
				cons = []*search.DocumentMatch{
					s.currShould,
				}
			}
			rv = s.scorer.Score(cons)
			err = s.advanceNextMust()
			if err != nil {
				return nil, err
			}
			break
		} else if s.shouldSearcher == nil || s.shouldSearcher.Min() == 0 {
			// match is OK anyway
			rv = s.scorer.Score([]*search.DocumentMatch{s.currMust})
			err = s.advanceNextMust()
			if err != nil {
				return nil, err
			}
			break
		}

		err = s.advanceNextMust()
		if err != nil {
			return nil, err
		}
	}
	return rv, nil
}

func (s *BooleanSearcher) Advance(ID string) (*search.DocumentMatch, error) {

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
		s.currentID = s.currMust.ID
	} else if s.mustSearcher == nil && s.currShould != nil {
		s.currentID = s.currShould.ID
	} else {
		s.currentID = ""
	}

	return s.Next()
}

func (s *BooleanSearcher) Count() uint64 {

	// for now return a worst case
	var sum uint64
	if s.mustSearcher != nil {
		sum += s.mustSearcher.Count()
	}
	if s.shouldSearcher != nil {
		sum += s.shouldSearcher.Count()
	}
	return sum
}

func (s *BooleanSearcher) Close() error {
	if s.mustSearcher != nil {
		err := s.mustSearcher.Close()
		if err != nil {
			return err
		}
	}
	if s.shouldSearcher != nil {
		err := s.shouldSearcher.Close()
		if err != nil {
			return err
		}
	}
	if s.mustNotSearcher != nil {
		err := s.mustNotSearcher.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *BooleanSearcher) Min() int {
	return 0
}
