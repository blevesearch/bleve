package search

import (
	"math"

	"github.com/couchbaselabs/bleve/index"
)

type PhraseSearcher struct {
	query        *PhraseQuery
	index        index.Index
	mustSearcher *TermConjunctionSearcher
	queryNorm    float64
	currMust     *DocumentMatch
	slop         int
}

func NewPhraseSearcher(index index.Index, query *PhraseQuery) (*PhraseSearcher, error) {
	// build the downstream searchres
	var err error
	var mustSearcher *TermConjunctionSearcher

	if query.Terms != nil {
		qterms := make([]Query, 0, len(query.Terms))
		for _, qt := range query.Terms {
			if qt != nil {
				qterms = append(qterms, qt)
			}
		}
		tcq := TermConjunctionQuery{
			Terms:    qterms,
			BoostVal: 1.0,
			Explain:  query.Explain,
		}

		mustSearcher, err = NewTermConjunctionSearcher(index, &tcq)
		if err != nil {
			return nil, err
		}
	}

	// build our searcher
	rv := PhraseSearcher{
		index:        index,
		query:        query,
		mustSearcher: mustSearcher,
	}
	rv.computeQueryNorm()
	err = rv.initSearchers()
	if err != nil {
		return nil, err
	}

	return &rv, nil
}

func (s *PhraseSearcher) computeQueryNorm() {
	// first calculate sum of squared weights
	sumOfSquaredWeights := 0.0
	if s.mustSearcher != nil {
		sumOfSquaredWeights += s.mustSearcher.Weight()
	}

	// now compute query norm from this
	s.queryNorm = 1.0 / math.Sqrt(sumOfSquaredWeights)
	// finally tell all the downsteam searchers the norm
	if s.mustSearcher != nil {
		s.mustSearcher.SetQueryNorm(s.queryNorm)
	}
}

func (s *PhraseSearcher) initSearchers() error {
	var err error
	// get all searchers pointing at their first match
	if s.mustSearcher != nil {
		s.currMust, err = s.mustSearcher.Next()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *PhraseSearcher) advanceNextMust() error {
	var err error

	if s.mustSearcher != nil {
		s.currMust, err = s.mustSearcher.Next()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *PhraseSearcher) Weight() float64 {
	var rv float64
	rv += s.mustSearcher.Weight()

	return rv
}

func (s *PhraseSearcher) SetQueryNorm(qnorm float64) {
	s.mustSearcher.SetQueryNorm(qnorm)
}

func (s *PhraseSearcher) Next() (*DocumentMatch, error) {
	var rv *DocumentMatch
	for s.currMust != nil {
		rvtlm := make(TermLocationMap, 0)
		freq := 0
		firstTerm := s.query.Terms[0]
		termLocMap, ok := s.currMust.Locations[firstTerm.Field]
		if ok {
			locations, ok := termLocMap[firstTerm.Term]
			if ok {
			OUTER:
				for _, location := range locations {
					crvtlm := make(TermLocationMap, 0)
				INNER:
					for i := 0; i < len(s.query.Terms); i++ {
						nextTerm := s.query.Terms[i]
						if nextTerm != nil {
							// look through all this terms locations
							// to try and find the correct offsets
							nextLocations, ok := termLocMap[nextTerm.Term]
							if ok {
								for _, nextLocation := range nextLocations {
									if nextLocation.Pos == location.Pos+float64(i) {
										// found a location match for this term
										crvtlm.AddLocation(nextTerm.Term, nextLocation)
										continue INNER
									}
								}
								// if we got here we didnt find location match for this term
								continue OUTER
							}
						}
					}
					// if we got here all the terms matched
					freq += 1
					mergeTermLocationMaps(rvtlm, crvtlm)
				}
			}
		}

		if freq > 0 {
			// return match
			rv = s.currMust
			rv.Locations = FieldTermLocationMap{
				firstTerm.Field: rvtlm,
			}
			s.advanceNextMust()
			return rv, nil
		}

		s.advanceNextMust()
	}

	return nil, nil
}

func (s *PhraseSearcher) Advance(ID string) (*DocumentMatch, error) {
	s.mustSearcher.Advance(ID)
	return s.Next()
}

func (s *PhraseSearcher) Count() uint64 {
	// for now return a worst case
	var sum uint64 = 0
	sum += s.mustSearcher.Count()
	return sum
}

func (s *PhraseSearcher) Close() {
	if s.mustSearcher != nil {
		s.mustSearcher.Close()
	}
}
