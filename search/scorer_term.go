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
	"fmt"
	"math"

	"github.com/couchbaselabs/bleve/index"
)

const MAX_SCORE_CACHE = 64

type TermQueryScorer struct {
	query                  *TermQuery
	docTerm                uint64
	docTotal               uint64
	idf                    float64
	explain                bool
	idfExplanation         *Explanation
	scoreCache             map[int]float64
	scoreExplanationCache  map[int]*Explanation
	queryNorm              float64
	queryWeight            float64
	queryWeightExplanation *Explanation
}

func NewTermQueryScorer(query *TermQuery, docTotal, docTerm uint64, explain bool) *TermQueryScorer {
	rv := TermQueryScorer{
		query:                 query,
		docTerm:               docTerm,
		docTotal:              docTotal,
		idf:                   1.0 + math.Log(float64(docTotal)/float64(docTerm+1.0)),
		explain:               explain,
		scoreCache:            make(map[int]float64, MAX_SCORE_CACHE),
		scoreExplanationCache: make(map[int]*Explanation, MAX_SCORE_CACHE),
		queryWeight:           1.0,
	}

	if explain {
		rv.idfExplanation = &Explanation{
			Value:   rv.idf,
			Message: fmt.Sprintf("idf(docFreq=%d, maxDocs=%d)", docTerm, docTotal),
		}
	}

	return &rv
}

func (s *TermQueryScorer) Weight() float64 {
	sum := s.query.Boost() * s.idf
	return sum * sum
}

func (s *TermQueryScorer) SetQueryNorm(qnorm float64) {
	s.queryNorm = qnorm

	// update the query weight
	s.queryWeight = s.query.Boost() * s.idf * s.queryNorm

	if s.explain {
		childrenExplanations := make([]*Explanation, 3)
		childrenExplanations[0] = &Explanation{
			Value:   s.query.Boost(),
			Message: "boost",
		}
		childrenExplanations[1] = s.idfExplanation
		childrenExplanations[2] = &Explanation{
			Value:   s.queryNorm,
			Message: "queryNorm",
		}
		s.queryWeightExplanation = &Explanation{
			Value:    s.queryWeight,
			Message:  fmt.Sprintf("queryWeight(%s:%s^%f), product of:", s.query.Field, string(s.query.Term), s.query.Boost()),
			Children: childrenExplanations,
		}
	}
}

func (s *TermQueryScorer) Score(termMatch *index.TermFieldDoc) *DocumentMatch {

	var scoreExplanation *Explanation
	// see if the score was cached
	score, ok := s.scoreCache[int(termMatch.Freq)]
	if !ok {
		// need to compute score
		var tf float64
		if termMatch.Freq < MAX_SQRT_CACHE {
			tf = SQRT_CACHE[int(termMatch.Freq)]
		} else {
			tf = math.Sqrt(float64(termMatch.Freq))
		}

		score = tf * termMatch.Norm * s.idf

		if s.explain {
			childrenExplanations := make([]*Explanation, 3)
			childrenExplanations[0] = &Explanation{
				Value:   tf,
				Message: fmt.Sprintf("tf(termFreq(%s:%s)=%d", s.query.Field, string(s.query.Term), termMatch.Freq),
			}
			childrenExplanations[1] = &Explanation{
				Value:   termMatch.Norm,
				Message: fmt.Sprintf("fieldNorm(field=%s, doc=%s)", s.query.Field, termMatch.ID),
			}
			childrenExplanations[2] = s.idfExplanation
			scoreExplanation = &Explanation{
				Value:    score,
				Message:  fmt.Sprintf("fieldWeight(%s:%s in %s), product of:", s.query.Field, string(s.query.Term), termMatch.ID),
				Children: childrenExplanations,
			}
		}

		// if the query weight isn't 1, multiply
		if s.queryWeight != 1.0 {
			score = score * s.queryWeight
			if s.explain {
				childExplanations := make([]*Explanation, 2)
				childExplanations[0] = s.queryWeightExplanation
				childExplanations[1] = scoreExplanation
				scoreExplanation = &Explanation{
					Value:    score,
					Message:  fmt.Sprintf("weight(%s:%s^%f in %s), product of:", s.query.Field, string(s.query.Term), s.query.Boost(), termMatch.ID),
					Children: childExplanations,
				}
			}
		}

		if termMatch.Freq < MAX_SCORE_CACHE {
			s.scoreCache[int(termMatch.Freq)] = score
			if s.explain {
				s.scoreExplanationCache[int(termMatch.Freq)] = scoreExplanation
			}
		}
	}

	if ok && s.explain {
		scoreExplanation = s.scoreExplanationCache[int(termMatch.Freq)]
	}

	rv := DocumentMatch{
		ID:    termMatch.ID,
		Score: score,
	}
	if s.explain {
		rv.Expl = scoreExplanation
	}

	if termMatch.Vectors != nil && len(termMatch.Vectors) > 0 {
		locations := make(Locations, len(termMatch.Vectors))
		for i, v := range termMatch.Vectors {
			loc := Location{
				Pos:   float64(v.Pos),
				Start: float64(v.Start),
				End:   float64(v.End),
			}
			locations[i] = &loc
		}
		tlm := make(TermLocationMap)
		tlm[s.query.Term] = locations
		rv.Locations = make(FieldTermLocationMap)
		rv.Locations[s.query.Field] = tlm
	}

	return &rv
}
