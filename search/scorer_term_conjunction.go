//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import ()

type TermConjunctionQueryScorer struct {
	explain bool
}

func NewTermConjunctionQueryScorer(explain bool) *TermConjunctionQueryScorer {
	return &TermConjunctionQueryScorer{
		explain: explain,
	}
}

func (s *TermConjunctionQueryScorer) Score(constituents []*DocumentMatch) *DocumentMatch {
	rv := DocumentMatch{
		ID: constituents[0].ID,
	}

	var sum float64
	var childrenExplanations []*Explanation
	if s.explain {
		childrenExplanations = make([]*Explanation, len(constituents))
	}

	locations := []FieldTermLocationMap{}
	for i, docMatch := range constituents {
		sum += docMatch.Score
		if s.explain {
			childrenExplanations[i] = docMatch.Expl
		}
		if docMatch.Locations != nil {
			locations = append(locations, docMatch.Locations)
		}
	}
	rv.Score = sum
	if s.explain {
		rv.Expl = &Explanation{Value: sum, Message: "sum of:", Children: childrenExplanations}
	}

	if len(locations) == 1 {
		rv.Locations = locations[0]
	} else if len(locations) > 1 {
		rv.Locations = mergeLocations(locations)
	}

	return &rv
}
