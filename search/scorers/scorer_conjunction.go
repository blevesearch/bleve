//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package scorers

import (
	"github.com/blevesearch/bleve/search"
)

type ConjunctionQueryScorer struct {
	explain bool
}

func NewConjunctionQueryScorer(explain bool) *ConjunctionQueryScorer {
	return &ConjunctionQueryScorer{
		explain: explain,
	}
}

func (s *ConjunctionQueryScorer) Score(constituents []*search.DocumentMatch) *search.DocumentMatch {
	rv := search.DocumentMatch{
		ID: constituents[0].ID,
	}

	var sum float64
	var childrenExplanations []*search.Explanation
	if s.explain {
		childrenExplanations = make([]*search.Explanation, len(constituents))
	}

	locations := []search.FieldTermLocationMap{}
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
		rv.Expl = &search.Explanation{Value: sum, Message: "sum of:", Children: childrenExplanations}
	}

	if len(locations) == 1 {
		rv.Locations = locations[0]
	} else if len(locations) > 1 {
		rv.Locations = search.MergeLocations(locations)
	}

	return &rv
}
