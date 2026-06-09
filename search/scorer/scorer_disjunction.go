//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scorer

import (
	"fmt"
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
)

var reflectStaticSizeDisjunctionQueryScorer int

func init() {
	var dqs DisjunctionQueryScorer
	reflectStaticSizeDisjunctionQueryScorer = int(reflect.TypeOf(dqs).Size())
}

type DisjunctionQueryScorer struct {
	options search.SearcherOptions
}

func (s *DisjunctionQueryScorer) Size() int {
	return reflectStaticSizeDisjunctionQueryScorer + size.SizeOfPtr
}

func NewDisjunctionQueryScorer(options search.SearcherOptions) *DisjunctionQueryScorer {
	return &DisjunctionQueryScorer{
		options: options,
	}
}

func (s *DisjunctionQueryScorer) Score(ctx *search.SearchContext, constituents []*search.DocumentMatch, countMatch, countTotal int) *search.DocumentMatch {
	rv := constituents[0]
	var sum float64
	for _, docMatch := range constituents {
		sum += docMatch.Score
	}
	coord := float64(countMatch) / float64(countTotal)
	rv.Score = sum * coord
	rv.Expl = nil
	rv.FieldTermLocations = search.MergeFieldTermLocations(rv.FieldTermLocations, constituents[1:])
	if s.options.Explain {
		s.scoreExplain(rv, constituents, sum, coord, countMatch, countTotal)
	}
	return rv
}

// ScoreFast is a lightweight variant of Score for the MAXSCORE lazy path.
// In that path, scoreCurrentDoc (TermQueryScorer.ScoreInto) only sets Score —
// FieldTermLocations is never written, and s.options.Explain is always false.
// ScoreFast skips MergeFieldTermLocations and the explain branch so it stays
// inlinable (cost < 80), allowing the call in nextMAXSCORE to be folded in.
func (s *DisjunctionQueryScorer) ScoreFast(constituents []*search.DocumentMatch, countMatch, countTotal int) *search.DocumentMatch {
	rv := constituents[0]
	var sum float64
	for _, docMatch := range constituents {
		sum += docMatch.Score
	}
	// When all terms matched (coord == 1.0), skip the multiply+divide.
	// For topical queries where all terms co-occur this is the common path.
	if countMatch == countTotal {
		rv.Score = sum
	} else {
		rv.Score = sum * float64(countMatch) / float64(countTotal)
	}
	rv.Expl = nil
	return rv
}

// scoreExplain populates rv.Expl; called only when s.options.Explain is set.
func (s *DisjunctionQueryScorer) scoreExplain(rv *search.DocumentMatch, constituents []*search.DocumentMatch, sum, coord float64, countMatch, countTotal int) {
	childrenExplanations := make([]*search.Explanation, len(constituents))
	for i, docMatch := range constituents {
		childrenExplanations[i] = docMatch.Expl
	}
	rawExpl := &search.Explanation{Value: sum, Message: "sum of:", Children: childrenExplanations}
	ce := make([]*search.Explanation, 2)
	ce[0] = rawExpl
	ce[1] = &search.Explanation{Value: coord, Message: fmt.Sprintf("coord(%d/%d)", countMatch, countTotal)}
	rv.Expl = &search.Explanation{Value: rv.Score, Message: "product of:", Children: ce, PartialMatch: countMatch != countTotal}
}

// This method is used only when disjunction searcher is used over multiple
// KNN searchers, where only the score breakdown and the optional explanation breakdown
// is required. The final score and explanation is set when we finalize the KNN hits.
func (s *DisjunctionQueryScorer) ScoreAndExplBreakdown(ctx *search.SearchContext, constituents []*search.DocumentMatch,
	matchingIdxs []int, originalPositions []int, countTotal int) *search.DocumentMatch {

	rv := constituents[0]
	if rv.ScoreBreakdown == nil {
		rv.ScoreBreakdown = make(map[int]float64, len(constituents))
	}
	var childrenExplanations []*search.Explanation
	if s.options.Explain {
		// since we want to notify which expl belongs to which matched searcher within the disjunction searcher
		childrenExplanations = make([]*search.Explanation, countTotal)
	}

	for i, docMatch := range constituents {
		var index int
		if originalPositions != nil {
			// scorer used in disjunction slice searcher
			index = originalPositions[matchingIdxs[i]]
		} else {
			// scorer used in disjunction heap searcher
			index = matchingIdxs[i]
		}
		rv.ScoreBreakdown[index] = docMatch.Score
		if s.options.Explain {
			childrenExplanations[index] = docMatch.Expl
		}
	}
	var explBreakdown *search.Explanation
	if s.options.Explain {
		explBreakdown = &search.Explanation{Children: childrenExplanations}
	}
	rv.Expl = explBreakdown
	rv.FieldTermLocations = search.MergeFieldTermLocations(
		rv.FieldTermLocations, constituents[1:])
	return rv
}
