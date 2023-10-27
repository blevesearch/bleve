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
	"context"
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
	overrideScorer search.SynonymScorerCallbackFn
	options        search.SearcherOptions
}

func (s *DisjunctionQueryScorer) Size() int {
	return reflectStaticSizeDisjunctionQueryScorer + size.SizeOfPtr
}

func NewDisjunctionQueryScorer(ctx context.Context, options search.SearcherOptions) *DisjunctionQueryScorer {
	if overridingScorer := ctx.Value(search.SynonymScorerKey); overridingScorer != nil {
		if scorerF, ok := overridingScorer.(search.SynonymScorerCallbackFn); ok {
			return &DisjunctionQueryScorer{
				overrideScorer: scorerF,
				options:        options,
			}
		}
	}
	return &DisjunctionQueryScorer{
		options: options,
	}
}

func (s *DisjunctionQueryScorer) Score(ctx *search.SearchContext, constituents []*search.DocumentMatch, countMatch, countTotal int) *search.DocumentMatch {
	if s.overrideScorer != nil {
		return s.overrideScorer(ctx, constituents, s.options)
	}

	var sum float64
	var childrenExplanations []*search.Explanation
	if s.options.Explain {
		childrenExplanations = make([]*search.Explanation, len(constituents))
	}

	for i, docMatch := range constituents {
		sum += docMatch.Score
		if s.options.Explain {
			childrenExplanations[i] = docMatch.Expl
		}
	}

	var rawExpl *search.Explanation
	if s.options.Explain {
		rawExpl = &search.Explanation{Value: sum, Message: "sum of:", Children: childrenExplanations}
	}

	coord := float64(countMatch) / float64(countTotal)
	newScore := sum * coord
	var newExpl *search.Explanation
	if s.options.Explain {
		ce := make([]*search.Explanation, 2)
		ce[0] = rawExpl
		ce[1] = &search.Explanation{Value: coord, Message: fmt.Sprintf("coord(%d/%d)", countMatch, countTotal)}
		newExpl = &search.Explanation{Value: newScore, Message: "product of:", Children: ce}
	}

	// reuse constituents[0] as the return value
	rv := constituents[0]
	rv.Score = newScore
	rv.Expl = newExpl
	rv.FieldTermLocations = search.MergeFieldTermLocations(
		rv.FieldTermLocations, constituents[1:])

	return rv
}
