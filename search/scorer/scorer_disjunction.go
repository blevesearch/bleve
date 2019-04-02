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

	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/size"
)

var reflectStaticSizeDisjunctionQueryScorer int

func init() {
	var dqs DisjunctionQueryScorer
	reflectStaticSizeDisjunctionQueryScorer = int(reflect.TypeOf(dqs).Size())
}

type DisjunctionQueryScorer struct {
	options      search.SearcherOptions
	coordEnabled bool
}

func (s *DisjunctionQueryScorer) Size() int {
	return reflectStaticSizeDisjunctionQueryScorer + size.SizeOfPtr
}

func NewDisjunctionQueryScorer(options search.SearcherOptions) *DisjunctionQueryScorer {
	return &DisjunctionQueryScorer{
		options:      options,
		coordEnabled: true,
	}
}

func NewUncoordDisjunctionQueryScorer(options search.SearcherOptions) *DisjunctionQueryScorer {
	q := NewDisjunctionQueryScorer(options)
	q.coordEnabled = false
	return q
}

func (s *DisjunctionQueryScorer) Score(ctx *search.SearchContext, constituents []*search.DocumentMatch, countMatch, countTotal int) *search.DocumentMatch {
	var score float64
	var childrenExplanations []*search.Explanation
	if s.options.Explain {
		childrenExplanations = make([]*search.Explanation, len(constituents))
	}

	for i, docMatch := range constituents {
		score += docMatch.Score
		if s.options.Explain {
			childrenExplanations[i] = docMatch.Expl
		}
	}

	var expl *search.Explanation
	if s.options.Explain {
		expl = &search.Explanation{Value: score, Message: "sum of:", Children: childrenExplanations}
	}

	if s.coordEnabled {
		coord := float64(countMatch) / float64(countTotal)
		score = score * coord
		if s.options.Explain {
			expl = &search.Explanation{
				Value:   score,
				Message: "product of:",
				Children: []*search.Explanation{
					expl,
					&search.Explanation{Value: coord, Message: fmt.Sprintf("coord(%d/%d)", countMatch, countTotal)},
				},
			}
		}
	}

	// reuse constituents[0] as the return value
	rv := constituents[0]
	rv.Score = score
	rv.Expl = expl
	rv.FieldTermLocations = search.MergeFieldTermLocations(
		rv.FieldTermLocations, constituents[1:])

	return rv
}
