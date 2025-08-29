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
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeNestedConjunctionQueryScorer int

func init() {
	var ncqs NestedConjunctionQueryScorer
	reflectStaticSizeNestedConjunctionQueryScorer = int(reflect.TypeOf(ncqs).Size())
}

type NestedConjunctionQueryScorer struct {
	options search.SearcherOptions
}

func (s *NestedConjunctionQueryScorer) Size() int {
	return reflectStaticSizeNestedConjunctionQueryScorer + size.SizeOfPtr
}

func NewNestedConjunctionQueryScorer(options search.SearcherOptions) *NestedConjunctionQueryScorer {
	return &NestedConjunctionQueryScorer{
		options: options,
	}
}

func (s *NestedConjunctionQueryScorer) Score(ctx *search.SearchContext, constituents []*search.DocumentMatch,
	ancestry [][]index.IndexInternalID) *search.DocumentMatch {

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
	newScore := sum
	var newExpl *search.Explanation
	if s.options.Explain {
		newExpl = &search.Explanation{Value: sum, Message: "nested conjunction sum of:", Children: childrenExplanations}
	}
	// Step 1: find the shortest ancestor path
	lcaIdx := 0
	lcaPath := ancestry[lcaIdx]
	for i := 1; i < len(ancestry); i++ {
		if len(ancestry[i]) < len(lcaPath) {
			lcaIdx = i
			lcaPath = ancestry[lcaIdx]
		}
	}

	// collect all other constituents except the reused one
	subDocs := make([]*search.DocumentMatch, 0, len(constituents)-1)
	for i, dm := range constituents {
		if i != lcaIdx {
			subDocs = append(subDocs, dm)
		}
	}

	// reuse constituents[lcaIdx] as the return value
	rv := constituents[lcaIdx]
	rv.Score = newScore
	rv.Expl = newExpl
	rv.FieldTermLocations = search.MergeFieldTermLocations(rv.FieldTermLocations, subDocs)
	if rv.Children == nil {
		rv.Children = make([]*search.DocumentMatch, 0, len(constituents)-1)
	}
	rv.Children = append(rv.Children, subDocs...)
	return rv
}
