//  Copyright (c) 2025 Couchbase, Inc.
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
	"slices"

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
	ancestry [][]index.IndexInternalID, joinIdx int) (*search.DocumentMatch, error) {
	// Find the constituent with the shortest effective depth.
	lcaIdx := 0
	lcaDepth := computeDepth(ancestry[0], joinIdx)

	for i := 1; i < len(ancestry); i++ {
		d := computeDepth(ancestry[i], joinIdx)
		if d < lcaDepth {
			lcaDepth = d
			lcaIdx = i
		}
	}

	// Clone the LCA document ID and start a fresh DocumentMatch.
	lcaDocID := constituents[lcaIdx].IndexInternalID
	result := &search.DocumentMatch{
		IndexInternalID: slices.Clone(lcaDocID),
	}

	// Merge all constituents into the new match.
	for _, dm := range constituents {
		if err := result.MergeWith(dm); err != nil {
			return nil, err
		}
	}

	return result, nil
}

// computeDepth returns the depth considered for LCA selection.
func computeDepth(anc []index.IndexInternalID, joinIdx int) int {
	if len(anc) <= joinIdx {
		return len(anc)
	}
	return joinIdx + 1
}
