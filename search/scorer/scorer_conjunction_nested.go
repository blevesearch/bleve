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
	// find the shortest ancestor path
	// for all ancestry chains that are shallower than the joinIdx,
	// we will consider their full depth instead, and for the
	// others we will consider up to the joinIdx depth as
	// the LCA candidate
	lcaIdx := -1
	lcaDepth := -1
	for i, anc := range ancestry {
		var candidateDepth int
		if len(anc) <= joinIdx {
			candidateDepth = len(anc)
		} else {
			candidateDepth = joinIdx + 1
		}
		if lcaDepth == -1 || candidateDepth < lcaDepth {
			lcaDepth = candidateDepth
			lcaIdx = i
		}
	}
	// take the lca document
	lcaDocID := constituents[lcaIdx].IndexInternalID
	// create a new DocumentMatch for the LCA
	// we do this because we want to avoid modifying
	// any of the constituents directly, as they may be
	// reused elsewhere
	rv := &search.DocumentMatch{
		IndexInternalID: slices.Clone(lcaDocID),
	}
	// merge all other constituents into it
	for _, dm := range constituents {
		err := rv.MergeWith(dm)
		if err != nil {
			return nil, err
		}
	}
	return rv, nil
}
