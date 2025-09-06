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
	ancestry [][]index.IndexInternalID) (*search.DocumentMatch, error) {
	// find the shortest ancestor path
	lcaIdx := 0
	lcaPath := ancestry[lcaIdx]
	// find the lowest common ancestor path
	for i := 1; i < len(ancestry) && i < len(constituents); i++ {
		if len(ancestry[i]) < len(lcaPath) {
			lcaIdx = i
			lcaPath = ancestry[i]
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
