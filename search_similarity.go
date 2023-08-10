//  Copyright (c) 2023 Couchbase, Inc.
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

//go:build densevector
// +build densevector

package bleve

import (
	"sort"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
)

type SearchRequest struct {
	Query            query.Query       `json:"query"`
	Size             int               `json:"size"`
	From             int               `json:"from"`
	Highlight        *HighlightRequest `json:"highlight"`
	Fields           []string          `json:"fields"`
	Facets           FacetsRequest     `json:"facets"`
	Explain          bool              `json:"explain"`
	Sort             search.SortOrder  `json:"sort"`
	IncludeLocations bool              `json:"includeLocations"`
	Score            string            `json:"score,omitempty"`
	SearchAfter      []string          `json:"search_after"`
	SearchBefore     []string          `json:"search_before"`

	Similarity *SimilarityRequest `json:"knn"`

	sortFunc func(sort.Interface)
}

type SimilarityRequest struct {
	Field  string    `json:"field"`
	Vector []float32 `json:"vector"`
	K      int64     `json:"k"`
}

func (r *SearchRequest) SetSimilarity(field string, vector []float32, k int64) {
	r.Similarity = &SimilarityRequest{
		Field:  field,
		Vector: vector,
		K:      k,
	}
}
