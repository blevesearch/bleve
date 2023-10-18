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

//go:build vectors
// +build vectors

package bleve

import (
	"encoding/json"
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

	KNN []*KNNRequest `json:"knn"`

	sortFunc func(sort.Interface)
}

type KNNRequest struct {
	Field  string       `json:"field"`
	Vector []float32    `json:"vector"`
	K      int64        `json:"k"`
	Boost  *query.Boost `json:"boost,omitempty"`
}

func (r *SearchRequest) AddKNN(field string, vector []float32, k int64, boost float64) {
	b := query.Boost(boost)
	r.KNN = append(r.KNN, &KNNRequest{
		Field:  field,
		Vector: vector,
		K:      k,
		Boost:  &b,
	})
}

// UnmarshalJSON deserializes a JSON representation of
// a SearchRequest
func (r *SearchRequest) UnmarshalJSON(input []byte) error {
	var temp struct {
		Q                json.RawMessage   `json:"query"`
		Size             *int              `json:"size"`
		From             int               `json:"from"`
		Highlight        *HighlightRequest `json:"highlight"`
		Fields           []string          `json:"fields"`
		Facets           FacetsRequest     `json:"facets"`
		Explain          bool              `json:"explain"`
		Sort             []json.RawMessage `json:"sort"`
		IncludeLocations bool              `json:"includeLocations"`
		Score            string            `json:"score"`
		SearchAfter      []string          `json:"search_after"`
		SearchBefore     []string          `json:"search_before"`
		KNN              []*KNNRequest     `json:"knn"`
	}

	err := json.Unmarshal(input, &temp)
	if err != nil {
		return err
	}

	if temp.Size == nil {
		r.Size = 10
	} else {
		r.Size = *temp.Size
	}
	if temp.Sort == nil {
		r.Sort = search.SortOrder{&search.SortScore{Desc: true}}
	} else {
		r.Sort, err = search.ParseSortOrderJSON(temp.Sort)
		if err != nil {
			return err
		}
	}
	r.From = temp.From
	r.Explain = temp.Explain
	r.Highlight = temp.Highlight
	r.Fields = temp.Fields
	r.Facets = temp.Facets
	r.IncludeLocations = temp.IncludeLocations
	r.Score = temp.Score
	r.SearchAfter = temp.SearchAfter
	r.SearchBefore = temp.SearchBefore
	r.Query, err = query.ParseQuery(temp.Q)
	if err != nil {
		return err
	}

	if r.Size < 0 {
		r.Size = 10
	}
	if r.From < 0 {
		r.From = 0
	}

	r.KNN = temp.KNN

	return nil

}

// -----------------------------------------------------------------------------

func copySearchRequest(req *SearchRequest) *SearchRequest {
	rv := SearchRequest{
		Query:            req.Query,
		Size:             req.Size + req.From,
		From:             0,
		Highlight:        req.Highlight,
		Fields:           req.Fields,
		Facets:           req.Facets,
		Explain:          req.Explain,
		Sort:             req.Sort.Copy(),
		IncludeLocations: req.IncludeLocations,
		Score:            req.Score,
		SearchAfter:      req.SearchAfter,
		SearchBefore:     req.SearchBefore,
		KNN:              req.KNN,
	}
	return &rv

}

func disjunctQueryWithKNN(req *SearchRequest) query.Query {
	if len(req.KNN) > 0 {
		disjuncts := []query.Query{req.Query}
		for _, knn := range req.KNN {
			if knn != nil {
				knnQuery := query.NewKNNQuery(knn.Vector)
				knnQuery.SetFieldVal(knn.Field)
				knnQuery.SetK(knn.K)
				knnQuery.SetBoost(knn.Boost.Value())
				disjuncts = append(disjuncts, knnQuery)
			}
		}
		return query.NewDisjunctionQuery(disjuncts)
	}
	return req.Query
}
