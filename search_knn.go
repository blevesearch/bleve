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
	"container/heap"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
)

type knnOperator string

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

	KNN         []*KNNRequest `json:"knn"`
	KNNOperator knnOperator   `json:"knn_operator"`

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

func (r *SearchRequest) AddKNNOperator(operator knnOperator) {
	r.KNNOperator = operator
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
		KNNOperator      knnOperator       `json:"knn_operator"`
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
	r.KNNOperator = temp.KNNOperator
	if r.KNNOperator == "" {
		r.KNNOperator = knnOperatorOr
	}

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
		KNNOperator:      req.KNNOperator,
	}
	return &rv

}

var (
	knnOperatorAnd = knnOperator("and")
	knnOperatorOr  = knnOperator("or")
)

func queryWithKNN(req *SearchRequest) (query.Query, error) {
	if len(req.KNN) > 0 {
		subQueries := []query.Query{req.Query}
		for _, knn := range req.KNN {
			if knn != nil {
				knnQuery := query.NewKNNQuery(knn.Vector)
				knnQuery.SetFieldVal(knn.Field)
				knnQuery.SetK(knn.K)
				knnQuery.SetBoost(knn.Boost.Value())
				subQueries = append(subQueries, knnQuery)
			}
		}
		if req.KNNOperator == knnOperatorAnd {
			return query.NewConjunctionQuery(subQueries), nil
		} else if req.KNNOperator == knnOperatorOr || req.KNNOperator == "" {
			return query.NewDisjunctionQuery(subQueries), nil
		} else {
			return nil, fmt.Errorf("unknown knn operator: %s", req.KNNOperator)
		}
	}
	return req.Query, nil
}

func validateKNN(req *SearchRequest) error {
	for _, q := range req.KNN {
		if q.K <= 0 || len(q.Vector) == 0 {
			return fmt.Errorf("k must be greater than 0 and vector must be non-empty")
		}
	}
	return nil
}

func mergeKNNResults(req *SearchRequest, sr *SearchResult) {
	if len(req.KNN) > 0 {
		mergeKNN(req, sr)
	}
}

func adjustRequestSizeForKNN(req *SearchRequest, numIndexPartitions int) int {
	var adjustedSize int
	if req != nil {
		adjustedSize = req.Size
		if len(req.KNN) > 0 {
			var minSizeReq int64
			for _, knn := range req.KNN {
				minSizeReq += knn.K
			}
			minSizeReq *= int64(numIndexPartitions)
			if int64(adjustedSize) < minSizeReq {
				adjustedSize = int(minSizeReq)
			}
		}
	}
	return adjustedSize
}

// heap impl
type scoreHeap struct {
	scoreBreakdown []*[]float64
	sortIndex      int
}

func (s *scoreHeap) Len() int { return len(s.scoreBreakdown) }

func (s *scoreHeap) Less(i, j int) bool {
	return (*s.scoreBreakdown[i])[s.sortIndex] > (*s.scoreBreakdown[j])[s.sortIndex]
}

func (s *scoreHeap) Swap(i, j int) {
	s.scoreBreakdown[i], s.scoreBreakdown[j] = s.scoreBreakdown[j], s.scoreBreakdown[i]
}

func (s *scoreHeap) Push(x interface{}) {
	s.scoreBreakdown = append(s.scoreBreakdown, x.(*[]float64))
}

func (s *scoreHeap) Pop() interface{} {
	old := s.scoreBreakdown
	n := len(old)
	x := old[n-1]
	s.scoreBreakdown = old[0 : n-1]
	return x
}

func mergeKNN(req *SearchRequest, sr *SearchResult) {
	// index 0 of score breakdown is always tf-idf score
	numKnnQuery := len(req.KNN)
	maxHeap := &scoreHeap{
		scoreBreakdown: make([]*[]float64, 0),
	}
	for i := 0; i < numKnnQuery; i++ {
		kVal := req.KNN[i].K
		maxHeap.sortIndex = i + 1
		for _, hit := range sr.Hits {
			heap.Push(maxHeap, &hit.ScoreBreakdown)
		}
		for maxHeap.Len() > 0 {
			arr := heap.Pop(maxHeap).(*[]float64)
			if kVal > 0 {
				kVal--
			} else {
				(*arr)[maxHeap.sortIndex] = 0
			}
		}
	}
	operator := 0
	if _, ok := req.Query.(*query.ConjunctionQuery); ok {
		operator = 1
	}
	nonZeroScoreHits := make([]*search.DocumentMatch, 0, len(sr.Hits))
	maxScore := 0.0
	var numHitsDropped uint64
	for _, hit := range sr.Hits {
		newScore := recomputeTotalScore(operator, hit)
		if newScore > 0 {
			hit.Score = newScore
			if newScore > maxScore {
				maxScore = newScore
			}
			nonZeroScoreHits = append(nonZeroScoreHits, hit)
		} else {
			numHitsDropped++
		}
	}
	sr.Hits = nonZeroScoreHits
	sr.MaxScore = maxScore
	sr.Total -= numHitsDropped
}

func recomputeTotalScore(operator int, hit *search.DocumentMatch) float64 {
	totalScore := 0.0
	numNonZero := 0
	for _, score := range hit.ScoreBreakdown {
		if score != 0 {
			numNonZero += 1
		}
		totalScore += score
	}
	if operator == 0 {
		coord := float64(numNonZero) / float64(len(hit.ScoreBreakdown))
		totalScore = totalScore * coord
	}
	return totalScore
}
