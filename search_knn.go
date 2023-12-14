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
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/collector"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
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

	// PreSearchData will be a  map that will be used
	// in the second phase of any 2-phase search, to provide additional
	// context to the second phase. This is useful in the case of index
	// aliases where the first phase will gather the PreSearchData from all
	// the indexes in the alias, and the second phase will use that
	// PreSearchData to perform the actual search.
	// The currently accepted map configuration is:
	//
	// "_knn_pre_search_data_key": []*search.DocumentMatch
	// "_synonym_pre_search_data_key":		[]*synonym.SynonymDefinition

	PreSearchData map[string]interface{} `json:"pre_search_data,omitempty"`

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
		PreSearchData    json.RawMessage   `json:"pre_search_data"`
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

	if temp.PreSearchData != nil {
		r.PreSearchData, err = query.ParsePreSearchData(temp.PreSearchData)
		if err != nil {
			return err
		}
	}

	return nil

}

// -----------------------------------------------------------------------------

func copySearchRequest(req *SearchRequest, preSearchData map[string]interface{}) *SearchRequest {
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
		PreSearchData:    preSearchData,
	}
	return &rv

}

var (
	knnOperatorAnd = knnOperator("and")
	knnOperatorOr  = knnOperator("or")
)

func createKNNQuery(req *SearchRequest) (query.Query, []int64, int64, error) {
	if requestHasKNN(req) {
		// first perform validation
		err := validateKNN(req)
		if err != nil {
			return nil, nil, 0, err
		}
		var subQueries []query.Query
		kArray := make([]int64, 0, len(req.KNN))
		sumOfK := int64(0)
		for _, knn := range req.KNN {
			knnQuery := query.NewKNNQuery(knn.Vector)
			knnQuery.SetFieldVal(knn.Field)
			knnQuery.SetK(knn.K)
			knnQuery.SetBoost(knn.Boost.Value())
			subQueries = append(subQueries, knnQuery)
			kArray = append(kArray, knn.K)
			sumOfK += knn.K
		}
		rv := query.NewDisjunctionQuery(subQueries)
		rv.RetrieveScoreBreakdown(true)
		return rv, kArray, sumOfK, nil
	}
	return nil, nil, 0, nil
}

func validateKNN(req *SearchRequest) error {
	for _, q := range req.KNN {
		if q == nil {
			return fmt.Errorf("knn query cannot be nil")
		}
		if q.K <= 0 || len(q.Vector) == 0 {
			return fmt.Errorf("k must be greater than 0 and vector must be non-empty")
		}
	}
	switch req.KNNOperator {
	case knnOperatorAnd, knnOperatorOr, "":
		// Valid cases, do nothing
	default:
		return fmt.Errorf("knn_operator must be either 'and' / 'or'")
	}
	return nil
}

func (i *indexImpl) runKnnCollector(ctx context.Context, req *SearchRequest, reader index.IndexReader, preSearch bool) ([]*search.DocumentMatch, error) {
	KNNQuery, kArray, sumOfK, err := createKNNQuery(req)
	if err != nil {
		return nil, err
	}
	knnSearcher, err := KNNQuery.Searcher(ctx, reader, i.m, search.SearcherOptions{
		Explain: req.Explain,
	})
	if err != nil {
		return nil, err
	}
	knnCollector := collector.NewKNNCollector(kArray, sumOfK)
	err = knnCollector.Collect(ctx, knnSearcher, reader)
	if err != nil {
		return nil, err
	}
	knnHits := knnCollector.Results()
	if !preSearch {
		if req.KNNOperator == knnOperatorAnd {
			idx := 0
			for _, hit := range knnHits {
				if len(hit.ScoreBreakdown) == len(kArray) {
					knnHits[idx] = hit
					idx++
				}
			}
			knnHits = knnHits[:idx]
		}
		if req.Score == "none" {
			for _, hit := range knnHits {
				hit.Score = 0.0
				hit.ScoreBreakdown = nil
			}
		}
	}
	return knnHits, nil
}

func setKnnHitsInCollector(knnHits []*search.DocumentMatch, req *SearchRequest, coll *collector.TopNCollector) {
	if len(knnHits) > 0 {
		coll.SetKNNHits(knnHits,
			func(tdIdfDocMatch *search.DocumentMatch, knnMatch *search.DocumentMatch) float64 {
				totalScore := 0.0
				if tdIdfDocMatch != nil {
					totalScore += tdIdfDocMatch.Score
				}
				for _, score := range knnMatch.ScoreBreakdown {
					totalScore += score
				}
				return totalScore
			},
			func() bool {
				return req.KNNOperator == knnOperatorAnd
			},
		)
	}
}

func mergeKNNDocumentMatches(req *SearchRequest, knnHits []*search.DocumentMatch, mergeOut []map[string]interface{}) {
	kArray := make([]int64, len(req.KNN))
	for i, knnReq := range req.KNN {
		kArray[i] = knnReq.K
	}
	knnStore := collector.GetNewKNNCollectorStore(kArray)
	for _, hit := range knnHits {
		knnStore.AddDocument(hit)
	}
	mergedKNNhits := knnStore.AllHits()
	if req.KNNOperator == knnOperatorAnd {
		for hit := range mergedKNNhits {
			if len(hit.ScoreBreakdown) != len(req.KNN) {
				delete(mergedKNNhits, hit)
			}
		}
	}
	if req.Score == "none" {
		for hit := range mergedKNNhits {
			hit.Score = 0.0
			hit.ScoreBreakdown = nil
		}
	}
	indexNumToDocMatchList := make(map[int][]*search.DocumentMatch)
	for docMatch := range mergedKNNhits {
		distributeKNNHit(docMatch, indexNumToDocMatchList)
	}
	for i := 0; i < len(mergeOut); i++ {
		mergeOut[i][search.KnnPreSearchDataKey] = indexNumToDocMatchList[i]
	}
}

func distributeKNNHit(docMatch *search.DocumentMatch, indexNumToDocMatchList map[int][]*search.DocumentMatch) {
	top := docMatch.IndexId[len(docMatch.IndexId)-1]
	docMatch.IndexId = docMatch.IndexId[:len(docMatch.IndexId)-1]
	indexNumToDocMatchList[top] = append(indexNumToDocMatchList[top], docMatch)
}

func requestHasKNN(req *SearchRequest) bool {
	return len(req.KNN) > 0
}

func addKnnToDummyRequest(dummyReq *SearchRequest, realReq *SearchRequest) {
	dummyReq.KNN = realReq.KNN
	dummyReq.KNNOperator = knnOperatorOr
}

func redistributeKNNPreSearchData(req *SearchRequest, mergedOut []map[string]interface{}) error {
	knnHits, ok := req.PreSearchData[search.KnnPreSearchDataKey].([]*search.DocumentMatch)
	if !ok {
		return fmt.Errorf("preSearchData does not have knn preSearchData for redistribution")
	}
	indexNumToDocMatchList := make(map[int][]*search.DocumentMatch)
	for _, docMatch := range knnHits {
		distributeKNNHit(docMatch, indexNumToDocMatchList)
	}
	for i := 0; i < len(mergedOut); i++ {
		newMD := make(map[string]interface{})
		for k, v := range req.PreSearchData {
			switch k {
			case search.KnnPreSearchDataKey:
				newMD[k] = indexNumToDocMatchList[i]
			default:
				newMD[k] = v
			}
		}
		mergedOut[i] = newMD
	}
	return nil
}
