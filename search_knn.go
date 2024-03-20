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

// Must be updated only at init
var BleveMaxK = int64(10000)

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
	if req.KNN != nil &&
		req.KNNOperator != "" &&
		req.KNNOperator != knnOperatorOr &&
		req.KNNOperator != knnOperatorAnd {
		return fmt.Errorf("unknown knn operator: %s", req.KNNOperator)
	}
	for _, q := range req.KNN {
		if q == nil {
			return fmt.Errorf("knn query cannot be nil")
		}
		if q.K <= 0 || len(q.Vector) == 0 {
			return fmt.Errorf("k must be greater than 0 and vector must be non-empty")
		}
		if q.K > BleveMaxK {
			return fmt.Errorf("k must be less than %d", BleveMaxK)
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

func addSortAndFieldsToKNNHits(req *SearchRequest, knnHits []*search.DocumentMatch, reader index.IndexReader, name string) (err error) {
	requiredSortFields := req.Sort.RequiredFields()
	var dvReader index.DocValueReader
	var updateFieldVisitor index.DocValueVisitor
	if len(requiredSortFields) > 0 {
		dvReader, err = reader.DocValueReader(requiredSortFields)
		if err != nil {
			return err
		}
		updateFieldVisitor = func(field string, term []byte) {
			req.Sort.UpdateVisitor(field, term)
		}
	}
	for _, hit := range knnHits {
		if len(requiredSortFields) > 0 {
			err = dvReader.VisitDocValues(hit.IndexInternalID, updateFieldVisitor)
			if err != nil {
				return err
			}
		}
		req.Sort.Value(hit)
		err, _ = LoadAndHighlightFields(hit, req, "", reader, nil)
		if err != nil {
			return err
		}
		hit.Index = name
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
		knnHits = finalizeKNNResults(req, knnHits)
	}
	// at this point, irrespective of whether it is a preSearch or not,
	// the knn hits are populated with Sort and Fields.
	// it must be ensured downstream that the Sort and Fields are not
	// re-evaluated, for these hits.
	// also add the index names to the hits, so that when early
	// exit takes place after the first phase, the hits will have
	// a valid value for Index.
	err = addSortAndFieldsToKNNHits(req, knnHits, reader, i.name)
	if err != nil {
		return nil, err
	}
	return knnHits, nil
}

func setKnnHitsInCollector(knnHits []*search.DocumentMatch, req *SearchRequest, coll *collector.TopNCollector) {
	if len(knnHits) > 0 {
		newScoreExplComputer := func(queryMatch *search.DocumentMatch, knnMatch *search.DocumentMatch) (float64, *search.Explanation) {
			totalScore := queryMatch.Score + knnMatch.Score
			if !req.Explain {
				// exit early as we don't need to compute the explanation
				return totalScore, nil
			}
			return totalScore, &search.Explanation{Value: totalScore, Message: "sum of:", Children: []*search.Explanation{queryMatch.Expl, knnMatch.Expl}}
		}
		coll.SetKNNHits(knnHits, search.ScoreExplCorrectionCallbackFunc(newScoreExplComputer))
	}
}

func finalizeKNNResults(req *SearchRequest, knnHits []*search.DocumentMatch) []*search.DocumentMatch {
	// if the KNN operator is AND, then we need to filter out the hits that
	// do not have match the KNN queries.
	if req.KNNOperator == knnOperatorAnd {
		idx := 0
		for _, hit := range knnHits {
			if len(hit.ScoreBreakdown) == len(req.KNN) {
				knnHits[idx] = hit
				idx++
			}
		}
		knnHits = knnHits[:idx]
	}
	// fix the score using score breakdown now
	// if the score is none, then we need to set the score to 0.0
	// if req.Explain is true, then we need to use the expl breakdown to
	// finalize the correct explanation.
	for _, hit := range knnHits {
		hit.Score = 0.0
		if req.Score != "none" {
			for _, score := range hit.ScoreBreakdown {
				hit.Score += score
			}
		}
		if req.Explain {
			childrenExpl := make([]*search.Explanation, 0, len(hit.ScoreBreakdown))
			for i := range hit.ScoreBreakdown {
				childrenExpl = append(childrenExpl, hit.Expl.Children[i])
			}
			hit.Expl = &search.Explanation{Value: hit.Score, Message: "sum of:", Children: childrenExpl}
		}
		// we don't need the score breakdown anymore
		// so we can set it to nil
		hit.ScoreBreakdown = nil
	}
	return knnHits
}

// when we are setting KNN hits in the preSearchData, we need to make sure that
// the KNN hit goes to the right index. This is because the KNN hits are
// collected from all the indexes in the alias, but the preSearchData is
// specific to each index. If alias A1 contains indexes I1 and I2 and
// the KNN hits collected from both I1 and I2, and merged to get top K
// hits, then the top K hits need to be distributed to I1 and I2,
// so that the preSearchData for I1 contains the top K hits from I1 and
// the preSearchData for I2 contains the top K hits from I2.
func validateAndDistributeKNNHits(knnHits []*search.DocumentMatch, indexes []Index) (map[string][]*search.DocumentMatch, error) {
	// create a set of all the index names of this alias
	indexNames := make(map[string]struct{}, len(indexes))
	for _, index := range indexes {
		indexNames[index.Name()] = struct{}{}
	}
	segregatedKnnHits := make(map[string][]*search.DocumentMatch)
	for _, hit := range knnHits {
		// for each hit, we need to perform a validation check to ensure that the stack
		// is still valid.
		//
		// if the stack is empty, then we have an inconsistency/abnormality
		// since any hit with an empty stack is supposed to land on a leaf index,
		// and not an alias. This cannot happen in normal circumstances. But
		// performing this check to be safe. Since we extract the stack top
		// in the following steps.
		if len(hit.IndexNames) == 0 {
			return nil, ErrorTwoPhaseSearchInconsistency
		}
		// since the stack is not empty, we need to check if the top of the stack
		// is a valid index name, of an index that is part of this alias. If not,
		// then we have an inconsistency that could be caused due to a topology
		// change.
		stackTopIdx := len(hit.IndexNames) - 1
		top := hit.IndexNames[stackTopIdx]
		if _, exists := indexNames[top]; !exists {
			return nil, ErrorTwoPhaseSearchInconsistency
		}
		if stackTopIdx == 0 {
			// if the stack consists of only one index, then popping the top
			// would result in an empty slice, and handle this case by setting
			// indexNames to nil. So that the final search results will not
			// contain the indexNames field.
			hit.IndexNames = nil
		} else {
			hit.IndexNames = hit.IndexNames[:stackTopIdx]
		}
		segregatedKnnHits[top] = append(segregatedKnnHits[top], hit)
	}
	return segregatedKnnHits, nil
}

func requestHasKNN(req *SearchRequest) bool {
	return len(req.KNN) > 0
}

// returns true if the search request contains a KNN request that can be
// satisfied by just performing a preSearch, completely bypassing the
// actual search.
func isKNNrequestSatisfiedByPreSearch(req *SearchRequest) bool {
	// if req.Query is not match_none => then we need to go to phase 2
	// to perform the actual query.
	if _, ok := req.Query.(*query.MatchNoneQuery); !ok {
		return false
	}
	// req.Query is a match_none query
	//
	// if request contains facets, we need to perform phase 2 to calculate
	// the facet result. Since documents were removed as part of the
	// merging process after phase 1, if the facet results were to be calculated
	// during phase 1, then they will be now be incorrect, since merging would
	// remove some documents.
	if req.Facets != nil {
		return false
	}
	// the request is a match_none query and does not contain any facets
	// so we can satisfy the request using just the preSearch result.
	return true
}

func constructKnnPreSearchData(mergedOut map[string]map[string]interface{}, preSearchResult *SearchResult,
	indexes []Index) (map[string]map[string]interface{}, error) {

	distributedHits, err := validateAndDistributeKNNHits([]*search.DocumentMatch(preSearchResult.Hits), indexes)
	if err != nil {
		return nil, err
	}
	for _, index := range indexes {
		mergedOut[index.Name()][search.KnnPreSearchDataKey] = distributedHits[index.Name()]
	}
	return mergedOut, nil
}

func addKnnToDummyRequest(dummyReq *SearchRequest, realReq *SearchRequest) {
	dummyReq.KNN = realReq.KNN
	dummyReq.KNNOperator = knnOperatorOr
	dummyReq.Explain = realReq.Explain
	dummyReq.Fields = realReq.Fields
	dummyReq.Sort = realReq.Sort
}

// the preSearchData for KNN is a list of DocumentMatch objects
// that need to be redistributed to the right index.
// This is used only in the case of an alias tree, where the indexes
// are at the leaves of the tree, and the master alias is at the root.
// At each level of the tree, the preSearchData needs to be redistributed
// to the indexes/aliases at that level. Because the preSearchData is
// specific to each final index at the leaf.
func redistributeKNNPreSearchData(req *SearchRequest, indexes []Index) (map[string]map[string]interface{}, error) {
	knnHits, ok := req.PreSearchData[search.KnnPreSearchDataKey].([]*search.DocumentMatch)
	if !ok {
		return nil, fmt.Errorf("request does not have knn preSearchData for redistribution")
	}
	segregatedKnnHits, err := validateAndDistributeKNNHits(knnHits, indexes)
	if err != nil {
		return nil, err
	}

	rv := make(map[string]map[string]interface{})
	for _, index := range indexes {
		rv[index.Name()] = make(map[string]interface{})
	}

	for _, index := range indexes {
		for k, v := range req.PreSearchData {
			switch k {
			case search.KnnPreSearchDataKey:
				rv[index.Name()][k] = segregatedKnnHits[index.Name()]
			default:
				rv[index.Name()][k] = v
			}
		}
	}
	return rv, nil
}

func newKnnPreSearchResultProcessor(req *SearchRequest) *knnPreSearchResultProcessor {
	kArray := make([]int64, len(req.KNN))
	for i, knnReq := range req.KNN {
		kArray[i] = knnReq.K
	}
	knnStore := collector.GetNewKNNCollectorStore(kArray)
	return &knnPreSearchResultProcessor{
		addFn: func(sr *SearchResult, indexName string) {
			for _, hit := range sr.Hits {
				// tag the hit with the index name, so that when the
				// final search result is constructed, the hit will have
				// a valid path to follow along the alias tree to reach
				// the index.
				hit.IndexNames = append(hit.IndexNames, indexName)
				knnStore.AddDocument(hit)
			}
		},
		finalizeFn: func(sr *SearchResult) {
			// passing nil as the document fixup function, because we don't need to
			// fixup the document, since this was already done in the first phase,
			// hence error is always nil.
			// the merged knn hits are finalized and set in the search result.
			sr.Hits, _ = knnStore.Final(nil)
		},
	}
}
