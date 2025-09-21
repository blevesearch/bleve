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

package bleve

import (
	"fmt"

	"github.com/blevesearch/bleve/v2/fusion"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
)

const (
	DefaultScoreRankConstant = 60
)

// Rescorer is applied after all the query and knn results are obtained.
// The main use of Rescorer is in hybrid search; all the individual scores
// for query and knn are combined using Rescorer. Makes use of algorithms
// defined in `fusion`
type rescorer struct {
	req        *SearchRequest
	origFrom   int
	origSize   int
	origBoosts []float64
}

// Stores information about the hybrid search into FusionRescorer.
// Also mutates the SearchRequest by:
// - Setting boosts to 1: top level boosts only used for rescoring
// - Setting From and Size to 0 and ScoreWindowSize
func (r *rescorer) prepareSearchRequest() error {
	if r.req.RequestParams == nil {
		r.req.RequestParams = NewDefaultParams(r.req.Size)
	}

	r.origFrom = r.req.From
	r.origSize = r.req.Size

	r.req.From = 0
	r.req.Size = r.req.RequestParams.ScoreWindowSize

	// req.Query's top level boost comes first, followed by the KNN queries
	numQueries := numKNNQueries(r.req) + 1
	r.origBoosts = make([]float64, numQueries)

	// only modify queries if it is boostable. If not, ignore
	if bQuery, ok := r.req.Query.(query.BoostableQuery); ok {
		r.origBoosts[0] = bQuery.Boost()
		bQuery.SetBoost(1.0)
	} else {
		r.origBoosts[0] = 1.0
	}

	// for all the knn queries, replace boost values
	r.prepareKnnRequest()

	return nil
}

func (r *rescorer) restoreSearchRequest() {
	r.req.From = r.origFrom
	r.req.Size = r.origSize

	if bQuery, ok := r.req.Query.(query.BoostableQuery); ok {
		bQuery.SetBoost(r.origBoosts[0])
	}

	// for all the knn queries, restore boost values
	r.restoreKnnRequest()
}

func (r *rescorer) rescore(sr *SearchResult) {
	r.mergeDocs(sr)

	var fusionResult *fusion.FusionResult

	for _, hit := range sr.Hits {
		fmt.Println(hit.ID, hit.Score)
	}

	switch r.req.Score {
	case ScoreRRF:
		res := fusion.ReciprocalRankFusion(
			sr.Hits,
			r.origBoosts,
			r.req.RequestParams.ScoreRankConstant,
			r.req.RequestParams.ScoreWindowSize,
			numKNNQueries(r.req),
			r.req.Explain,
		)
		fusionResult = &res
	case ScoreRSF:
		res := fusion.ScoreFusion(
			sr.Hits,
			r.origBoosts,
			r.req.RequestParams.ScoreWindowSize,
			numKNNQueries(r.req),
			false,
			r.req.Explain,
		)
		fusionResult = &res
	case ScoreDBSF:
		res := fusion.ScoreFusion(
			sr.Hits,
			r.origBoosts,
			r.req.RequestParams.ScoreWindowSize,
			numKNNQueries(r.req),
			true,
			r.req.Explain,
		)
		fusionResult = &res
	}

	sr.Hits = fusionResult.Hits
	sr.Total = fusionResult.Total
	sr.MaxScore = fusionResult.MaxScore
}

func (r *rescorer) mergeDocs(sr *SearchResult) {
	if len(sr.FusionKnnHits) == 0 {
		return
	}

	knnHitMap := make(map[string]*search.DocumentMatch, len(sr.FusionKnnHits))

	for _, hit := range sr.FusionKnnHits {
		hit.Score = 0.0
		knnHitMap[hit.ID] = hit
	}

	for _, hit := range sr.Hits {
		if knnHit, ok := knnHitMap[hit.ID]; ok {
			hit.ScoreBreakdown = knnHit.ScoreBreakdown
			if r.req.Explain {
				hit.Expl = &search.Explanation{Value: 0.0, Message: "", Children: append([]*search.Explanation{hit.Expl}, knnHit.Expl.Children...)}
			}
			delete(knnHitMap, hit.ID)
		}
	}

	for _, hit := range knnHitMap {
		sr.Hits = append(sr.Hits, hit)
		if r.req.Explain {
			hit.Expl = &search.Explanation{Value: 0.0, Message: "", Children: append([]*search.Explanation{nil}, hit.Expl.Children...)}
		}
	}
}

func newRescorer(req *SearchRequest) *rescorer {
	return &rescorer{
		req: req,
	}
}
