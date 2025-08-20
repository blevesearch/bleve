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

package bleve

import (
	"github.com/blevesearch/bleve/v2/fusion"
	"github.com/blevesearch/bleve/v2/search/query"
)

// Rescorer is applied after all the query and knn results are obtained.
// The main use of Rescorer is in hybrid search; all the individual scores
// for query and knn are combined using Rescorer
type rescorer interface {
	prepareSearchRequest()
	restoreSearchRequest()
	rescore(*SearchResult)
}

const (
	ReciprocalRankFusionStrategy string = "rrf"
)

// Concrete implementation of rescorer for hybrid search. Makes use of
// algorithms defined in `fusion`.
type fusionRescorer struct {
	req        *SearchRequest
	origFrom   int
	origSize   int
	origBoosts []float64
}

// Stores information about the hybrid search into FusionRescorer.
// Also mutates the SearchRequest by:
// - Setting boosts to 1: top level boosts only used for rescoring
// - Setting From and Size to 0 and ScoreWindowSize
func (r *fusionRescorer) prepareSearchRequest() {
	if r.req.Params.ScoreRankConstant == nil {
		src := 60
		r.req.Params.ScoreRankConstant = &src
	}

	if r.req.Params.ScoreWindowSize == nil {
		sws := r.req.Size
		r.req.Params.ScoreWindowSize = &sws
	}
	r.origFrom = r.req.From
	r.origSize = r.req.Size

	r.req.From = 0
	r.req.Size = *r.req.Params.ScoreWindowSize

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
}

func (r *fusionRescorer) restoreSearchRequest() {
	r.req.From = r.origFrom
	r.req.Size = r.origSize

	if bQuery, ok := r.req.Query.(query.BoostableQuery); ok {
		bQuery.SetBoost(r.origBoosts[0])
	}

	// for all the knn queries, restore boost values
	r.restoreKnnRequest()
}

func (r *fusionRescorer) rescore(sr *SearchResult) {
	var fusionResult fusion.FusionResult

	switch r.req.Score {
	case ReciprocalRankFusionStrategy:
		fusionResult = fusion.ReciprocalRankFusion(
			sr.Hits,
			r.origBoosts,
			*r.req.Params.ScoreRankConstant,
			*r.req.Params.ScoreWindowSize,
			numKNNQueries(r.req),
			r.req.Explain,
		)
	}

	sr.Hits = fusionResult.Hits
	sr.Total = fusionResult.Total
	sr.MaxScore = fusionResult.MaxScore
}

func newFusionRescorer(req *SearchRequest) rescorer {
	return &fusionRescorer{
		req: req,
	}
}
