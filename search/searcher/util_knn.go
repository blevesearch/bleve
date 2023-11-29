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

package searcher

import (
	"context"
	"math"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// util func used by both disjunction and conjunction searchers
// to compute the query norm.
// This follows a separate code path from the non-knn version
// because we need to separate out the weights from the KNN searchers
// and the rest of the searchers to make the knn
// score completely independent of tf-idf.
// the sumOfSquaredWeights depends on the tf-idf weights
// and using the same value for knn searchers will make the
// knn score dependent on tf-idf.
func computeQueryNorm(searchers []search.Searcher) (float64, float64) {
	var queryNorm float64
	var queryNormForKNN float64
	// first calculate sum of squared weights
	sumOfSquaredWeights := 0.0

	sumOfSquaredWeightsForKNN := 0.0

	for _, searcher := range searchers {
		if knnSearcher, ok := searcher.(*KNNSearcher); ok {
			sumOfSquaredWeightsForKNN += knnSearcher.Weight()
		} else {
			sumOfSquaredWeights += searcher.Weight()
		}
	}
	// now compute query norm from this
	if sumOfSquaredWeights != 0.0 {
		queryNorm = 1.0 / math.Sqrt(sumOfSquaredWeights)
	}
	if sumOfSquaredWeightsForKNN != 0.0 {
		queryNormForKNN = 1.0 / math.Sqrt(sumOfSquaredWeightsForKNN)
	}
	// finally tell all the downstream searchers the norm
	for _, searcher := range searchers {
		if knnSearcher, ok := searcher.(*KNNSearcher); ok {
			knnSearcher.SetQueryNorm(queryNormForKNN)
		} else {
			searcher.SetQueryNorm(queryNorm)
		}
	}
	return queryNorm, queryNormForKNN
}

func (s *DisjunctionSliceSearcher) computeQueryNorm() {
	s.queryNorm, s.queryNormForKNN = computeQueryNorm(s.searchers)
}

func (s *DisjunctionHeapSearcher) computeQueryNorm() {
	s.queryNorm, s.queryNormForKNN = computeQueryNorm(s.searchers)
}

func (s *ConjunctionSearcher) computeQueryNorm() {
	s.queryNorm, s.queryNormForKNN = computeQueryNorm(s.searchers)
}

func optimizeCompositeSearcher(ctx context.Context, optimizationKind string,
	indexReader index.IndexReader, qsearchers []search.Searcher,
	options search.SearcherOptions) (search.Searcher, error) {
	var octx index.OptimizableContext

	for _, searcher := range qsearchers {
		// if is KNN searcher, continue
		// should not break due to a kNN searcher.
		if _, ok := searcher.(*KNNSearcher); ok {
			continue
		}

		o, ok := searcher.(index.Optimizable)
		if !ok {
			return nil, nil
		}

		var err error
		octx, err = o.Optimize(optimizationKind, octx)
		if err != nil {
			return nil, err
		}

		if octx == nil {
			return nil, nil
		}
	}

	optimized, err := octx.Finish()
	if err != nil || optimized == nil {
		return nil, err
	}

	tfr, ok := optimized.(index.TermFieldReader)
	if !ok {
		return nil, nil
	}

	return newTermSearcherFromReader(indexReader, tfr,
		[]byte(optimizationKind), "*", 1.0, options)
}
