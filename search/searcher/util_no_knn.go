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

//go:build !vectors
// +build !vectors

package searcher

import "math"

func (s *DisjunctionSliceSearcher) computeQueryNorm() {
	// first calculate sum of squared weights
	sumOfSquaredWeights := 0.0
	for _, searcher := range s.searchers {
		sumOfSquaredWeights += searcher.Weight()
	}
	// now compute query norm from this
	s.queryNorm = 1.0 / math.Sqrt(sumOfSquaredWeights)
	// finally tell all the downstream searchers the norm
	for _, searcher := range s.searchers {
		searcher.SetQueryNorm(s.queryNorm)
	}
}

func (s *DisjunctionHeapSearcher) computeQueryNorm() {
	// first calculate sum of squared weights
	sumOfSquaredWeights := 0.0
	for _, searcher := range s.searchers {
		sumOfSquaredWeights += searcher.Weight()
	}
	// now compute query norm from this
	s.queryNorm = 1.0 / math.Sqrt(sumOfSquaredWeights)
	// finally tell all the downstream searchers the norm
	for _, searcher := range s.searchers {
		searcher.SetQueryNorm(s.queryNorm)
	}
}

func (s *ConjunctionSearcher) computeQueryNorm() {
	// first calculate sum of squared weights
	sumOfSquaredWeights := 0.0
	for _, searcher := range s.searchers {
		sumOfSquaredWeights += searcher.Weight()
	}
	// now compute query norm from this
	s.queryNorm = 1.0 / math.Sqrt(sumOfSquaredWeights)
	// finally tell all the downstream searchers the norm
	for _, searcher := range s.searchers {
		searcher.SetQueryNorm(s.queryNorm)
	}
}

func optimizeCompositeSearcher(ctx context.Context, optimizationKind string,
	indexReader index.IndexReader, qsearchers []search.Searcher,
	options search.SearcherOptions) (search.Searcher, error) {
	var octx index.OptimizableContext

	for _, searcher := range qsearchers {
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
