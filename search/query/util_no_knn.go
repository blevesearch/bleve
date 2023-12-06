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

package query

import (
	"context"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

func (q *ConjunctionQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	ss := make([]search.Searcher, 0, len(q.Conjuncts))
	for _, conjunct := range q.Conjuncts {
		sr, err := conjunct.Searcher(ctx, i, m, options)
		if err != nil {
			for _, searcher := range ss {
				if searcher != nil {
					_ = searcher.Close()
				}
			}
			return nil, err
		}
		if _, ok := sr.(*searcher.MatchNoneSearcher); ok && q.queryStringMode {
			// in query string mode, skip match none
			continue
		}
		ss = append(ss, sr)
	}

	if len(ss) < 1 {
		return searcher.NewMatchNoneSearcher(i)
	}
	nctx := context.WithValue(ctx, search.IncludeScoreBreakdownKey, q.retrieveScoreBreakdown)

	return searcher.NewConjunctionSearcher(nctx, i, ss, options)
}

func (q *DisjunctionQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping,
	options search.SearcherOptions) (search.Searcher, error) {
	ss := make([]search.Searcher, 0, len(q.Disjuncts))
	for _, disjunct := range q.Disjuncts {
		sr, err := disjunct.Searcher(ctx, i, m, options)
		if err != nil {
			for _, searcher := range ss {
				if searcher != nil {
					_ = searcher.Close()
				}
			}
			return nil, err
		}
		if sr != nil {
			if _, ok := sr.(*searcher.MatchNoneSearcher); ok && q.queryStringMode {
				// in query string mode, skip match none
				continue
			}
			ss = append(ss, sr)
		}
	}

	if len(ss) < 1 {
		return searcher.NewMatchNoneSearcher(i)
	}

	nctx := context.WithValue(ctx, search.IncludeScoreBreakdownKey, q.retrieveScoreBreakdown)

	return searcher.NewDisjunctionSearcher(nctx, i, ss, q.Min, options)
}
