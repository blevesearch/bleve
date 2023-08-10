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

package query

import (
	"context"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

type SimilarityQuery struct {
	VectorField string    `json:"field"`
	Vector      []float32 `json:"vector"`
	K           int64     `json:"k"`
	BoostVal    *Boost    `json:"boost,omitempty"`
}

func NewSimilarityQuery(vector []float32) *SimilarityQuery {
	return &SimilarityQuery{Vector: vector}
}

func (q *SimilarityQuery) Field() string {
	return q.VectorField
}

func (q *SimilarityQuery) SetK(k int64) {
	q.K = k
}

func (q *SimilarityQuery) SetFieldVal(field string) {
	q.VectorField = field
}

func (q *SimilarityQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *SimilarityQuery) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *SimilarityQuery) Searcher(ctx context.Context, i index.IndexReader,
	m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {

	return searcher.NewSimilaritySearcher(ctx, i, m, options, q.VectorField,
		q.Vector, q.K, q.BoostVal.Value())
}
