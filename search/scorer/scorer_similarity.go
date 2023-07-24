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

package scorer

import (
	"math/rand"
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeSimilarityQueryScorer int

func init() {
	var sqs SimilarityQueryScorer
	reflectStaticSizeSimilarityQueryScorer = int(reflect.TypeOf(sqs).Size())
}

type SimilarityQueryScorer struct {
	queryVector  []float32
	queryField   string
	docTerm      uint64
	docTotal     uint64
	options      search.SearcherOptions
	includeScore bool
}

func NewSimilarityQueryScorer(queryVector []float32, queryField string,
	docTerm uint64, docTotal uint64, options search.SearcherOptions) *SimilarityQueryScorer {
	return &SimilarityQueryScorer{
		queryVector:  queryVector,
		queryField:   queryField,
		docTerm:      docTerm,
		docTotal:     docTotal,
		options:      options,
		includeScore: options.Score != "none",
	}
}

func (sqs *SimilarityQueryScorer) Score(ctx *search.SearchContext,
	similarityMatch *index.VectorDoc) *search.DocumentMatch {
	rv := ctx.DocumentMatchPool.Get()

	rv.IndexInternalID = append(rv.IndexInternalID, similarityMatch.ID...)

	// TODO Need to replace dummy score with actual score.

	if sqs.includeScore {
		rv.Score = float64(rand.Intn(50))
	}

	return rv
}

func (sqs *SimilarityQueryScorer) Weight() float64 {
	return 1
}
