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

package searcher

import (
	"context"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/scorer"
	index "github.com/blevesearch/bleve_index_api"
)

type SimilaritySearcher struct {
	field        string
	vector       []float32
	k            int64
	indexReader  index.IndexReader
	vectorReader index.VectorReader
	scorer       *scorer.SimilarityQueryScorer
	count        uint64
	vd           index.VectorDoc
}

func NewSimilaritySearcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping,
	options search.SearcherOptions, field string, vector []float32, k int64) (search.Searcher, error) {
	if vr, ok := i.(index.VectorIndexReader); ok {
		vectorReader, _ := vr.VectorReader(ctx, vector, field, k)

		count, err := i.DocCount()
		if err != nil {
			_ = vectorReader.Close()
			return nil, err
		}

		similarityScorer := scorer.NewSimilarityQueryScorer(vector, field,
			vectorReader.Count(), count, options)
		return &SimilaritySearcher{
			indexReader:  i,
			vectorReader: vectorReader,
			field:        field,
			vector:       vector,
			k:            k,
			scorer:       similarityScorer,
		}, nil
	}
	return nil, nil
}

func (s *SimilaritySearcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (
	*search.DocumentMatch, error) {
	return nil, nil
}

func (s *SimilaritySearcher) Close() error {
	return nil
}

// JUST DUMMY, NO-OP
func (s *SimilaritySearcher) Count() uint64 {
	return 0
}

// TODO JUST A NO-OP
func (s *SimilaritySearcher) DocumentMatchPoolSize() int {
	return 1
}

// TODO What does this mean?
func (s *SimilaritySearcher) Min() int {
	return 0
}

func (s *SimilaritySearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {

	similarityMatch, err := s.vectorReader.Next(s.vd.Reset())
	if err != nil {
		return nil, err
	}

	if similarityMatch == nil {
		return nil, nil
	}

	docMatch := s.scorer.Score(ctx, similarityMatch)

	return docMatch, nil

}

func (s *SimilaritySearcher) SetQueryNorm(qnorm float64) {
	// no-op
}

func (s *SimilaritySearcher) Size() int {
	return 0
}

func (s *SimilaritySearcher) Weight() float64 {
	return s.scorer.Weight()
}
