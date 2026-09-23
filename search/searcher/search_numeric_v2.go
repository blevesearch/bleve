//  Copyright (c) 2026 Couchbase, Inc.
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

package searcher

import (
	"context"
	"fmt"
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/scorer"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeNumericV2Searcher int

func init() {
	var nv2s NumericV2Searcher
	reflectStaticSizeNumericV2Searcher = int(reflect.TypeOf(nv2s).Size())
}

// NumericV2Searcher is a filtering searcher over a number_v2 field. Every
// document the underlying reader returns is a confirmed match, so the searcher
// contributes a constant score rather than a computed one.
type NumericV2Searcher struct {
	numericIndexReader index.NumericV2FieldReader
	scorer             *scorer.ConstantScorer

	nd index.NumericV2FieldDoc
}

func NewNumericV2Searcher(ctx context.Context, indexReader index.IndexReader,
	min, max *float64, inclusiveMin, inclusiveMax *bool, field string,
	boost float64, options search.SearcherOptions,
) (search.Searcher, error) {

	nr, ok := indexReader.(index.NumericV2IndexReader)
	if !ok {
		return nil, fmt.Errorf("indexReader does not support number_v2 queries")
	}

	// get the NumericV2FieldReader for the specified field
	numericIndexReader, err := nr.NumericV2FieldReader(ctx, field)
	if err != nil {
		return nil, err
	}

	// perform the search on the NumericV2FieldReader for the specified range
	err = numericIndexReader.Search(min, max, inclusiveMin, inclusiveMax)
	if err != nil {
		return nil, err
	}

	return &NumericV2Searcher{
		numericIndexReader: numericIndexReader,
		scorer:             scorer.NewConstantScorer(1, boost, options),
		nd:                 index.NumericV2FieldDoc{},
	}, nil
}

// Next returns the next document match, scored by the ConstantScorer.
func (n *NumericV2Searcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	match, err := n.numericIndexReader.Next(n.nd.Reset())
	if err != nil {
		return nil, err
	}
	if match == nil {
		return nil, nil
	}

	return n.scorer.Score(ctx, match.ID), nil
}

// Advance moves the searcher to the first document with an ID greater than or
// equal to the specified ID.
func (n *NumericV2Searcher) Advance(ctx *search.SearchContext,
	ID index.IndexInternalID) (*search.DocumentMatch, error) {
	match, err := n.numericIndexReader.Advance(ID, n.nd.Reset())
	if err != nil {
		return nil, err
	}
	if match == nil {
		return nil, nil
	}

	return n.scorer.Score(ctx, match.ID), nil
}

func (n *NumericV2Searcher) Close() error {
	return n.numericIndexReader.Close()
}

func (n *NumericV2Searcher) Count() uint64 {
	return n.numericIndexReader.Count()
}

func (n *NumericV2Searcher) DocumentMatchPoolSize() int {
	return 1
}

func (n *NumericV2Searcher) Min() int {
	return 0
}

func (n *NumericV2Searcher) SetQueryNorm(norm float64) {
	n.scorer.SetQueryNorm(norm)
}

func (n *NumericV2Searcher) Size() int {
	return reflectStaticSizeNumericV2Searcher + n.numericIndexReader.Size() +
		n.scorer.Size() + n.nd.Size()
}

func (n *NumericV2Searcher) Weight() float64 {
	return n.scorer.Weight()
}
