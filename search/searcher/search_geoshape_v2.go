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
	"reflect"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/scorer"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeGeoShapeV2Searcher int

func init() {
	var gsv2s GeoShapeV2Searcher
	reflectStaticSizeGeoShapeV2Searcher = int(reflect.TypeOf(gsv2s).Size())
}

type GeoShapeV2Searcher struct {
	geoCellReader index.GeoCellReader
	scorer        *scorer.ConstantScorer

	gd index.GeoCellFieldDoc
}

func NewGeoShapeV2Searcher(ctx context.Context, indexReader index.IndexReader, shape index.GeoJSON,
	relation string, field string, boost float64,
	options search.SearcherOptions,
) (search.Searcher, error) {

	if gr, ok := indexReader.(index.GeoCellIndexReader); ok {
		geoCellReader, err := gr.GeoCellReader(ctx, field)
		if err != nil {
			return nil, err
		}

		err = geoCellReader.Search(shape, relation)
		if err != nil {
			return nil, err
		}

		return &GeoShapeV2Searcher{
			geoCellReader: geoCellReader,
			scorer:        scorer.NewConstantScorer(1, boost, options),
			gd:            index.GeoCellFieldDoc{},
		}, nil
	}
	return nil, nil
}

func (g *GeoShapeV2Searcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	match, err := g.geoCellReader.Next(g.gd.Reset())
	if err != nil {
		return nil, err
	}
	if match == nil {
		return nil, nil
	}

	docMatch := g.scorer.Score(ctx, match.ID)
	return docMatch, nil
}

func (g *GeoShapeV2Searcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	knnMatch, err := g.geoCellReader.Advance(ID, g.gd.Reset())
	if err != nil {
		return nil, err
	}
	if knnMatch == nil {
		return nil, nil
	}

	docMatch := g.scorer.Score(ctx, knnMatch.ID)
	return docMatch, nil
}

func (g *GeoShapeV2Searcher) Close() error {
	return g.geoCellReader.Close()
}

func (g *GeoShapeV2Searcher) Count() uint64 {
	return g.geoCellReader.Count()
}

func (g *GeoShapeV2Searcher) DocumentMatchPoolSize() int {
	return 1
}

func (g *GeoShapeV2Searcher) Min() int {
	return 0
}

func (g *GeoShapeV2Searcher) SetQueryNorm(n float64) {
	g.scorer.SetQueryNorm(n)
}

func (g *GeoShapeV2Searcher) Size() int {
	return reflectStaticSizeGeoShapeV2Searcher + g.geoCellReader.Size() +
		g.scorer.Size() + g.gd.Size()
}

func (g *GeoShapeV2Searcher) Weight() float64 {
	return g.scorer.Weight()
}
