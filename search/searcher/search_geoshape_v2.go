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
	geoShapeIndexReader index.GeoShapeV2FieldReader
	scorer              *scorer.ConstantScorer

	gd index.GeoShapeV2FieldDoc
}

func NewGeoShapeV2Searcher(ctx context.Context, indexReader index.IndexReader,
	shape index.GeoJSON, relation string, field string, boost float64,
	options search.SearcherOptions,
) (search.Searcher, error) {

	if gr, ok := indexReader.(index.GeoShapeV2IndexReader); ok {
		// get the GeoShapeV2FieldReader for the specified field
		geoShapeIndexReader, err := gr.GeoShapeV2FieldReader(ctx, field)
		if err != nil {
			return nil, err
		}

		// perform the search on the GeoShapeV2FieldReader with the specified
		// shape and relation
		err = geoShapeIndexReader.Search(shape, relation)
		if err != nil {
			return nil, err
		}

		return &GeoShapeV2Searcher{
			geoShapeIndexReader: geoShapeIndexReader,
			scorer:              scorer.NewConstantScorer(1, boost, options),
			gd:                  index.GeoShapeV2FieldDoc{},
		}, nil
	}
	return nil, nil
}

// Next returns the next document match for the GeoShapeV2Searcher.
// It retrieves the next matching document from the GeoShapeV2FieldReader
// and scores it using the ConstantScorer.
func (g *GeoShapeV2Searcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	match, err := g.geoShapeIndexReader.Next(g.gd.Reset())
	if err != nil {
		return nil, err
	}
	if match == nil {
		return nil, nil
	}

	docMatch := g.scorer.Score(ctx, match.ID)
	return docMatch, nil
}

// Advance moves the searcher to the first document with an ID greater than or equal to the specified ID.
// It retrieves the next matching document from the GeoShapeV2FieldReader and scores it using the ConstantScorer.
func (g *GeoShapeV2Searcher) Advance(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error) {
	knnMatch, err := g.geoShapeIndexReader.Advance(ID, g.gd.Reset())
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
	return g.geoShapeIndexReader.Close()
}

func (g *GeoShapeV2Searcher) Count() uint64 {
	return g.geoShapeIndexReader.Count()
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
	return reflectStaticSizeGeoShapeV2Searcher + g.geoShapeIndexReader.Size() +
		g.scorer.Size() + g.gd.Size()
}

func (g *GeoShapeV2Searcher) Weight() float64 {
	return g.scorer.Weight()
}
