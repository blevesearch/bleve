//  Copyright (c) 2022 Couchbase, Inc.
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
	"bytes"
	"context"

	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
	"github.com/blevesearch/geo/geojson"
	"github.com/blevesearch/geo/s2"
)

func NewGeoShapeSearcher(ctx context.Context, indexReader index.IndexReader, shape index.GeoJSON,
	relation string, field string, boost float64,
	options search.SearcherOptions,
) (search.Searcher, error) {
	var err error
	var spatialPlugin index.SpatialAnalyzerPlugin

	// check for the spatial plugin from the index.
	if sr, ok := indexReader.(index.SpatialIndexPlugin); ok {
		spatialPlugin, _ = sr.GetSpatialAnalyzerPlugin("s2")
	}

	if spatialPlugin == nil {
		// fallback to the default spatial plugin(s2).
		spatialPlugin = geo.GetSpatialAnalyzerPlugin("s2")
	}

	// obtain the query tokens.
	terms := spatialPlugin.GetQueryTokens(shape)
	mSearcher, err := NewMultiTermSearcher(ctx, indexReader, terms,
		field, boost, options, false)
	if err != nil {
		return nil, err
	}

	dvReader, err := indexReader.DocValueReader([]string{field})
	if err != nil {
		return nil, err
	}

	return NewFilteringSearcher(ctx, mSearcher, buildRelationFilterOnShapes(ctx, dvReader, field, relation, shape)), nil
}

func buildRelationFilterOnShapes(ctx context.Context, dvReader index.DocValueReader, field string,
	relation string, shape index.GeoJSON,
) FilterFunc {
	// this is for accumulating the shape's actual complete value
	// spread across multiple docvalue visitor callbacks.
	var reader *bytes.Reader
	var found bool

	var bufPool *s2.GeoBufferPool
	if bufPoolCallback, ok := ctx.Value(search.GeoBufferPoolCallbackKey).(search.GeoBufferPoolCallbackFunc); ok {
		bufPool = bufPoolCallback()
	}

	dvVisitor := func(_ string, term []byte) {
		if len(term) > 4 && bytes.Equal(term[0:2], geo.GlueBytes) && bytes.Equal(term[len(term)-2:], geo.GlueBytes) {
			v, err := geojson.FilterGeoShapesOnRelation(shape, term[2:len(term)-2], relation, &reader, bufPool)
			if err == nil && v {
				found = true
			}
		}
	}

	return func(sctx *search.SearchContext, d *search.DocumentMatch) bool {
		// reset state variables for each document
		found = false
		if err := dvReader.VisitDocValues(d.IndexInternalID, dvVisitor); err == nil && found {
			bytes := dvReader.BytesRead()
			if bytes > 0 {
				reportIOStats(ctx, bytes)
				search.RecordSearchCost(ctx, search.AddM, bytes)
			}
			return found
		}

		return false
	}
}
