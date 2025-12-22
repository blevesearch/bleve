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

// Using the same term splitter slice used in the doc values in zap.
// TODO: This needs to be revisited whenever we change the zap
// implementation of doc values.
var termSeparatorSplitSlice = []byte{0xff}

func buildRelationFilterOnShapes(ctx context.Context, dvReader index.DocValueReader, field string,
	relation string, shape index.GeoJSON,
) FilterFunc {
	// this is for accumulating the shape's actual complete value
	// spread across multiple docvalue visitor callbacks.
	var dvShapeValue []byte
	var startReading, finishReading, found bool
	var reader *bytes.Reader

	var bufPool *s2.GeoBufferPool
	if bufPoolCallback, ok := ctx.Value(search.GeoBufferPoolCallbackKey).(search.GeoBufferPoolCallbackFunc); ok {
		bufPool = bufPoolCallback()
	}

	dvVisitor := func(_ string, term []byte) {
		if found {
			// avoid redundant work if already found
			return
		}
		tl := len(term)
		// only consider the values which are GlueBytes prefixed or
		// if it had already started reading the shape bytes from previous callbacks.
		if startReading || tl > geo.GlueBytesOffset {

			if !startReading && bytes.Equal(geo.GlueBytes, term[:geo.GlueBytesOffset]) {
				startReading = true

				if bytes.Equal(geo.GlueBytes, term[tl-geo.GlueBytesOffset:]) {
					term = term[:tl-geo.GlueBytesOffset]
					finishReading = true
				}

				dvShapeValue = append(dvShapeValue, term[geo.GlueBytesOffset:]...)

			} else if startReading && !finishReading {
				if tl > geo.GlueBytesOffset &&
					bytes.Equal(geo.GlueBytes, term[tl-geo.GlueBytesOffset:]) {
					term = term[:tl-geo.GlueBytesOffset]
					finishReading = true
				}

				dvShapeValue = append(dvShapeValue, termSeparatorSplitSlice...)
				dvShapeValue = append(dvShapeValue, term...)
			}

			// apply the filter once the entire docvalue is finished reading.
			if finishReading {
				v, err := geojson.FilterGeoShapesOnRelation(shape, dvShapeValue, relation, &reader, bufPool)
				if err == nil && v {
					found = true
				}

				dvShapeValue = dvShapeValue[:0]
				startReading = false
				finishReading = false
			}
		}
	}

	return func(sctx *search.SearchContext, d *search.DocumentMatch) bool {
		// reset state variables for each document
		found = false
		startReading = false
		finishReading = false
		dvShapeValue = dvShapeValue[:0]
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
