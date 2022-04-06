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

	"github.com/blevesearch/bleve/v2/geo"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

func NewGeoShapeSearcher(indexReader index.IndexReader, shape index.GeoJSON,
	relation string, field string, boost float64,
	options search.SearcherOptions) (search.Searcher, error) {
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
	mSearcher, err := NewMultiTermSearcher(indexReader, terms,
		field, boost, options, false)
	if err != nil {
		return nil, err
	}

	dvReader, err := indexReader.DocValueReader([]string{field})
	if err != nil {
		return nil, err
	}

	return NewFilteringSearcher(mSearcher,
		buildRelationFilterOnShapes(dvReader, field, relation, shape)), nil

}

func buildRelationFilterOnShapes(dvReader index.DocValueReader, field string,
	relation string, shape index.GeoJSON) FilterFunc {
	return func(d *search.DocumentMatch) bool {
		var found bool

		err := dvReader.VisitDocValues(d.IndexInternalID,
			func(field string, term []byte) {
				// only consider the values which are GlueBytes prefixed.
				if len(term) > geo.GlueBytesOffset {
					if bytes.Equal(geo.GlueBytes, term[:geo.GlueBytesOffset]) {

						v, err := geo.FilterGeoShapesOnRelation(shape,
							term[geo.GlueBytesOffset:], relation)
						if err == nil && v {
							found = true
						}
					}
				}
			})

		if err == nil && found {
			return found
		}

		return false
	}
}
