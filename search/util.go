//  Copyright (c) 2014 Couchbase, Inc.
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

package search

import (
	"context"

	"github.com/blevesearch/geo/s2"
)

func MergeLocations(locations []FieldTermLocationMap) FieldTermLocationMap {
	rv := locations[0]

	for i := 1; i < len(locations); i++ {
		nextLocations := locations[i]
		for field, termLocationMap := range nextLocations {
			rvTermLocationMap, rvHasField := rv[field]
			if rvHasField {
				rv[field] = MergeTermLocationMaps(rvTermLocationMap, termLocationMap)
			} else {
				rv[field] = termLocationMap
			}
		}
	}

	return rv
}

func MergeTermLocationMaps(rv, other TermLocationMap) TermLocationMap {
	for term, locationMap := range other {
		// for a given term/document there cannot be different locations
		// if they came back from different clauses, overwrite is ok
		rv[term] = locationMap
	}
	return rv
}

func MergeFieldTermLocations(dest []FieldTermLocation, matches []*DocumentMatch) []FieldTermLocation {
	n := len(dest)
	for _, dm := range matches {
		n += len(dm.FieldTermLocations)
	}
	if cap(dest) < n {
		dest = append(make([]FieldTermLocation, 0, n), dest...)
	}

	for _, dm := range matches {
		for _, ftl := range dm.FieldTermLocations {
			dest = append(dest, FieldTermLocation{
				Field: ftl.Field,
				Term:  ftl.Term,
				Location: Location{
					Pos:            ftl.Location.Pos,
					Start:          ftl.Location.Start,
					End:            ftl.Location.End,
					ArrayPositions: append(ArrayPositions(nil), ftl.Location.ArrayPositions...),
				},
			})
		}
	}

	return dest
}

const SearchIOStatsCallbackKey = "_search_io_stats_callback_key"

type SearchIOStatsCallbackFunc func(uint64)

// Implementation of SearchIncrementalCostCallbackFn should handle the following messages
//   - add: increment the cost of a search operation
//     (which can be specific to a query type as well)
//   - abort: query was aborted due to a cancel of search's context (for eg),
//     which can be handled differently as well
//   - done: indicates that a search was complete and the tracked cost can be
//     handled safely by the implementation.
type SearchIncrementalCostCallbackFn func(SearchIncrementalCostCallbackMsg,
	SearchQueryType, uint64)
type SearchIncrementalCostCallbackMsg uint
type SearchQueryType uint

const (
	Term = SearchQueryType(1 << iota)
	Geo
	Numeric
	GenericCost
)

const (
	AddM = SearchIncrementalCostCallbackMsg(1 << iota)
	AbortM
	DoneM
)

const SearchIncrementalCostKey = "_search_incremental_cost_key"
const QueryTypeKey = "_query_type_key"
const FuzzyMatchPhraseKey = "_fuzzy_match_phrase_key"
const IncludeScoreBreakdownKey = "_include_score_breakdown_key"

func RecordSearchCost(ctx context.Context,
	msg SearchIncrementalCostCallbackMsg, bytes uint64) {
	if ctx != nil {
		queryType, ok := ctx.Value(QueryTypeKey).(SearchQueryType)
		if !ok {
			// for the cost of the non query type specific factors such as
			// doc values and stored fields section.
			queryType = GenericCost
		}

		aggCallbackFn := ctx.Value(SearchIncrementalCostKey)
		if aggCallbackFn != nil {
			aggCallbackFn.(SearchIncrementalCostCallbackFn)(msg, queryType, bytes)
		}
	}
}

const GeoBufferPoolCallbackKey = "_geo_buffer_pool_callback_key"

// Assigning the size of the largest buffer in the pool to 24KB and
// the smallest buffer to 24 bytes. The pools are used to read a
// sequence of vertices which are always 24 bytes each.
const MaxGeoBufPoolSize = 24 * 1024
const MinGeoBufPoolSize = 24

type GeoBufferPoolCallbackFunc func() *s2.GeoBufferPool

const KnnPreSearchDataKey = "_knn_pre_search_data_key"

const PreSearchKey = "_presearch_key"

type ScoreExplCorrectionCallbackFunc func(queryMatch *DocumentMatch, knnMatch *DocumentMatch) (float64, *Explanation)

type SearcherStartCallbackFn func(size uint64) error
type SearcherEndCallbackFn func(size uint64) error

const SearcherStartCallbackKey = "_searcher_start_callback_key"
const SearcherEndCallbackKey = "_searcher_end_callback_key"
