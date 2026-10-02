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

package bleve

import (
	"context"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/collector"
	"github.com/blevesearch/bleve/v2/search/query"
)

// perSegmentSearchEnabled is the switch of the per segment search path. It is
// on, and is turned off to compare the two paths.
var perSegmentSearchEnabled atomic.Bool

// perSegmentSearches counts the searches served by the per segment path.
var perSegmentSearches atomic.Uint64

// topNCollectorsBuilt counts the TopNCollectors built by searches: one for each
// search that the per segment path doesn't serve.
var topNCollectorsBuilt atomic.Uint64

func init() {
	perSegmentSearchEnabled.Store(true)
}

// perSegmentSearchEligible reports whether a search request may be served by
// the per segment search path: a per segment searcher feeding the per segment
// collector (see search/searcher.PerSegmentTermSearcher and
// search/collector.PerSegmentTopNCollector).
//
// That path produces the same hits as the regular one, but only for a narrow
// kind of request. It is limited to a term query, a match of terms, or a
// conjunction or disjunction of those, sorted by
// descending score, that needs nothing more than the score of each hit (or
// that there be none, with Score: "none"):
// no locations or highlights, facets, KNN, search after / before, pre search data,
// synonyms, nested documents or score fusion. (Explanations are fine, but for
// ones of a search that has no scores: the collector asks for those of the hits it
// returns once it has them.)
//
// Whether the index reader is able to provide per segment postings isn't
// decided here: if it can't, the query hands out a regular searcher, and the
// regular collector is the one that gets used.
func perSegmentSearchEligible(ctx context.Context, req *SearchRequest,
	fts search.FieldTermSynonymMap, fusion bool) bool {
	if !perSegmentSearchEnabled.Load() {
		return false
	}
	if !query.SupportsPerSegment(req.Query) {
		return false
	}
	if req.IncludeLocations || req.Highlight != nil {
		return false
	}
	// An explanation of scores that were asked not to be computed is a tree of
	// zeros in the regular path, and the per segment path would show the scores the
	// hits don't have.
	if req.Explain && req.Score == ScoreNone {
		return false
	}
	if len(req.Facets) > 0 || requestHasKNN(req) {
		return false
	}
	// SearchBefore is turned into SearchAfter before the collector is built
	if req.SearchAfter != nil || req.SearchBefore != nil || len(req.PreSearchData) > 0 {
		return false
	}
	if fts != nil || fusion {
		return false
	}
	if len(req.Sort) != 1 {
		return false
	}
	if byScore, ok := req.Sort[0].(*search.SortScore); !ok || !byScore.Desc {
		return false
	}
	if nestedMode, ok := ctx.Value(search.NestedSearchKey).(bool); ok && nestedMode {
		return false
	}
	// an application supplied document match handler expects DocumentMatches
	if ctx.Value(search.MakeDocumentMatchHandlerKey) != nil {
		return false
	}
	return true
}

// searchResultCollector is what a search needs from whichever collector runs
// it, TopNCollector or PerSegmentTopNCollector.
type searchResultCollector interface {
	search.Collector
	EarlyStopped() bool
	Size() int
}

// perSegmentCollector returns the collector to run if the searcher is a per
// segment one, and nil if the regular collector is the one to be used.
func perSegmentCollector(searcher search.Searcher, req *SearchRequest) searchResultCollector {
	if _, ok := searcher.(search.PerSegmentSearcher); !ok {
		return nil
	}
	perSegmentSearches.Add(1)
	rv := collector.NewPerSegmentTopNCollector(req.Size, req.From)
	rv.SetExplain(req.Explain)
	return rv
}
