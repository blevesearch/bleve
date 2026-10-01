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

package search

// PerSegmentSearchKey is a context key whose (bool) value, when true, tells a
// top level query that the caller is able to consume a per segment searcher
// (see search/searcher.PerSegmentTermSearcher and
// search/collector.PerSegmentTopNCollector), so the query may hand one out
// instead of a regular Searcher. It's only to be set for requests the per
// segment path is known to serve correctly, and only when the query being
// run is the top level one.
const PerSegmentSearchKey ContextKey = "_per_segment_search_key"
