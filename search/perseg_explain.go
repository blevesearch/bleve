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

// PerSegmentExplainer is implemented by the per segment searchers that can say
// how a doc they matched was scored.
//
// Explaining is not part of searching: no explanation is built while the matches
// are found and scored. Once a search has settled on the hits to return, the
// collector asks for the explanation of each of those, and only those -- a
// lookup of the doc in the postings of the terms, and the scores worked out
// again from what is found there. The score at the root of an explanation is the
// score the hit was ranked by.
type PerSegmentExplainer interface {
	// ExplainMatch explains the score of the doc (its number across the index,
	// in segment seg), and returns false if it is not a match. It may be asked
	// about any doc, once the searcher has been read.
	ExplainMatch(seg int, doc uint64) (explanation *Explanation, match bool, err error)
}
