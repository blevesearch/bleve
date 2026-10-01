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

package query

// SupportsPerSegment reports whether the query is of a shape whose searchers
// can be per segment searchers (see search.PerSegmentSearchKey): a term, a
// match of terms, and conjunctions and disjunctions of these.
//
// It says nothing of the rest of the request, nor of the index.
func SupportsPerSegment(q Query) bool {
	switch q := q.(type) {
	case *TermQuery:
		return true
	case *MatchQuery:
		// a fuzzy match is made of fuzzy queries, not terms
		return q.Fuzziness == 0 && !q.autoFuzzy
	case *ConjunctionQuery:
		return allSupportPerSegment(q.Conjuncts)
	case *DisjunctionQuery:
		return !q.retrieveScoreBreakdown && allSupportPerSegment(q.Disjuncts)
	}
	return false
}

func allSupportPerSegment(qs []Query) bool {
	if len(qs) == 0 {
		return false
	}
	for _, q := range qs {
		if !SupportsPerSegment(q) {
			return false
		}
	}
	return true
}
