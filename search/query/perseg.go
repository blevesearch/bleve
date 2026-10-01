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
// boolean field, a match of terms, a query string that is made of these, and
// conjunctions, disjunctions and boolean queries of these.
//
// It says nothing of the rest of the request, nor of the index.
func SupportsPerSegment(q Query) bool {
	switch q := q.(type) {
	case *TermQuery, *BoolFieldQuery:
		return true
	case *QueryStringQuery:
		// the string is parsed into a query, which is what is searched
		parsed, err := parseQuerySyntax(q.Query)
		return err == nil && SupportsPerSegment(parsed)
	case *BooleanQuery:
		// a filter is a searcher that is walked a doc at a time alongside
		if q.Filter != nil {
			return false
		}
		// the docs are those of the must clause or, without one, of the should
		// clause; what only excludes has nothing to start from
		if q.Must == nil && q.Should == nil {
			return false
		}
		for _, c := range []Query{q.Must, q.Should, q.MustNot} {
			if c != nil && !SupportsPerSegment(c) {
				return false
			}
		}
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
