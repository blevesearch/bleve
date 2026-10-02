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

// A searcher of no matches can be a clause of a composite of the per segment
// kind: it has no matches in any segment. A match of nothing, say a text that
// analyzes to no tokens, is one, and it may be a clause of a query that is
// otherwise made of terms.

var _ perSegChild = (*MatchNoneSearcher)(nil)

// segCursor implements perSegChild.
func (s *MatchNoneSearcher) segCursor(seg int, scored bool) (docCursor, bool) {
	return nil, false
}

// segCursorSeeked implements perSegChild.
func (s *MatchNoneSearcher) segCursorSeeked(seg int, scored bool, seeks uint64) (docCursor, bool) {
	return nil, false
}

// segCost implements perSegChild.
func (s *MatchNoneSearcher) segCost(seg int) uint64 { return 0 }

// numSegments implements perSegChild.
func (s *MatchNoneSearcher) numSegments() int { return 0 }
