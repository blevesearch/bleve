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

// How a composite's score is made of its clauses' scores. Every algorithm that
// scores a doc (the cursors, the windows, WAND, MaxScore) and the explanation
// of a doc work it out by these, so that a doc has the same score whichever
// found it, and the explanation shows the score the hit was ranked by, to the bit.
// The sum itself is always over the matching clauses in the order of the query.

// disjunctionCoord is the share of the n clauses of a disjunction that matched.
func disjunctionCoord(matched int, n float32) float32 {
	return float32(matched) / n
}

// disjunctionScore is the score of a doc that matched matched of the n clauses
// of a disjunction, the scores of those summing to sum.
func disjunctionScore(sum float32, matched int, n float32) float32 {
	return sum * disjunctionCoord(matched, n)
}
