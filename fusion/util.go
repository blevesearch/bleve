//  Copyright (c) 2025 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fusion

import (
	"sort"

	"github.com/blevesearch/bleve/v2/search"
)

// docScore captures a score for a document index, letting callers reuse
// precomputed values without re-reading the match or its breakdown map.
type docScore struct {
	idx   int
	score float64
}

// sortDocMatchesByScore orders the provided collection in-place by the primary
// score in descending order, breaking ties with the original `HitNumber` to
// ensure deterministic output.
func sortDocMatchesByScore(hits search.DocumentMatchCollection) {
	if len(hits) < 2 {
		return
	}

	sort.Slice(hits, func(a, b int) bool {
		i := hits[a]
		j := hits[b]
		if i.Score == j.Score {
			return i.HitNumber < j.HitNumber
		}
		return i.Score > j.Score
	})
}

// sortDocScores orders the supplied `docScore` slice in descending score order
// while still breaking ties on `HitNumber`. It allows callers to sort cached
// score data without rebuilding intermediate `[]*DocumentMatch` slices.
func sortDocScores(scores []docScore, hits search.DocumentMatchCollection) {
	if len(scores) < 2 {
		return
	}

	sort.Slice(scores, func(a, b int) bool {
		i := scores[a]
		j := scores[b]
		if i.score == j.score {
			return hits[i.idx].HitNumber < hits[j.idx].HitNumber
		}
		return i.score > j.score
	})
}

// getFusionExplAt copies the existing explanation child at the requested index
// and wraps it in a new node describing how the fusion algorithm adjusted the
// score.
func getFusionExplAt(hit *search.DocumentMatch, i int, value float64, message string) *search.Explanation {
	return &search.Explanation{
		Value:    value,
		Message:  message,
		Children: []*search.Explanation{hit.Expl.Children[i]},
	}
}

// finalizeFusionExpl installs the collection of fusion explanation children and
// updates the root message so the caller sees the fused score as the sum of its
// parts.
func finalizeFusionExpl(hit *search.DocumentMatch, explChildren []*search.Explanation) {
	hit.Expl.Children = explChildren

	hit.Expl.Value = hit.Score
	hit.Expl.Message = "sum of"
}
