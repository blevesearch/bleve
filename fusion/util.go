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
	"cmp"
	"math"
	"slices"
	"sort"

	"github.com/blevesearch/bleve/v2/search"
)

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

// scoreBreakdownForQuery fetches the score for a specific KNN query index from
// the provided hit. The boolean return indicates whether the score is present.
func scoreBreakdownForQuery(hit *search.DocumentMatch, idx int) (float64, bool) {
	if hit == nil || hit.ScoreBreakdown == nil {
		return 0, false
	}

	score, ok := hit.ScoreBreakdown[idx]
	return score, ok
}

const smallFusionBufferSize = 32

type scoredDocumentMatch struct {
	hit   *search.DocumentMatch
	score float64
}

// sortDocMatchesByBreakdown caches the source scores and sorts present and
// missing hits separately. Both groups retain the current order for equal keys,
// including ties inherited from earlier sources. The entire input is reordered,
// since even hits outside the scoring window can affect ties in the next source.
// scratch must have room for every hit and can be reused for successive sources.
// The returned prefix contains the hits with a score for this source.
func sortDocMatchesByBreakdown(hits search.DocumentMatchCollection, queryIdx int, scratch []scoredDocumentMatch) []scoredDocumentMatch {
	present, missing := 0, len(hits)
	hasNaN := false
	for _, hit := range hits {
		score, ok := scoreBreakdownForQuery(hit, queryIdx)
		if ok {
			scratch[present] = scoredDocumentMatch{hit: hit, score: score}
			present++
			hasNaN = hasNaN || math.IsNaN(score)
		} else {
			missing--
			scratch[missing] = scoredDocumentMatch{hit: hit}
		}
	}

	if hasNaN {
		// NaN does not define a total order. Keep the original comparison and
		// sorting sequence rather than changing the behavior of such inputs.
		sortDocMatchesByBreakdownFallback(hits, queryIdx)
		present = 0
		for _, hit := range hits {
			score, ok := scoreBreakdownForQuery(hit, queryIdx)
			if !ok {
				break
			}
			scratch[present] = scoredDocumentMatch{hit: hit, score: score}
			present++
		}
		return scratch[:present]
	}

	// Missing hits were filled from the end, so restore their input order
	// before the stable sort. Equal HitNumbers must not be reversed.
	slices.Reverse(scratch[present:len(hits)])
	slices.SortStableFunc(scratch[:present], func(a, b scoredDocumentMatch) int {
		if a.score == b.score {
			return cmp.Compare(a.hit.HitNumber, b.hit.HitNumber)
		}
		if a.score > b.score {
			return -1
		}
		return 1
	})
	slices.SortStableFunc(scratch[present:len(hits)], func(a, b scoredDocumentMatch) int {
		return cmp.Compare(a.hit.HitNumber, b.hit.HitNumber)
	})
	for i := range hits {
		hits[i] = scratch[i].hit
	}
	return scratch[:present]
}

// sortDocMatchesByBreakdownFallback orders the hits in-place using the KNN score for
// the supplied query index (descending), breaking ties with `HitNumber` and
// placing hits without a score at the end.
func sortDocMatchesByBreakdownFallback(hits search.DocumentMatchCollection, queryIdx int) {
	if len(hits) < 2 {
		return
	}

	sort.SliceStable(hits, func(a, b int) bool {
		left := hits[a]
		right := hits[b]

		var leftScore float64
		leftOK := false
		if left != nil && left.ScoreBreakdown != nil {
			leftScore, leftOK = left.ScoreBreakdown[queryIdx]
		}

		var rightScore float64
		rightOK := false
		if right != nil && right.ScoreBreakdown != nil {
			rightScore, rightOK = right.ScoreBreakdown[queryIdx]
		}

		if leftOK && rightOK {
			if leftScore == rightScore {
				return left.HitNumber < right.HitNumber
			}
			return leftScore > rightScore
		}

		if leftOK != rightOK {
			return leftOK
		}

		return left.HitNumber < right.HitNumber
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
