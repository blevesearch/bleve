//  Copyright (c) 2025 Couchbase, Inc.
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

package fusion

import (
	"fmt"
	"sort"

	"github.com/blevesearch/bleve/v2/search"
)

// formatRSFMessage builds the explanation string associated with a single
// component of the Relative Score Fusion calculation.
func formatRSFMessage(weight float64, normalizedScore float64, minScore float64, maxScore float64) string {
	return fmt.Sprintf("rsf score (weight=%.3f, normalized=%.6f, min=%.6f, max=%.6f), normalized score of",
		weight, normalizedScore, minScore, maxScore)
}

// RelativeScoreFusion normalizes the best-scoring documents from the primary
// FTS query and each KNN query, scales those normalized values by the supplied
// weights, and combines them into a single fused score. Only the top
// `windowSize` documents per source are considered, and explanations are
// materialized lazily when requested.
func RelativeScoreFusion(hits search.DocumentMatchCollection, weights []float64, windowSize int, numKNNQueries int, explain bool) FusionResult {
	if len(hits) == 0 {
		return FusionResult{
			Hits:     hits,
			Total:    0,
			MaxScore: 0.0,
		}
	}

	scores := make([]float64, len(hits))

	var explChildren [][]*search.Explanation
	if explain {
		explChildren = make([][]*search.Explanation, len(hits))
	}

	scoringIdxs := make([]int, 0, len(hits))

	for idx, hit := range hits {
		if hit.Score != 0.0 {
			scoringIdxs = append(scoringIdxs, idx)
		}
	}

	if len(scoringIdxs) > 1 {
		sort.Slice(scoringIdxs, func(a, b int) bool {
			left := hits[scoringIdxs[a]]
			right := hits[scoringIdxs[b]]
			if left.Score == right.Score {
				return left.HitNumber < right.HitNumber
			}
			return left.Score > right.Score
		})
	}

	if limit := len(scoringIdxs); limit > 0 {
		if windowSize >= 0 && windowSize < limit {
			limit = windowSize
		}
		if limit < len(scoringIdxs) {
			scoringIdxs = scoringIdxs[:limit]
		}
	}

	if len(scoringIdxs) > 0 {
		max := hits[scoringIdxs[0]].Score
		min := hits[scoringIdxs[len(scoringIdxs)-1]].Score
		denom := max - min
		weight := weights[0]

		for _, idx := range scoringIdxs {
			hit := hits[idx]
			norm := 1.0
			if denom > 0 {
				norm = (hit.Score - min) / denom
			}

			scores[idx] += weight * norm

			if explain {
				expl := getFusionExplAt(
					hit,
					0,
					norm,
					formatRSFMessage(weight, norm, min, max),
				)
				explChildren[idx] = append(explChildren[idx], expl)
			}
		}
	}

	knnDocs := make([]docScore, 0, len(hits))

	for queryIdx := 0; queryIdx < numKNNQueries; queryIdx++ {
		knnDocs = knnDocs[:0]

		for idx, hit := range hits {
			if hit.ScoreBreakdown == nil {
				continue
			}
			if score, ok := hit.ScoreBreakdown[queryIdx]; ok {
				knnDocs = append(knnDocs, docScore{
					idx:   idx,
					score: score,
				})
			}
		}

		if len(knnDocs) == 0 {
			continue
		}

		sortDocScores(knnDocs, hits)

		limit := len(knnDocs)
		if windowSize >= 0 && windowSize < limit {
			limit = windowSize
		}

		if limit == 0 {
			continue
		}

		max := knnDocs[0].score
		min := knnDocs[limit-1].score
		denom := max - min
		weight := weights[queryIdx+1]

		for i := 0; i < limit; i++ {
			entry := knnDocs[i]
			norm := 1.0
			if denom > 0 {
				norm = (entry.score - min) / denom
			}

			scores[entry.idx] += weight * norm

			if explain {
				hit := hits[entry.idx]
				expl := getFusionExplAt(
					hit,
					queryIdx+1,
					norm,
					formatRSFMessage(weight, norm, min, max),
				)
				explChildren[entry.idx] = append(explChildren[entry.idx], expl)
			}
		}
	}

	var maxScore float64
	for idx, hit := range hits {
		score := scores[idx]
		hit.Score = score
		if score > maxScore {
			maxScore = score
		}
		if explain && len(explChildren[idx]) > 0 {
			finalizeFusionExpl(hit, explChildren[idx])
		}
		hit.ScoreBreakdown = nil
	}

	sortDocMatchesByScore(hits)

	if len(hits) > windowSize {
		hits = hits[:windowSize]
	}

	return FusionResult{
		Hits:     hits,
		Total:    uint64(len(hits)),
		MaxScore: maxScore,
	}
}
