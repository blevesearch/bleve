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

func formatRSFMessage(weight float64, normalizedScore float64, minScore float64, maxScore float64) string {
	return fmt.Sprintf("rsf score (weight=%.3f, normalized=%.6f, min=%.6f, max=%.6f), normalized score of",
		weight, normalizedScore, minScore, maxScore)
}

// RelativeScoreFusion normalizes scores based on min/max values for FTS and each KNN query, then applies weights.
func RelativeScoreFusion(hits search.DocumentMatchCollection, weights []float64, windowSize int, numKNNQueries int, explain bool) FusionResult {
	if len(hits) == 0 {
		return FusionResult{
			Hits:     hits,
			Total:    0,
			MaxScore: 0.0,
		}
	}

	rsfScores := make(map[string]float64)

	// contains the docs under consideration for scoring.
	// Reused for fts and knn hits
	scoringDocs := make([]*search.DocumentMatch, 0, len(hits))
	var explMap map[string][]*search.Explanation
	if explain {
		explMap = make(map[string][]*search.Explanation)
	}
	// remove non-fts hits
	for _, hit := range hits {
		if hit.Score != 0.0 {
			scoringDocs = append(scoringDocs, hit)
		}
	}
	// sort hits by fts score
	sort.Slice(scoringDocs, func(a, b int) bool {
		return scoreSortFunc()(scoringDocs[a], scoringDocs[b]) < 0
	})
	// Reslice to correct size
	if len(scoringDocs) > windowSize {
		scoringDocs = scoringDocs[:windowSize]
	}

	var min, max float64
	if len(scoringDocs) > 0 {
		min, max = scoringDocs[len(scoringDocs)-1].Score, scoringDocs[0].Score
	}

	for _, hit := range scoringDocs {
		var tempRsfScore float64
		if max > min {
			tempRsfScore = (hit.Score - min) / (max - min)
		} else {
			tempRsfScore = 1.0
		}

		if explain {
			// create and replace new explanation
			expl := getFusionExplAt(
				hit,
				0,
				tempRsfScore,
				formatRSFMessage(weights[0], tempRsfScore, min, max),
			)
			explMap[hit.ID] = append(explMap[hit.ID], expl)
		}

		rsfScores[hit.ID] = weights[0] * tempRsfScore
	}

	for i := range numKNNQueries {
		scoringDocs = scoringDocs[:0]
		for _, hit := range hits {
			if _, exists := hit.ScoreBreakdown[i]; exists {
				scoringDocs = append(scoringDocs, hit)
			}
		}

		sort.Slice(scoringDocs, func(a, b int) bool {
			return scoreBreakdownSortFunc(i)(scoringDocs[a], scoringDocs[b]) < 0
		})

		if len(scoringDocs) > windowSize {
			scoringDocs = scoringDocs[:windowSize]
		}

		if len(scoringDocs) > 0 {
			min, max = scoringDocs[len(scoringDocs)-1].ScoreBreakdown[i], scoringDocs[0].ScoreBreakdown[i]
		} else {
			min, max = 0.0, 0.0
		}

		for _, hit := range scoringDocs {
			var tempRsfScore float64
			if max > min {
				tempRsfScore = (hit.ScoreBreakdown[i] - min) / (max - min)
			} else {
				tempRsfScore = 1.0
			}

			if explain {
				expl := getFusionExplAt(
					hit,
					i+1,
					tempRsfScore,
					formatRSFMessage(weights[i+1], tempRsfScore, min, max),
				)
				explMap[hit.ID] = append(explMap[hit.ID], expl)
			}

			rsfScores[hit.ID] += weights[i+1] * tempRsfScore
		}
	}

	var maxScore float64
	for _, hit := range hits {
		if rsfScore, exists := rsfScores[hit.ID]; exists {
			hit.Score = rsfScore
			if rsfScore > maxScore {
				maxScore = rsfScore
			}
			if explain {
				finalizeFusionExpl(hit, explMap[hit.ID])
			}
		} else {
			hit.Score = 0.0
		}

		hit.ScoreBreakdown = nil
	}

	sort.Sort(hits)

	if len(hits) > windowSize {
		hits = hits[:windowSize]
	}

	return FusionResult{
		Hits:     hits,
		Total:    uint64(len(hits)),
		MaxScore: maxScore,
	}
}
