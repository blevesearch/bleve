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
	"fmt"

	"github.com/blevesearch/bleve/v2/search"
)

// formatRRFMessage builds the explanation string for a single component of the
// Reciprocal Rank Fusion calculation.
func formatRRFMessage(weight float64, rank int, rankConstant int) string {
	return fmt.Sprintf("rrf score (weight=%.3f, rank=%d, rank_constant=%d), normalized score of", weight, rank, rankConstant)
}

// ReciprocalRankFusion applies Reciprocal Rank Fusion across the primary FTS
// results and each KNN sub-query. Ranks are limited to `windowSize` per source,
// weighted, and combined into a single fused score, with optional explanation
// details.
func ReciprocalRankFusion(hits search.DocumentMatchCollection, weights []float64, rankConstant int, windowSize int, numKNNQueries int, explain bool) FusionResult {
	if len(hits) == 0 {
		return FusionResult{
			Hits:     hits,
			Total:    0,
			MaxScore: 0.0,
		}
	}

	// The code here mainly deals with obtaining rank/score for fts hits.
	// First sort hits by score
	sortDocMatchesByScore(hits)

	// Limit to consider only min(windowSize, len(hits))
	limit := len(hits)
	if windowSize >= 0 && windowSize < limit {
		limit = windowSize
	}

	// init explanations if required
	var fusionExpl map[*search.DocumentMatch][]*search.Explanation
	if explain {
		fusionExpl = make(map[*search.DocumentMatch][]*search.Explanation, len(hits))
	}

	// Calculate fts rank+scores
	if limit > 0 {
		ftsWeight := weights[0]
		for i := 0; i < limit; i++ {
			hit := hits[i]
			originalScore := hit.Score
			if originalScore == 0.0 {
				break
			}
			rank := i + 1
			if explain {
				contrib := ftsWeight / float64(rankConstant+rank)
				expl := getFusionExplAt(
					hit,
					0,
					contrib,
					formatRRFMessage(ftsWeight, rank, rankConstant),
				)
				fusionExpl[hit] = append(fusionExpl[hit], expl)
				hit.Score = contrib
			} else {
				hit.Score = ftsWeight / float64(rankConstant+rank)
			}
		}
		for i := limit; i < len(hits); i++ {
			hits[i].Score = 0.0
		}
	} else {
		for _, hit := range hits {
			hit.Score = 0.0
		}
	}

	// Code from here is to calculate knn ranks and scores
	// iterate over each knn query and calculate knn rank+scores
	for queryIdx := 0; queryIdx < numKNNQueries; queryIdx++ {
		limit := len(hits)
		if windowSize >= 0 && windowSize < limit {
			limit = windowSize
		}
		if limit == 0 {
			continue
		}

		sortDocMatchesByBreakdown(hits, queryIdx)

		weight := weights[queryIdx+1]
		rank := 0
		for _, hit := range hits {
			if _, ok := scoreBreakdownForQuery(hit, queryIdx); !ok {
				break
			}
			rank++
			contrib := weight / float64(rankConstant+rank)
			if explain {
				expl := getFusionExplAt(
					hit,
					queryIdx+1,
					contrib,
					formatRRFMessage(weight, rank, rankConstant),
				)
				fusionExpl[hit] = append(fusionExpl[hit], expl)
			}
			hit.Score += contrib
			if rank == limit {
				break
			}
		}
	}

	var maxScore float64
	for _, hit := range hits {
		if explain {
			finalizeFusionExpl(hit, fusionExpl[hit])
		}
		hit.ScoreBreakdown = nil

		if hit.Score > maxScore {
			maxScore = hit.Score
		}
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
