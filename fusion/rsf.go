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

	var fusionExpl map[*search.DocumentMatch][]*search.Explanation
	if explain {
		fusionExpl = make(map[*search.DocumentMatch][]*search.Explanation, len(hits))
	}

	// Code here for calculating fts results
	// Sort by fts scores
	sortDocMatchesByScore(hits)

	// numScoring holds the total number of valid fts hits
	numScoring := 0
	for _, hit := range hits {
		if hit.Score == 0.0 {
			break
		}
		numScoring++
	}

	// ftsLimit is min(numScoring, windowSize)
	ftsLimit := numScoring
	if windowSize >= 0 && windowSize < ftsLimit {
		ftsLimit = windowSize
	}

	// calculate fts rank+scores
	if ftsLimit > 0 {
		max := hits[0].Score
		min := hits[ftsLimit-1].Score
		denom := max - min
		weight := weights[0]

		for i := 0; i < ftsLimit; i++ {
			hit := hits[i]
			original := hit.Score
			norm := 1.0
			if denom > 0 {
				norm = (original - min) / denom
			}
			contrib := weight * norm
			if explain {
				expl := getFusionExplAt(
					hit,
					0,
					norm,
					formatRSFMessage(weight, norm, min, max),
				)
				fusionExpl[hit] = append(fusionExpl[hit], expl)
			}
			hit.Score = contrib
		}
		for i := ftsLimit; i < len(hits); i++ {
			hits[i].Score = 0.0
		}
	} else {
		for _, hit := range hits {
			hit.Score = 0.0
		}
	}

	// Code from here is for calculating knn ranks+scores
	for queryIdx := 0; queryIdx < numKNNQueries; queryIdx++ {
		sortDocMatchesByBreakdown(hits, queryIdx)

		// numWithScore holds the total number of knn hits retrieved
		numWithScore := 0
		for numWithScore < len(hits) {
			if _, ok := scoreBreakdownForQuery(hits[numWithScore], queryIdx); !ok {
				break
			}
			numWithScore++
		}

		// limit holds the number of knn hits to consider
		limit := numWithScore
		if windowSize >= 0 && windowSize < limit {
			limit = windowSize
		}
		if limit == 0 {
			continue
		}

		max, _ := scoreBreakdownForQuery(hits[0], queryIdx)
		min, _ := scoreBreakdownForQuery(hits[limit-1], queryIdx)
		denom := max - min
		weight := weights[queryIdx+1]

		for i := 0; i < limit; i++ {
			hit := hits[i]
			score, _ := scoreBreakdownForQuery(hit, queryIdx)
			norm := 1.0
			if denom > 0 {
				norm = (score - min) / denom
			}
			contrib := weight * norm
			if explain {
				expl := getFusionExplAt(
					hit,
					queryIdx+1,
					norm,
					formatRSFMessage(weight, norm, min, max),
				)
				fusionExpl[hit] = append(fusionExpl[hit], expl)
			}
			hit.Score += contrib
		}
	}

	// Finalize scores
	var maxScore float64
	for _, hit := range hits {
		if explain {
			finalizeFusionExpl(hit, fusionExpl[hit])
		}
		if hit.Score > maxScore {
			maxScore = hit.Score
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
