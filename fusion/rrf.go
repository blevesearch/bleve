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
	"sort"

	"github.com/blevesearch/bleve/v2/search"
)

func formatRRFMessage(weight float64, rank int, rankConstant int) string {
	return fmt.Sprintf("rrf score (weight=%.3f, rank=%d, rank_constant=%d), normalized score of", weight, rank, rankConstant)
}

// ReciprocalRankFusion performs a reciprocal rank fusion on the search results.
func ReciprocalRankFusion(hits search.DocumentMatchCollection, weights []float64, rankConstant int, windowSize int, numKNNQueries int, explain bool) FusionResult {
	if len(hits) == 0 {
		return FusionResult{
			Hits:     hits,
			Total:    0,
			MaxScore: 0.0,
		}
	}

	// Create a map of document ID to a slice of ranks.
	// The first element of the slice is the rank from the FTS search,
	// and the subsequent elements are the ranks from the KNN searches.
	docRanks := make(map[string][]int)

	// Pre-assign rank lists to each candidate document
	for _, hit := range hits {
		docRanks[hit.ID] = make([]int, numKNNQueries+1)
	}

	// Only a max of `window_size` elements need to be counted for. Stop
	// calculating rank once this threshold is hit.
	sort.Slice(hits, func(a, b int) bool {
		return scoreSortFunc()(hits[a], hits[b]) < 0
	})
	// Only consider top windowSize docs for rescoring
	for i := range min(windowSize, len(hits)) {
		if hits[i].Score != 0.0 {
			// Skip if Score is 0, since that means the document was not
			// found as part of FTS, and only in KNN.
			docRanks[hits[i].ID][0] = i + 1
		}
	}

	// Allocate knnDocs and reuse it within the loop
	knnDocs := make([]*search.DocumentMatch, 0, len(hits))

	// For each KNN query, rank the documents based on their KNN score.
	for i := range numKNNQueries {
		knnDocs = knnDocs[:0]

		for _, hit := range hits {
			if _, ok := hit.ScoreBreakdown[i]; ok {
				knnDocs = append(knnDocs, hit)
			}
		}

		// Sort the documents based on their score for this KNN query.
		sort.Slice(knnDocs, func(a, b int) bool {
			return scoreBreakdownSortFunc(i)(knnDocs[a], knnDocs[b]) < 0
		})

		// Update the ranks of the documents in the docRanks map.
		// Only consider top windowSize docs for rescoring.
		for j := range min(windowSize, len(knnDocs)) {
			docRanks[knnDocs[j].ID][i+1] = j + 1
		}
	}

	// Calculate the RRF score for each document.
	var maxScore float64
	for _, hit := range hits {
		var rrfScore float64
		var explChildren []*search.Explanation
		if explain {
			explChildren = make([]*search.Explanation, 0, numKNNQueries+1)
		}
		for i, rank := range docRanks[hit.ID] {
			if rank > 0 {
				partialRrfScore := weights[i] * 1.0 / float64(rankConstant+rank)
				if explain {
					expl := getFusionExplAt(
						hit,
						i,
						partialRrfScore,
						formatRRFMessage(weights[i], rank, rankConstant),
					)
					explChildren = append(explChildren, expl)
				}
				rrfScore += partialRrfScore
			}
		}
		hit.Score = rrfScore
		hit.ScoreBreakdown = nil
		if rrfScore > maxScore {
			maxScore = rrfScore
		}

		if explain {
			finalizeFusionExpl(hit, explChildren)
		}
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
