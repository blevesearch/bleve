//  Copyright (c) 2024 Couchbase, Inc.
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
func ReciprocalRankFusion(hits search.DocumentMatchCollection, weights []float64, rank_constant int, window_size int, numKNNQueries int, explain bool) FusionResult {
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

	// Only a max of `window_size` elements need to be counted for. Stop
	// calculating rank once this threshold is hit.
	sort.Slice(hits, func(a, b int) bool {
		return scoreSortFunc()(hits[a], hits[b]) < 0
	})
	for i, hit := range hits {
		if hit.Score != 0.0 {
			// Skip if Score is 0, since that means the document was not
			// found as part of FTS, and only in KNN.
			ranks := make([]int, numKNNQueries+1)
			ranks[0] = i + 1
			docRanks[hit.ID] = ranks
		}

		if i == window_size-1 {
			// No need to calculate ranks from here
			break
		}
	}

	// For each KNN query, rank the documents based on their KNN score.
	for i := range numKNNQueries {
		// Create a slice of documents that have a score for this KNN query.
		knnDocs := make([]*search.DocumentMatch, 0, len(hits))
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
		for j, hit := range knnDocs {
			if ranks, ok := docRanks[hit.ID]; ok {
				ranks[i+1] = j + 1
			} else {
				// If document doesn't exist yet in docRanks, create it
				docRanks[hit.ID] = make([]int, numKNNQueries+1)
				docRanks[hit.ID][i+1] = j + 1
			}

			if j == window_size-1 {
				// No need to calculate ranks from here
				break
			}
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
		if ranks, ok := docRanks[hit.ID]; ok {
			for i, rank := range ranks {
				if rank > 0 {
					partialRrfScore := weights[i] * 1.0 / float64(rank_constant+rank)
					if explain {
						expl := getFusionExplAt(
							hit,
							i,
							partialRrfScore,
							formatRRFMessage(weights[i], rank, rank_constant),
						)
						explChildren = append(explChildren, expl)
					}
					rrfScore += partialRrfScore
				}
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
	if len(hits) > window_size {
		hits = hits[:window_size]
	}
	return FusionResult{
		Hits:     hits,
		Total:    uint64(len(hits)),
		MaxScore: maxScore,
	}
}
