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

// sortDocMatchIdxsByScore orders the provided index slice in-place based on the
// corresponding hit scores, breaking ties by `HitNumber`.
func sortDocMatchIdxsByScore(hits search.DocumentMatchCollection, idxs []int) {
	if len(idxs) < 2 {
		return
	}

	sort.Slice(idxs, func(a, b int) bool {
		left := hits[idxs[a]]
		right := hits[idxs[b]]
		if left.Score == right.Score {
			return left.HitNumber < right.HitNumber
		}
		return left.Score > right.Score
	})
}

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

	numRanks := numKNNQueries + 1

	sortedIdxs := make([]int, len(hits))
	for i := range hits {
		sortedIdxs[i] = i
	}

	sortDocMatchIdxsByScore(hits, sortedIdxs)

	limit := len(sortedIdxs)
	if windowSize >= 0 && windowSize < limit {
		limit = windowSize
	}

	scores := make([]float64, len(hits))

	var ranks []int
	if explain {
		ranks = make([]int, len(hits)*numRanks)
	}

	if limit > 0 {
		ftsWeight := weights[0]
		for i := 0; i < limit; i++ {
			docIdx := sortedIdxs[i]
			hit := hits[docIdx]
			if hit.Score == 0.0 {
				continue
			}
			rank := i + 1
			if explain {
				ranks[docIdx*numRanks] = rank
			}
			scores[docIdx] += ftsWeight / float64(rankConstant+rank)
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

		weight := weights[queryIdx+1]
		for rankIdx := 0; rankIdx < limit; rankIdx++ {
			entry := knnDocs[rankIdx]
			docIdx := entry.idx
			rank := rankIdx + 1
			if explain {
				ranks[docIdx*numRanks+(queryIdx+1)] = rank
			}
			scores[docIdx] += weight / float64(rankConstant+rank)
		}
	}

	var maxScore float64
	for idx, hit := range hits {
		hit.Score = scores[idx]
		hit.ScoreBreakdown = nil

		if hit.Score > maxScore {
			maxScore = hit.Score
		}

		if explain {
			base := idx * numRanks
			explChildren := make([]*search.Explanation, 0, numRanks)
			for i := 0; i < numRanks; i++ {
				rank := ranks[base+i]
				if rank == 0 {
					continue
				}
				partial := weights[i] / float64(rankConstant+rank)
				expl := getFusionExplAt(
					hit,
					i,
					partial,
					formatRRFMessage(weights[i], rank, rankConstant),
				)
				explChildren = append(explChildren, expl)
			}
			finalizeFusionExpl(hit, explChildren)
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
