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
	"math"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

const epsilon float64 = 1e-3

func nearlyEqual(a float64, b float64, epsilon float64) bool {
	return math.Abs(a-b) < epsilon
}

func compareFusionResults(a, b FusionResult) bool {
	if a.Total != b.Total || !nearlyEqual(a.MaxScore, b.MaxScore, epsilon) || len(a.Hits) != len(b.Hits) {
		return false
	}
	for i := range a.Hits {
		if a.Hits[i].ID != b.Hits[i].ID || !nearlyEqual(a.Hits[i].Score, b.Hits[i].Score, epsilon) {
			return false
		}

		if a.Hits[i].ScoreBreakdown != nil || b.Hits[i].ScoreBreakdown != nil {
			return false
		}
	}
	return true
}

func TestReciprocalRankFusion(t *testing.T) {
	tests := []struct {
		name          string
		hits          search.DocumentMatchCollection
		weights       []float64
		rank_constant int
		window_size   int
		numKNNQueries int
		want          FusionResult
	}{
		{
			name:          "empty hits",
			hits:          search.DocumentMatchCollection{},
			weights:       []float64{0.5, 0.5},
			rank_constant: 60,
			window_size:   10,
			numKNNQueries: 1,
			want: FusionResult{
				Hits:     search.DocumentMatchCollection{},
				Total:    0,
				MaxScore: 0.0,
			},
		},
		{
			name: "single knn query",
			hits: search.DocumentMatchCollection{
				{ID: "a", Score: 0.9, ScoreBreakdown: map[int]float64{0: 0.8}},
				{ID: "b", Score: 0.8, ScoreBreakdown: map[int]float64{0: 0.9}},
				{ID: "c", Score: 0.7, ScoreBreakdown: map[int]float64{0: 0.7}},
			},
			weights:       []float64{0.4, 0.6},
			rank_constant: 1,
			window_size:   3,
			numKNNQueries: 1,
			want: FusionResult{
				Hits: search.DocumentMatchCollection{
					{ID: "b", Score: 0.433},
					{ID: "a", Score: 0.4},
					{ID: "c", Score: 0.25},
				},
				Total:    3,
				MaxScore: 0.433,
			},
		},
		{
			name: "multiple knn queries",
			hits: search.DocumentMatchCollection{
				{ID: "a", Score: 0.9, ScoreBreakdown: map[int]float64{0: 0.8, 1: 0.6}},
				{ID: "b", Score: 0.8, ScoreBreakdown: map[int]float64{0: 0.9, 1: 0.5}},
				{ID: "c", Score: 0.7, ScoreBreakdown: map[int]float64{0: 0.7, 1: 0.7}},
			},
			weights:       []float64{0.3, 0.4, 0.3},
			rank_constant: 1,
			window_size:   3,
			numKNNQueries: 2,
			want: FusionResult{
				Hits: search.DocumentMatchCollection{
					{ID: "a", Score: 0.383},
					{ID: "b", Score: 0.375},
					{ID: "c", Score: 0.325},
				},
				Total:    3,
				MaxScore: 0.383,
			},
		},
		{
			name: "window size smaller than hits",
			hits: search.DocumentMatchCollection{
				{ID: "a", Score: 0.9, ScoreBreakdown: map[int]float64{0: 0.7}},
				{ID: "b", Score: 0.8, ScoreBreakdown: map[int]float64{0: 0.9}},
				{ID: "c", Score: 0.7, ScoreBreakdown: map[int]float64{0: 0.8}},
			},
			weights:       []float64{0.4, 0.6},
			rank_constant: 1,
			window_size:   2,
			numKNNQueries: 1,
			want: FusionResult{
				Hits: search.DocumentMatchCollection{
					{ID: "b", Score: 0.433},
					{ID: "a", Score: 0.2},
				},
				Total:    2,
				MaxScore: 0.433,
			},
		},
		{
			name: "documents with partial scores missing KNN scores",
			hits: search.DocumentMatchCollection{
				{ID: "a", Score: 0.9, ScoreBreakdown: map[int]float64{0: 0.8}},         // has FTS and KNN query 0, missing KNN query 1
				{ID: "b", Score: 0.8, ScoreBreakdown: map[int]float64{1: 0.7}},         // has FTS and KNN query 1, missing KNN query 0
				{ID: "c", Score: 0.7, ScoreBreakdown: map[int]float64{0: 0.6, 1: 0.9}}, // has all scores
				{ID: "d", Score: 0.6, ScoreBreakdown: map[int]float64{}},               // has only FTS, missing all KNN scores
			},
			weights:       []float64{0.3, 0.4, 0.3}, // FTS, KNN query 0, KNN query 1
			rank_constant: 1,
			window_size:   4,
			numKNNQueries: 2,
			want: FusionResult{
				Hits: search.DocumentMatchCollection{
					{ID: "c", Score: 0.358}, // FTS rank 3, KNN0 rank 2, KNN1 rank 1: 0.3/4 + 0.4/3 + 0.3/2 = 0.075 + 0.133 + 0.15 = 0.358
					{ID: "a", Score: 0.35},  // FTS rank 1, KNN0 rank 1, no KNN1: 0.3/2 + 0.4/2 + 0 = 0.15 + 0.2 + 0 = 0.35
					{ID: "b", Score: 0.2},   // FTS rank 2, no KNN0, KNN1 rank 2: 0.3/3 + 0 + 0.3/3 = 0.1 + 0 + 0.1 = 0.2
					{ID: "d", Score: 0.06},  // FTS rank 4, no KNN0, no KNN1: 0.3/5 + 0 + 0 = 0.06
				},
				Total:    4,
				MaxScore: 0.358,
			},
		},
		{
			name: "documents with only KNN scores",
			hits: search.DocumentMatchCollection{
				{ID: "a", Score: 0.0, ScoreBreakdown: map[int]float64{0: 0.9}},         // no FTS rank (Score 0.0), only KNN query 0
				{ID: "b", Score: 0.0, ScoreBreakdown: map[int]float64{1: 0.8}},         // no FTS rank (Score 0.0), only KNN query 1
				{ID: "c", Score: 0.0, ScoreBreakdown: map[int]float64{0: 0.7, 1: 0.6}}, // no FTS rank (Score 0.0), both KNN queries
			},
			weights:       []float64{0.5, 0.3, 0.2}, // FTS, KNN query 0, KNN query 1
			rank_constant: 1,
			window_size:   3,
			numKNNQueries: 2,
			want: FusionResult{
				Hits: search.DocumentMatchCollection{
					{ID: "c", Score: 0.167}, // no FTS rank, KNN0 rank 2, KNN1 rank 2: 0 + 0.3/3 + 0.2/3 = 0 + 0.1 + 0.067 = 0.167
					{ID: "a", Score: 0.15},  // no FTS rank, KNN0 rank 1, no KNN1: 0 + 0.3/2 + 0 = 0 + 0.15 + 0 = 0.15
					{ID: "b", Score: 0.1},   // no FTS rank, no KNN0, KNN1 rank 1: 0 + 0 + 0.2/2 = 0 + 0 + 0.1 = 0.1
				},
				Total:    3,
				MaxScore: 0.167,
			},
		},
		{
			name: "mixed scenario with gaps in KNN queries",
			hits: search.DocumentMatchCollection{
				{ID: "a", Score: 0.8, ScoreBreakdown: map[int]float64{1: 0.9}}, // has FTS and KNN query 1, missing KNN query 0
				{ID: "b", Score: 0.6, ScoreBreakdown: map[int]float64{0: 0.8}}, // has FTS and KNN query 0, missing KNN query 1
				{ID: "c", Score: 0.0, ScoreBreakdown: map[int]float64{0: 0.7}}, // no FTS rank (Score 0.0), only KNN query 0
				{ID: "d", Score: 0.4, ScoreBreakdown: map[int]float64{}},       // only FTS, no KNN scores
			},
			weights:       []float64{0.4, 0.3, 0.3}, // FTS, KNN query 0, KNN query 1
			rank_constant: 1,
			window_size:   4,
			numKNNQueries: 2,
			want: FusionResult{
				Hits: search.DocumentMatchCollection{
					{ID: "a", Score: 0.35},  // FTS rank 1, no KNN0, KNN1 rank 1: 0.4/2 + 0 + 0.3/2 = 0.2 + 0 + 0.15 = 0.35
					{ID: "b", Score: 0.283}, // FTS rank 2, KNN0 rank 1, no KNN1: 0.4/3 + 0.3/2 + 0 = 0.133 + 0.15 + 0 = 0.283
					{ID: "d", Score: 0.1},   // FTS rank 3, no KNN0, no KNN1: 0.4/4 + 0 + 0 = 0.1
					{ID: "c", Score: 0.1},   // no FTS rank, KNN0 rank 2, no KNN1: 0 + 0.3/3 + 0 = 0 + 0.1 + 0 = 0.1
				},
				Total:    4,
				MaxScore: 0.35,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for i, hit := range tt.hits {
				hit.HitNumber = uint64(i)
			}

			if got := ReciprocalRankFusion(tt.hits, tt.weights, tt.rank_constant, tt.window_size, tt.numKNNQueries, false); !compareFusionResults(*got, tt.want) {
				t.Errorf("ReciprocalRankFusion() = %v, want %v", got, tt.want)
			}
		})
	}
}
