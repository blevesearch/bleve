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
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

func TestRelativeScoreFusion(t *testing.T) {
	tests := []struct {
		name          string
		hits          search.DocumentMatchCollection
		weights       []float64
		windowSize    int
		numKNNQueries int
		want          FusionResult
	}{
		{
			name:          "empty hits",
			hits:          search.DocumentMatchCollection{},
			weights:       []float64{0.5, 0.5},
			windowSize:    10,
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
			windowSize:    3,
			numKNNQueries: 1,
			want: FusionResult{
				Hits: search.DocumentMatchCollection{
					{ID: "b", Score: 0.8}, // FTS: (0.8-0.7)/(0.9-0.7) * 0.4 + KNN: (0.9-0.7)/(0.9-0.7) * 0.6 = 0.2 + 0.6 = 0.8
					{ID: "a", Score: 0.7}, // FTS: (0.9-0.7)/(0.9-0.7) * 0.4 + KNN: (0.8-0.7)/(0.9-0.7) * 0.6 = 0.4 + 0.3 = 0.7
					{ID: "c", Score: 0.0}, // FTS: (0.7-0.7)/(0.9-0.7) * 0.4 + KNN: (0.7-0.7)/(0.9-0.7) * 0.6 = 0.0 + 0.0 = 0.0
				},
				Total:    3,
				MaxScore: 0.8,
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
			windowSize:    3,
			numKNNQueries: 2,
			want: FusionResult{
				Hits: search.DocumentMatchCollection{
					{ID: "a", Score: 0.65}, // FTS: (0.9-0.7)/(0.9-0.7)*0.3 + KNN0: (0.8-0.7)/(0.9-0.7)*0.4 + KNN1: (0.6-0.5)/(0.7-0.5)*0.3 = 1.0*0.3 + 0.5*0.4 + 0.5*0.3 = 0.65
					{ID: "b", Score: 0.55}, // FTS: (0.8-0.7)/(0.9-0.7)*0.3 + KNN0: (0.9-0.7)/(0.9-0.7)*0.4 + KNN1: (0.5-0.5)/(0.7-0.5)*0.3 = 0.5*0.3 + 1.0*0.4 + 0.0*0.3 = 0.55
					{ID: "c", Score: 0.3},  // FTS: (0.7-0.7)/(0.9-0.7)*0.3 + KNN0: (0.7-0.7)/(0.9-0.7)*0.4 + KNN1: (0.7-0.5)/(0.7-0.5)*0.3 = 0.0*0.3 + 0.0*0.4 + 1.0*0.3 = 0.3
				},
				Total:    3,
				MaxScore: 0.65,
			},
		},
		{
			name: "all scores identical should normalize to 1.0",
			hits: search.DocumentMatchCollection{
				{ID: "a", Score: 0.8, ScoreBreakdown: map[int]float64{0: 0.9}},
				{ID: "b", Score: 0.8, ScoreBreakdown: map[int]float64{0: 0.9}},
				{ID: "c", Score: 0.8, ScoreBreakdown: map[int]float64{0: 0.9}},
			},
			weights:       []float64{0.4, 0.6},
			windowSize:    3,
			numKNNQueries: 1,
			want: FusionResult{
				Hits: search.DocumentMatchCollection{
					{ID: "a", Score: 1.0}, // All scores identical: 1.0 * 0.4 + 1.0 * 0.6 = 1.0
					{ID: "b", Score: 1.0},
					{ID: "c", Score: 1.0},
				},
				Total:    3,
				MaxScore: 1.0,
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
			windowSize:    2,
			numKNNQueries: 1,
			want: FusionResult{
				Hits: search.DocumentMatchCollection{
					{ID: "b", Score: 0.6}, // Using top 2 for min/max: FTS min/max from [0.9,0.8] = [0.8,0.9], KNN min/max from [0.9,0.7] = [0.7,0.9]
					{ID: "a", Score: 0.4}, // FTS: (0.9-0.8)/(0.9-0.8) * 0.4 + KNN: (0.7-0.7)/(0.9-0.7) * 0.6 = 0.4 + 0 = 0.4
				},
				Total:    2,
				MaxScore: 0.6,
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
			windowSize:    4,
			numKNNQueries: 2,
			want: FusionResult{
				Hits: search.DocumentMatchCollection{
					{ID: "a", Score: 0.7}, // FTS: (0.9-0.6)/(0.9-0.6)*0.3 + KNN0: (0.8-0.6)/(0.8-0.6)*0.4 + KNN1: 0 = 1.0*0.3 + 1.0*0.4 + 0 = 0.7
					{ID: "c", Score: 0.4}, // FTS: (0.7-0.6)/(0.9-0.6)*0.3 + KNN0: (0.6-0.6)/(0.8-0.6)*0.4 + KNN1: (0.9-0.7)/(0.9-0.7)*0.3 = 0.33*0.3 + 0.0*0.4 + 1.0*0.3 = 0.1 + 0 + 0.3 = 0.4
					{ID: "b", Score: 0.2}, // FTS: (0.8-0.6)/(0.9-0.6)*0.3 + KNN0: 0 + KNN1: (0.7-0.7)/(0.9-0.7)*0.3 = 0.67*0.3 + 0 + 0.0*0.3 = 0.2 + 0 + 0 = 0.2
					{ID: "d", Score: 0.0}, // FTS: (0.6-0.6)/(0.9-0.6)*0.3 + KNN0: 0 + KNN1: 0 = 0.0*0.3 + 0 + 0 = 0
				},
				Total:    4,
				MaxScore: 0.7,
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
			windowSize:    3,
			numKNNQueries: 2,
			want: FusionResult{
				Hits: search.DocumentMatchCollection{
					{ID: "a", Score: 0.3}, // FTS: 0 + KNN0: 1.0 * 0.3 + KNN1: 0 = 0.3
					{ID: "b", Score: 0.2}, // FTS: 0 + KNN0: 0 + KNN1: 1.0 * 0.2 = 0.2
					{ID: "c", Score: 0.0}, // FTS: 0 + KNN0: 0 * 0.3 + KNN1: 0 * 0.2 = 0
				},
				Total:    3,
				MaxScore: 0.3,
			},
		},
		{
			name: "mixed scenario with different score ranges",
			hits: search.DocumentMatchCollection{
				{ID: "a", Score: 1.0, ScoreBreakdown: map[int]float64{0: 0.1}}, // high FTS, low KNN
				{ID: "b", Score: 0.1, ScoreBreakdown: map[int]float64{0: 1.0}}, // low FTS, high KNN
				{ID: "c", Score: 0.5, ScoreBreakdown: map[int]float64{0: 0.5}}, // mid FTS, mid KNN
			},
			weights:       []float64{0.5, 0.5}, // Equal weights
			windowSize:    3,
			numKNNQueries: 1,
			want: FusionResult{
				Hits: search.DocumentMatchCollection{
					{ID: "a", Score: 0.5},   // FTS: (1.0-0.1)/(1.0-0.1)*0.5 + KNN: (0.1-0.1)/(1.0-0.1)*0.5 = 1.0*0.5 + 0.0*0.5 = 0.5
					{ID: "b", Score: 0.5},   // FTS: (0.1-0.1)/(1.0-0.1)*0.5 + KNN: (1.0-0.1)/(1.0-0.1)*0.5 = 0.0*0.5 + 1.0*0.5 = 0.5
					{ID: "c", Score: 0.444}, // FTS: (0.5-0.1)/(1.0-0.1)*0.5 + KNN: (0.5-0.1)/(1.0-0.1)*0.5 = 0.444*0.5 + 0.444*0.5 = 0.444
				},
				Total:    3,
				MaxScore: 0.5,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RelativeScoreFusion(tt.hits, tt.weights, tt.windowSize, tt.numKNNQueries, false); !compareFusionResults(got, tt.want) {
				t.Errorf("RelativeScoreFusion() = %v, want %v", got, tt.want)
				// Print detailed comparison for debugging
				t.Logf("Got hits:")
				for i, hit := range got.Hits {
					t.Logf("  [%d] ID: %s, Score: %.6f", i, hit.ID, hit.Score)
				}
				t.Logf("Want hits:")
				for i, hit := range tt.want.Hits {
					t.Logf("  [%d] ID: %s, Score: %.6f", i, hit.ID, hit.Score)
				}
				t.Logf("Got Total: %d, MaxScore: %.6f", got.Total, got.MaxScore)
				t.Logf("Want Total: %d, MaxScore: %.6f", tt.want.Total, tt.want.MaxScore)
			}
		})
	}
}
