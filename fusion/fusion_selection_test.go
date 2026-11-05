// Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fusion

import (
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

func TestFusionSelectionBufferBoundary(t *testing.T) {
	for _, size := range []int{32, 33} {
		for _, mode := range []string{"rrf", "rsf"} {
			t.Run(fmt.Sprintf("%s/%d", mode, size), func(t *testing.T) {
				hits := make(search.DocumentMatchCollection, size)
				for i := range hits {
					hits[i] = &search.DocumentMatch{
						ID: fmt.Sprint(i), HitNumber: uint64(i),
						ScoreBreakdown: map[int]float64{0: float64(i + 1)},
					}
				}
				// runSelectionFusion uses rankConstant=1; every candidate is
				// included so the normalization endpoints are 1 and size.
				result := runSelectionFusion(mode, hits, []float64{0, 1}, size, 1, false)
				if len(result.Hits) != size || result.Total != uint64(size) {
					t.Fatalf("result length/total = %d/%d, want %d", len(result.Hits), result.Total, size)
				}
				for rank, hit := range result.Hits {
					want := 1 / float64(rank+2)
					if mode == "rsf" {
						want = float64(size-rank-1) / float64(size-1)
					}
					if hit.ID != fmt.Sprint(size-rank-1) || math.Abs(hit.Score-want) > 1e-12 || hit.ScoreBreakdown != nil {
						t.Errorf("rank %d: got %q/%v, want %q/%v with cleared breakdown", rank, hit.ID, hit.Score, fmt.Sprint(size-rank-1), want)
					}
				}
			})
		}
	}
}

func TestSortDocMatchesByBreakdownSelection(t *testing.T) {
	tests := []struct {
		name   string
		hits   search.DocumentMatchCollection
		query  int
		ids    []string
		scores []float64
	}{
		{name: "empty"},
		{
			name:   "single present zero",
			hits:   search.DocumentMatchCollection{{ID: "zero", ScoreBreakdown: map[int]float64{0: 0}}},
			ids:    []string{"zero"},
			scores: []float64{0},
		},
		{
			name: "single missing score",
			hits: search.DocumentMatchCollection{{ID: "missing"}},
			ids:  []string{"missing"},
		},
		{
			name: "missing scores use hit number and preserve equal keys",
			hits: search.DocumentMatchCollection{
				{ID: "late", HitNumber: 3},
				{ID: "early", HitNumber: 1, ScoreBreakdown: map[int]float64{}},
				{ID: "tied", HitNumber: 1, ScoreBreakdown: map[int]float64{1: 99}},
				{ID: "middle", HitNumber: 2},
			},
			ids: []string{"early", "tied", "middle", "late"},
		},
		{
			name: "zero and negative scores precede missing scores",
			hits: search.DocumentMatchCollection{
				{ID: "missing", HitNumber: 0},
				{ID: "negative", HitNumber: 4, ScoreBreakdown: map[int]float64{0: -2}},
				{ID: "zero", HitNumber: 2, ScoreBreakdown: map[int]float64{0: 0}},
				{ID: "positive", HitNumber: 5, ScoreBreakdown: map[int]float64{0: 3}},
				{ID: "empty", HitNumber: 1, ScoreBreakdown: map[int]float64{}},
				{ID: "other", HitNumber: 3, ScoreBreakdown: map[int]float64{1: 100}},
			},
			ids:    []string{"positive", "zero", "negative", "missing", "empty", "other"},
			scores: []float64{3, 0, -2},
		},
		{
			name: "present scores use hit number and preserve equal keys",
			hits: search.DocumentMatchCollection{
				{ID: "later", HitNumber: 2, ScoreBreakdown: map[int]float64{1: 7}},
				{ID: "first", HitNumber: 1, ScoreBreakdown: map[int]float64{1: 7}},
				{ID: "second", HitNumber: 1, ScoreBreakdown: map[int]float64{1: 7}},
				{ID: "lower", HitNumber: 0, ScoreBreakdown: map[int]float64{1: 3}},
			},
			query:  1,
			ids:    []string{"first", "second", "later", "lower"},
			scores: []float64{7, 7, 7, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := append(search.DocumentMatchCollection(nil), tt.hits...)
			for i, hit := range original {
				hit.Score = float64(i + 10)
			}
			selected := sortDocMatchesByBreakdown(tt.hits, tt.query, make([]scoredDocumentMatch, len(tt.hits)))
			assertSelectionIDs(t, tt.hits, tt.ids)
			if len(selected) != len(tt.scores) {
				t.Fatalf("scored prefix length = %d, want %d", len(selected), len(tt.scores))
			}
			for i, candidate := range selected {
				if candidate.hit != tt.hits[i] || candidate.score != tt.scores[i] {
					t.Errorf("scored prefix[%d] = %+v, want hit %q with score %v", i, candidate, tt.hits[i].ID, tt.scores[i])
				}
			}
			for i, hit := range original {
				if hit.Score != float64(i+10) {
					t.Errorf("sorting changed primary score of %q", hit.ID)
				}
			}
		})
	}
}

func TestSortDocMatchesByBreakdownSelectionNaN(t *testing.T) {
	hits := search.DocumentMatchCollection{
		{ID: "nan", Score: 10, ScoreBreakdown: map[int]float64{0: math.NaN()}},
		{ID: "one", Score: 11, ScoreBreakdown: map[int]float64{0: 1}},
		{ID: "two", Score: 12, ScoreBreakdown: map[int]float64{0: 2}},
		{ID: "missing", Score: 13},
	}
	original := append(search.DocumentMatchCollection(nil), hits...)
	selected := sortDocMatchesByBreakdown(hits, 0, make([]scoredDocumentMatch, len(hits)))
	// The legacy stable sort keeps NaN ahead of the finite scores for this
	// input, while still ordering the finite scores and placing missing last.
	assertSelectionIDs(t, hits, []string{"nan", "two", "one", "missing"})
	if len(selected) != 3 {
		t.Fatalf("scored prefix length = %d, want 3", len(selected))
	}
	if !math.IsNaN(selected[0].score) || selected[1].score != 2 || selected[2].score != 1 {
		t.Errorf("cached scores do not match the fallback order: %+v", selected)
	}
	for i, candidate := range selected {
		if candidate.hit != hits[i] {
			t.Errorf("scored prefix[%d] does not reference the corresponding input hit", i)
		}
	}
	for i, hit := range original {
		if hit.Score != float64(i+10) {
			t.Errorf("sorting changed primary score of %q", hit.ID)
		}
	}
}

func TestSortDocMatchesByBreakdownSelectionScratchReuse(t *testing.T) {
	hits := selectionStableTailHits()
	scratch := make([]scoredDocumentMatch, len(hits))
	selected := sortDocMatchesByBreakdown(hits, 0, scratch)
	assertSelectionIDs(t, hits, []string{"c", "a", "e", "b", "d"})
	if len(selected) != 5 {
		t.Fatalf("first scored prefix length = %d, want 5", len(selected))
	}

	// The tail outside one source's window still determines the stable order
	// of equal-scoring hits in the next source. Reuse a previously full buffer.
	selected = sortDocMatchesByBreakdown(hits, 1, scratch)
	assertSelectionIDs(t, hits, []string{"e", "b", "d", "c", "a"})
	if len(selected) != 3 {
		t.Fatalf("second scored prefix length = %d, want 3", len(selected))
	}
	for i, candidate := range selected {
		if candidate.hit != hits[i] || candidate.score != 9 {
			t.Errorf("reused scored prefix[%d] = %+v", i, candidate)
		}
	}

	selected = sortDocMatchesByBreakdown(hits, 2, scratch)
	assertSelectionIDs(t, hits, []string{"e", "b", "d", "c", "a"})
	if len(selected) != 0 {
		t.Errorf("absent query retained %d stale candidates", len(selected))
	}
}

func TestFusionSelectionStableTail(t *testing.T) {
	for _, mode := range []string{"rrf", "rsf"} {
		t.Run(mode, func(t *testing.T) {
			hits := selectionStableTailHits()
			result := runSelectionFusion(mode, hits, []float64{0, 0.2, 0.8}, 2, 2, false)
			assertSelectionIDs(t, result.Hits, []string{"e", "b"})
			if result.Total != 2 {
				t.Errorf("Total = %d, want 2", result.Total)
			}
			want := []float64{0.8 / 2, 0.8 / 3}
			if mode == "rsf" {
				want = []float64{0.8, 0.8}
			}
			for i, hit := range result.Hits {
				assertSelectionScore(t, hit.ID, hit.Score, want[i])
			}
			assertSelectionScore(t, "MaxScore", result.MaxScore, want[0])
		})
	}
}

func TestRelativeScoreFusionSelectionWindow(t *testing.T) {
	for _, tt := range []struct {
		name   string
		scores []float64
		window int
		want   []float64
	}{
		{"exclude outlier from normalization", []float64{100, 80, -1000}, 2, []float64{1, 0, 0}},
		{"identical selected scores", []float64{3, 3, -9}, 2, []float64{1, 1, 0}},
		{"single selected score", []float64{3, -9}, 1, []float64{1, 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			hits := make(search.DocumentMatchCollection, len(tt.scores))
			for i, score := range tt.scores {
				hits[i] = &search.DocumentMatch{HitNumber: uint64(i), ScoreBreakdown: map[int]float64{0: score}}
			}
			original := append(search.DocumentMatchCollection(nil), hits...)
			result := RelativeScoreFusion(hits, []float64{0, 1}, tt.window, 1, false)
			if len(result.Hits) != tt.window || result.Total != uint64(tt.window) {
				t.Fatalf("returned %d hits, Total %d; want %d", len(result.Hits), result.Total, tt.window)
			}
			for i, hit := range original {
				assertSelectionScore(t, "input score", hit.Score, tt.want[i])
				if hit.ScoreBreakdown != nil {
					t.Errorf("input[%d] retained its score breakdown", i)
				}
			}
			assertSelectionScore(t, "MaxScore", result.MaxScore, 1)
		})
	}
}

func TestFusionSelectionExplanationsAndMutation(t *testing.T) {
	for _, mode := range []string{"rrf", "rsf"} {
		t.Run(mode, func(t *testing.T) {
			hits := search.DocumentMatchCollection{
				{ID: "a", HitNumber: 0, Score: 4, ScoreBreakdown: map[int]float64{0: 0.2}},
				{ID: "b", HitNumber: 1, Score: 2, ScoreBreakdown: map[int]float64{0: 0.9, 1: 0.5}},
				{ID: "c", HitNumber: 2, Score: 0, ScoreBreakdown: map[int]float64{0: 0.7, 1: 0.8}},
				{ID: "d", HitNumber: 3, Score: 1},
			}
			original := append(search.DocumentMatchCollection(nil), hits...)
			children := make([][]*search.Explanation, len(hits))
			for i, hit := range hits {
				children[i] = []*search.Explanation{{Message: "fts"}, {Message: "knn 0"}, {Message: "knn 1"}}
				hit.Expl = &search.Explanation{Children: children[i]}
			}
			result := runSelectionFusion(mode, hits, []float64{0.2, 0.5, 0.3}, 2, 2, true)
			assertSelectionIDs(t, result.Hits, []string{"b", "c"})
			if result.Total != 2 || result.Hits[0] != hits[0] || result.Hits[1] != hits[1] {
				t.Fatal("fusion did not return the two-hit prefix of the mutated input")
			}

			wantScores := []float64{0.2 / 2, 0.2/3 + 0.5/2 + 0.3/3, 0.5/3 + 0.3/2, 0}
			wantChildren := [][]*search.Explanation{
				{{Value: 0.2 / 2, Message: "rrf score (weight=0.200, rank=1, rank_constant=1), normalized score of", Children: children[0][:1]}},
				{
					{Value: 0.2 / 3, Message: "rrf score (weight=0.200, rank=2, rank_constant=1), normalized score of", Children: children[1][:1]},
					{Value: 0.5 / 2, Message: "rrf score (weight=0.500, rank=1, rank_constant=1), normalized score of", Children: children[1][1:2]},
					{Value: 0.3 / 3, Message: "rrf score (weight=0.300, rank=2, rank_constant=1), normalized score of", Children: children[1][2:]},
				},
				{
					{Value: 0.5 / 3, Message: "rrf score (weight=0.500, rank=2, rank_constant=1), normalized score of", Children: children[2][1:2]},
					{Value: 0.3 / 2, Message: "rrf score (weight=0.300, rank=1, rank_constant=1), normalized score of", Children: children[2][2:]},
				},
				nil,
			}
			if mode == "rsf" {
				wantScores = []float64{0.2, 0.5, 0.3, 0}
				wantChildren = [][]*search.Explanation{
					{{Value: 1, Message: "rsf score (weight=0.200, normalized=1.000000, min=2.000000, max=4.000000), normalized score of", Children: children[0][:1]}},
					{
						{Value: 0, Message: "rsf score (weight=0.200, normalized=0.000000, min=2.000000, max=4.000000), normalized score of", Children: children[1][:1]},
						{Value: 1, Message: "rsf score (weight=0.500, normalized=1.000000, min=0.700000, max=0.900000), normalized score of", Children: children[1][1:2]},
						{Value: 0, Message: "rsf score (weight=0.300, normalized=0.000000, min=0.500000, max=0.800000), normalized score of", Children: children[1][2:]},
					},
					{
						{Value: 0, Message: "rsf score (weight=0.500, normalized=0.000000, min=0.700000, max=0.900000), normalized score of", Children: children[2][1:2]},
						{Value: 1, Message: "rsf score (weight=0.300, normalized=1.000000, min=0.500000, max=0.800000), normalized score of", Children: children[2][2:]},
					},
					nil,
				}
			}
			for i, hit := range original {
				assertSelectionScore(t, hit.ID, hit.Score, wantScores[i])
				if hit.ScoreBreakdown != nil {
					t.Errorf("%q retained its score breakdown", hit.ID)
				}
				assertSelectionScore(t, hit.ID+" explanation", hit.Expl.Value, wantScores[i])
				if hit.Expl.Message != "sum of" || len(hit.Expl.Children) != len(wantChildren[i]) {
					t.Fatalf("%q explanation = %s, want %d children", hit.ID, hit.Expl, len(wantChildren[i]))
				}
				for j, child := range hit.Expl.Children {
					want := wantChildren[i][j]
					assertSelectionScore(t, hit.ID+" contribution", child.Value, want.Value)
					if child.Message != want.Message {
						t.Errorf("%q explanation child %d message = %q, want %q", hit.ID, j, child.Message, want.Message)
					}
					if len(child.Children) != 1 || child.Children[0] != want.Children[0] {
						t.Errorf("%q explanation child %d did not retain the original explanation", hit.ID, j)
					}
				}
			}
			assertSelectionScore(t, "MaxScore", result.MaxScore, wantScores[1])
		})
	}
}

func TestFusionSelectionZeroWindow(t *testing.T) {
	for _, mode := range []string{"rrf", "rsf"} {
		t.Run(mode, func(t *testing.T) {
			hit := &search.DocumentMatch{Score: 2, ScoreBreakdown: map[int]float64{0: 3}}
			result := runSelectionFusion(mode, search.DocumentMatchCollection{hit}, []float64{1, 1}, 0, 1, false)
			if len(result.Hits) != 0 || result.Total != 0 || result.MaxScore != 0 {
				t.Errorf("zero window result = %+v", result)
			}
			if hit.Score != 2 || !reflect.DeepEqual(hit.ScoreBreakdown, map[int]float64{0: 3}) {
				t.Error("zero window changed the input hit")
			}
		})
	}
}

func selectionStableTailHits() search.DocumentMatchCollection {
	return search.DocumentMatchCollection{
		{ID: "a", ScoreBreakdown: map[int]float64{0: 4}},
		{ID: "b", ScoreBreakdown: map[int]float64{0: 2, 1: 9}},
		{ID: "c", ScoreBreakdown: map[int]float64{0: 5}},
		{ID: "d", ScoreBreakdown: map[int]float64{0: 1, 1: 9}},
		{ID: "e", ScoreBreakdown: map[int]float64{0: 3, 1: 9}},
	}
}

func runSelectionFusion(mode string, hits search.DocumentMatchCollection, weights []float64, window, queries int, explain bool) *FusionResult {
	if mode == "rrf" {
		return ReciprocalRankFusion(hits, weights, 1, window, queries, explain)
	}
	return RelativeScoreFusion(hits, weights, window, queries, explain)
}

func assertSelectionIDs(t *testing.T, hits search.DocumentMatchCollection, want []string) {
	t.Helper()
	if len(hits) != len(want) {
		t.Fatalf("got %d hits, want %d", len(hits), len(want))
	}
	for i, hit := range hits {
		if hit.ID != want[i] {
			t.Errorf("hit[%d] = %q, want %q", i, hit.ID, want[i])
		}
	}
}

func assertSelectionScore(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.IsNaN(got) || math.Abs(got-want) > 1e-12 {
		t.Errorf("%s = %.16g, want %.16g", name, got, want)
	}
}
