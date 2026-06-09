//  Copyright (c) 2026 Couchbase, Inc.
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

package searcher

import (
	"sort"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

// newPivotFixture builds a DisjunctionSliceSearcher carrying just the state
// computeMAXSCOREPivot reads: the per-searcher MaxImpact values and their
// ascending argsort.
func newPivotFixture(impacts []float64) *DisjunctionSliceSearcher {
	order := make([]int, len(impacts))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		return impacts[order[a]] < impacts[order[b]]
	})
	return &DisjunctionSliceSearcher{
		searchers:      make([]search.Searcher, len(impacts)),
		wandMaxImpacts: impacts,
		maxscoreOrder:  order,
	}
}

// TestComputeMAXSCOREPivotInvariant is the soundness contract for the §8
// MAXSCORE partition.
//
// nextMAXSCORE iterates only the essential searchers, maxscoreOrder[pivotIdx:],
// and never calls Next() on the non-essential ones, maxscoreOrder[:pivotIdx].
// A document matching *only* non-essential terms is therefore never surfaced,
// so skipping it is sound only if it could not have beaten the threshold:
//
//	sum(MaxImpact of non-essential terms) <= threshold
//
// That is a bound on the PREFIX of the ascending order, not the suffix. If the
// pivot is derived from suffix sums instead, a low threshold (which is what a
// large Size produces) pushes the pivot too high and real results are dropped.
func TestComputeMAXSCOREPivotInvariant(t *testing.T) {
	cases := []struct {
		name    string
		impacts []float64
		thr     float64
	}{
		{"uniform/low threshold", []float64{1, 1, 1}, 0.5},
		{"uniform/mid threshold", []float64{1, 1, 1}, 2.5},
		{"one dominant term", []float64{1, 1, 10}, 2.5},
		{"graded impacts", []float64{0.1, 0.2, 0.3, 0.4, 0.5}, 0.35},
		{"near-equal impacts", []float64{0.3, 0.3, 0.3, 0.3, 0.3}, 0.31},
		{"threshold below smallest", []float64{0.4, 0.6, 0.9}, 0.2},
		{"threshold above total", []float64{0.1, 0.2}, 5.0},
		{"single term", []float64{0.7}, 0.3},
		{"unsorted input", []float64{0.9, 0.1, 0.5, 0.3}, 0.45},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newPivotFixture(tc.impacts)
			s.computeMAXSCOREPivot(tc.thr)

			n := len(tc.impacts)
			if s.pivotIdx < 0 || s.pivotIdx > n {
				t.Fatalf("pivotIdx=%d out of range [0,%d]", s.pivotIdx, n)
			}

			// Soundness: the non-essential prefix must not be able to beat
			// the threshold on its own.
			var nonEssential float64
			for _, si := range s.maxscoreOrder[:s.pivotIdx] {
				nonEssential += s.wandMaxImpacts[si]
			}
			if nonEssential > tc.thr {
				t.Errorf("UNSOUND: pivotIdx=%d leaves non-essential sum %.4f > threshold %.4f "+
					"— a doc matching only those terms is skipped but could score above it",
					s.pivotIdx, nonEssential, tc.thr)
			}

			// Maximality: pivotIdx should be as large as soundness allows, or
			// n when no document can beat the threshold at all.
			if s.pivotIdx < n {
				withNext := nonEssential + s.wandMaxImpacts[s.maxscoreOrder[s.pivotIdx]]
				if withNext <= tc.thr {
					t.Errorf("pivotIdx=%d is not maximal: including the next term gives "+
						"%.4f <= threshold %.4f, so it could also be non-essential",
						s.pivotIdx, withNext, tc.thr)
				}
			}
		})
	}
}

// TestComputeMAXSCOREPivotAllPrunable verifies the terminal signal: when even
// matching every term cannot beat the threshold, pivotIdx == len(maxscoreOrder),
// which Next() reads as "stop iterating, Total is now a lower bound".
func TestComputeMAXSCOREPivotAllPrunable(t *testing.T) {
	impacts := []float64{0.2, 0.3, 0.1}
	total := 0.6
	s := newPivotFixture(impacts)

	s.computeMAXSCOREPivot(total + 0.01) // threshold above the total
	if s.pivotIdx != len(impacts) {
		t.Errorf("threshold above total: pivotIdx=%d, want %d (all prunable)",
			s.pivotIdx, len(impacts))
	}

	s.computeMAXSCOREPivot(total - 0.01) // threshold just below the total
	if s.pivotIdx == len(impacts) {
		t.Errorf("threshold below total: pivotIdx=%d signals all-prunable, "+
			"but a doc matching every term would score above the threshold",
			s.pivotIdx)
	}
}
