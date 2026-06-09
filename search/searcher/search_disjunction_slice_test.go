// Copyright (c) 2024 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package searcher

import "testing"

// TestComputeMAXSCOREPivot verifies the suffix-sum pivot calculation used by
// the MAXSCORE algorithm (§8).  The pivot is the smallest index i in
// maxscoreOrder such that sum(wandMaxImpacts[maxscoreOrder[i:]]) > threshold.
//
// Correctness contract (from the struct comment):
//   - pivot == 0              → all terms essential (brute-force WAND)
//   - pivot == len(searchers) → nothing can beat threshold; skip entirely
//   - otherwise terms at maxscoreOrder[:pivot] are non-essential
func TestComputeMAXSCOREPivot(t *testing.T) {
	// impacts sorted ascending: positions 0=low, 1=mid, 2=high
	s := &DisjunctionSliceSearcher{
		wandMaxImpacts: []float64{1.0, 2.0, 3.0},
		maxscoreOrder:  []int{0, 1, 2},
	}

	tests := []struct {
		threshold float64
		wantPivot int
	}{
		// suffix sum from index 2 = 3.0 > 2.5 → pivot=2 (only highest-impact term essential)
		{threshold: 2.5, wantPivot: 2},
		// suffix sum from index 2 = 3.0 NOT > 4.5; from index 1 = 5.0 > 4.5 → pivot=1
		{threshold: 4.5, wantPivot: 1},
		// suffix sum from index 2 = 3.0 NOT > 5.9; from 1 = 5.0 NOT; from 0 = 6.0 > 5.9 → pivot=0
		{threshold: 5.9, wantPivot: 0},
		// total sum 6.0 not > 10.0 → pivot=3 (nothing can beat threshold)
		{threshold: 10.0, wantPivot: 3},
		// threshold exactly equal to suffix sum is NOT > so we go left; 3.0 not > 3.0, 5.0 > 3.0 → pivot=1
		{threshold: 3.0, wantPivot: 1},
	}

	for _, tc := range tests {
		s.computeMAXSCOREPivot(tc.threshold)
		if s.pivotIdx != tc.wantPivot {
			t.Errorf("threshold=%.1f: pivotIdx=%d, want %d", tc.threshold, s.pivotIdx, tc.wantPivot)
		}
		if s.lastThreshold != tc.threshold {
			t.Errorf("threshold=%.1f: lastThreshold not updated (got %f)", tc.threshold, s.lastThreshold)
		}
	}
}

// TestComputeMAXSCOREPivotSingleTerm verifies the degenerate case of one searcher.
func TestComputeMAXSCOREPivotSingleTerm(t *testing.T) {
	s := &DisjunctionSliceSearcher{
		wandMaxImpacts: []float64{5.0},
		maxscoreOrder:  []int{0},
	}

	s.computeMAXSCOREPivot(4.9)
	if s.pivotIdx != 0 {
		t.Errorf("single term above threshold: pivotIdx=%d, want 0", s.pivotIdx)
	}

	s.computeMAXSCOREPivot(5.0)
	if s.pivotIdx != 1 {
		t.Errorf("single term equal to threshold (not >): pivotIdx=%d, want 1", s.pivotIdx)
	}

	s.computeMAXSCOREPivot(100.0)
	if s.pivotIdx != 1 {
		t.Errorf("single term below threshold: pivotIdx=%d, want 1", s.pivotIdx)
	}
}

// TestComputeMAXSCOREPivotNonNaturalOrder verifies that a non-identity
// maxscoreOrder (e.g. impacts not in sorted order by index) is handled correctly.
func TestComputeMAXSCOREPivotNonNaturalOrder(t *testing.T) {
	// impacts: searcher 0=3.0, 1=1.0, 2=2.0
	// maxscoreOrder sorted ascending by impact: [1, 2, 0] (1.0, 2.0, 3.0)
	s := &DisjunctionSliceSearcher{
		wandMaxImpacts: []float64{3.0, 1.0, 2.0},
		maxscoreOrder:  []int{1, 2, 0},
	}

	// suffix sum from index 2: impacts[maxscoreOrder[2]] = impacts[0] = 3.0 > 2.5 → pivot=2
	s.computeMAXSCOREPivot(2.5)
	if s.pivotIdx != 2 {
		t.Errorf("pivotIdx=%d, want 2", s.pivotIdx)
	}
}
