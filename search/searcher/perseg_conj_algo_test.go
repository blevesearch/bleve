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

import "testing"

// An AND is done by the candidates of its leader when the leader has far fewer
// postings than all the terms together (16 times fewer, by default), and by windows
// otherwise; the setting forces either.
func TestCandidateDrivenChoice(t *testing.T) {
	cursors := func(costs ...uint64) []*termCursor {
		rv := make([]*termCursor, len(costs))
		for i, c := range costs {
			rv[i] = &termCursor{cost: c}
		}
		return rv
	}
	cases := []struct {
		name  string
		costs []uint64
		want  bool
	}{
		{"a leader much rarer than the others", []uint64{1000, 4000000, 3800000}, true},
		{"a leader of 1% of the postings", []uint64{1000, 99000}, true},
		{"just below the ratio", []uint64{62, 1000}, true}, // 62*16 = 992 < 1062
		{"just above it", []uint64{70, 1000}, false},       // 70*16 = 1120 > 1070
		{"terms of about the same size", []uint64{500000, 600000, 550000}, false},
		{"the leader isn't the first", []uint64{4000000, 1000, 3800000}, true},
	}
	for _, c := range cases {
		if got := candidateDriven(cursors(c.costs...)); got != c.want {
			t.Errorf("%s %v: candidate driven %v, want %v", c.name, c.costs, got, c.want)
		}
	}
	if candidateDriven(cursors(10)) {
		t.Error("a lone term is not an AND")
	}

	restore := SetPerSegmentConjunctionAlgo("window", 0)
	if candidateDriven(cursors(1000, 4000000)) {
		t.Error("forced to windows, it was candidate driven")
	}
	restore()
	restore = SetPerSegmentConjunctionAlgo("candidate", 0)
	if !candidateDriven(cursors(500000, 600000)) {
		t.Error("forced to candidates, it was not")
	}
	restore()
	restore = SetPerSegmentConjunctionAlgo("auto", 2)
	// (the leader has to have fewer than half of the postings: 300 of 1300 does, 500 of 1000 doesn't)
	if candidateDriven(cursors(500, 500)) || !candidateDriven(cursors(300, 1000)) {
		t.Error("a ratio of 2 was not the ratio of the choice")
	}
	restore()
	if !candidateDriven(cursors(1000, 99000)) {
		t.Error("the setting wasn't put back")
	}
}
