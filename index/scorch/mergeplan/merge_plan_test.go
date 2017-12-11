//  Copyright (c) 2017 Couchbase, Inc.
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

package mergeplan

import (
	"reflect"
	"testing"
)

// Implements the Segment interface for testing,
type segment struct {
	id       uint64
	fullSize int64
	liveSize int64
}

func (s *segment) Id() uint64      { return s.id }
func (s *segment) FullSize() int64 { return s.fullSize }
func (s *segment) LiveSize() int64 { return s.liveSize }

func makeLinearSegments(n int) (rv []Segment) {
	for i := 0; i < n; i++ {
		rv = append(rv, &segment{
			id:       uint64(i),
			fullSize: int64(i),
			liveSize: int64(i),
		})
	}
	return rv
}

func TestSimplePlan(t *testing.T) {
	segs := makeLinearSegments(10)

	tests := []struct {
		desc       string
		segments   []Segment
		expectPlan *MergePlan
		expectErr  error
	}{
		{"nil candidates",
			nil, nil, nil},
		{"empty candidates",
			[]Segment{}, nil, nil},
		{"1 candidate",
			[]Segment{segs[0]},
			nil,
			nil,
		},
		{"2 candidates",
			[]Segment{
				segs[0],
				segs[1],
			},
			&MergePlan{
				[]*MergeTask{
					&MergeTask{
						Segments: []Segment{
							segs[0],
							segs[1],
						},
					},
				},
			},
			nil,
		},
	}

	for testi, test := range tests {
		plan, err := Plan(test.segments, &DefaultMergePlanOptions)
		if err != test.expectErr {
			t.Errorf("testi: %d, test: %v, got err: %v",
				testi, test, err)
		}
		if !reflect.DeepEqual(plan, test.expectPlan) {
			t.Errorf("testi: %d, test: %v, got plan: %v",
				testi, test, plan)
		}
	}
}
