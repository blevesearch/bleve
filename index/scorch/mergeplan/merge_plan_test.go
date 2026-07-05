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
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"
)

// Implements the Segment interface for testing,
type segment struct {
	MyId       uint64
	MyFullSize int64
	MyLiveSize int64

	MyHasVector bool
	MyFileSize  int64
}

func (s *segment) Id() uint64      { return s.MyId }
func (s *segment) FullSize() int64 { return s.MyFullSize }
func (s *segment) LiveSize() int64 { return s.MyLiveSize }
func (s *segment) HasVector() bool { return s.MyHasVector }
func (s *segment) FileSize() int64 { return s.MyFileSize }
func (s *segment) LiveFileSize() int64 {
	fullSize := float64(s.MyFullSize)
	if fullSize <= 0 {
		return 0
	}
	liveSize := float64(s.MyLiveSize)
	if liveSize <= 0 {
		return 0
	}
	fileSize := float64(s.MyFileSize)
	if fileSize <= 0 {
		return 0
	}
	liveRatio := liveSize / fullSize
	return int64(fileSize * liveRatio)
}

func makeLinearSegments(n int) (rv []Segment) {
	for i := 0; i < n; i++ {
		rv = append(rv, &segment{
			MyId:       uint64(i),
			MyFullSize: int64(i),
			MyLiveSize: int64(i),
		})
	}
	return rv
}

// ----------------------------------------

func TestSimplePlan(t *testing.T) {
	segs := makeLinearSegments(10)

	tests := []struct {
		Desc       string
		Segments   []Segment
		Options    *MergePlanOptions
		ExpectPlan *MergePlan
		ExpectErr  error
	}{
		{
			"nil segments",
			nil, nil, nil, nil,
		},
		{
			"empty segments",
			[]Segment{},
			nil, nil, nil,
		},
		{
			"1 segment",
			[]Segment{segs[1]},
			nil,
			nil,
			nil,
		},
		{
			"2 segments",
			[]Segment{
				segs[1],
				segs[2],
			},
			nil,
			&MergePlan{
				Tasks: []*MergeTask{
					{
						Segments: []Segment{
							segs[2],
							segs[1],
						},
					},
				},
			},
			nil,
		},
		{
			"3 segments",
			[]Segment{
				segs[1],
				segs[2],
				segs[9],
			},
			nil,
			&MergePlan{
				Tasks: []*MergeTask{
					{
						Segments: []Segment{
							segs[9],
							segs[2],
							segs[1],
						},
					},
				},
			},
			nil,
		},
		{
			"many segments",
			[]Segment{
				segs[1],
				segs[2],
				segs[3],
				segs[4],
				segs[5],
				segs[6],
			},
			&MergePlanOptions{
				MaxSegmentsPerTier:   1,
				MaxSegmentSize:       1000,
				TierGrowth:           2.0,
				SegmentsPerMergeTask: 2,
				FloorSegmentSize:     1,
			},
			&MergePlan{
				Tasks: []*MergeTask{
					{
						Segments: []Segment{
							segs[6],
							segs[5],
						},
					},
				},
			},
			nil,
		},
	}

	for testi, test := range tests {
		plan, err := Plan(test.Segments, test.Options)

		if err != test.ExpectErr {
			testj, _ := json.Marshal(&test)

			t.Errorf("testi: %d, test: %s, got err: %v", testi, testj, err)
		}

		if !reflect.DeepEqual(plan, test.ExpectPlan) {
			testj, _ := json.Marshal(&test)

			planj, _ := json.Marshal(&plan)

			t.Errorf("testi: %d, test: %s, got plan: %s",
				testi, testj, planj)
		}
	}
}

// ----------------------------------------

func TestSort(t *testing.T) {
	segs := makeLinearSegments(10)

	sort.Sort(byLiveSizeDescending(segs))

	for i := 1; i < len(segs); i++ {
		if segs[i].LiveSize() >= segs[i-1].LiveSize() {
			t.Errorf("not descending")
		}
	}
}

// ----------------------------------------

func TestCalcBudget(t *testing.T) {
	tests := []struct {
		totalSize     int64
		firstTierSize int64
		o             MergePlanOptions
		expect        int
	}{
		{0, 0, MergePlanOptions{}, 0},
		{1, 0, MergePlanOptions{}, 1},
		{9, 0, MergePlanOptions{}, 9},
		{
			1, 1,
			MergePlanOptions{
				MaxSegmentsPerTier:   1,
				MaxSegmentSize:       1000,
				TierGrowth:           2.0,
				SegmentsPerMergeTask: 2,
				FloorSegmentSize:     1,
			},
			1,
		},
		{
			21, 1,
			MergePlanOptions{
				MaxSegmentsPerTier:   1,
				MaxSegmentSize:       1000,
				TierGrowth:           2.0,
				SegmentsPerMergeTask: 2,
				FloorSegmentSize:     1,
			},
			5,
		},
		{
			21, 1,
			MergePlanOptions{
				MaxSegmentsPerTier:   2,
				MaxSegmentSize:       1000,
				TierGrowth:           2.0,
				SegmentsPerMergeTask: 2,
				FloorSegmentSize:     1,
			},
			7,
		},
		{
			1000, 2000, DefaultMergePlanOptions,
			1,
		},
		{
			5000, 2000, DefaultMergePlanOptions,
			3,
		},
		{
			10000, 2000, DefaultMergePlanOptions,
			5,
		},
		{
			30000, 2000, DefaultMergePlanOptions,
			11,
		},
		{
			1000000, 2000, DefaultMergePlanOptions,
			24,
		},
		{
			1000000000, 2000, DefaultMergePlanOptions,
			54,
		},
	}

	for testi, test := range tests {
		res := CalcBudget(test.totalSize, test.firstTierSize, &test.o)
		if res != test.expect {
			t.Errorf("testi: %d, test: %#v, res: %v",
				testi, test, res)
		}
	}
}

func TestCalcBudgetForSingleSegmentMergePolicy(t *testing.T) {
	mpolicy := MergePlanOptions{
		MaxSegmentsPerTier:   1,
		MaxSegmentSize:       1 << 30, // ~ 1 Billion
		SegmentsPerMergeTask: 10,
		FloorSegmentSize:     1 << 30,
	}

	tests := []struct {
		totalSize     int64
		firstTierSize int64
		o             MergePlanOptions
		expect        int
	}{
		{0, mpolicy.RaiseToFloorSegmentSize(0), mpolicy, 0},
		{1, mpolicy.RaiseToFloorSegmentSize(1), mpolicy, 1},
		{9, mpolicy.RaiseToFloorSegmentSize(0), mpolicy, 1},
		{1, mpolicy.RaiseToFloorSegmentSize(1), mpolicy, 1},
		{21, mpolicy.RaiseToFloorSegmentSize(21), mpolicy, 1},
		{21, mpolicy.RaiseToFloorSegmentSize(21), mpolicy, 1},
		{1000, mpolicy.RaiseToFloorSegmentSize(2000), mpolicy, 1},
		{5000, mpolicy.RaiseToFloorSegmentSize(5000), mpolicy, 1},
		{10000, mpolicy.RaiseToFloorSegmentSize(10000), mpolicy, 1},
		{30000, mpolicy.RaiseToFloorSegmentSize(30000), mpolicy, 1},
		{1000000, mpolicy.RaiseToFloorSegmentSize(1000000), mpolicy, 1},
		{1000000000, 1 << 30, mpolicy, 1},
		{1013423541, 1 << 30, mpolicy, 1},
		{98765442, 1 << 30, mpolicy, 1},
	}

	for testi, test := range tests {
		res := CalcBudget(test.totalSize, test.firstTierSize, &test.o)
		if res != test.expect {
			t.Errorf("testi: %d, test: %#v, res: %v",
				testi, test, res)
		}
	}
}

// ----------------------------------------

func TestInsert1SameSizedSegmentBetweenMerges(t *testing.T) {
	o := &MergePlanOptions{
		MaxSegmentSize:       1000,
		MaxSegmentsPerTier:   3,
		TierGrowth:           3.0,
		SegmentsPerMergeTask: 3,
	}

	spec := testCyclesSpec{
		descrip: "i1sssbm",
		verbose: os.Getenv("VERBOSE") == "i1sssbm" || os.Getenv("VERBOSE") == "y",
		n:       200,
		o:       o,
		beforePlan: func(spec *testCyclesSpec) {
			spec.segments = append(spec.segments, &segment{
				MyId:       spec.nextSegmentId,
				MyFullSize: 1,
				MyLiveSize: 1,
			})
			spec.nextSegmentId++
		},
	}

	spec.runCycles(t)
}

func TestInsertManySameSizedSegmentsBetweenMerges(t *testing.T) {
	o := &MergePlanOptions{
		MaxSegmentSize:       1000,
		MaxSegmentsPerTier:   3,
		TierGrowth:           3.0,
		SegmentsPerMergeTask: 3,
	}

	spec := testCyclesSpec{
		descrip: "imsssbm",
		verbose: os.Getenv("VERBOSE") == "imsssbm" || os.Getenv("VERBOSE") == "y",
		n:       20,
		o:       o,
		beforePlan: func(spec *testCyclesSpec) {
			for i := 0; i < 10; i++ {
				spec.segments = append(spec.segments, &segment{
					MyId:       spec.nextSegmentId,
					MyFullSize: 1,
					MyLiveSize: 1,
				})
				spec.nextSegmentId++
			}
		},
	}

	spec.runCycles(t)
}

func TestInsertManySameSizedSegmentsWithDeletionsBetweenMerges(t *testing.T) {
	o := &MergePlanOptions{
		MaxSegmentSize:       1000,
		MaxSegmentsPerTier:   3,
		TierGrowth:           3.0,
		SegmentsPerMergeTask: 3,
	}

	spec := testCyclesSpec{
		descrip: "imssswdbm",
		verbose: os.Getenv("VERBOSE") == "imssswdbm" || os.Getenv("VERBOSE") == "y",
		n:       20,
		o:       o,
		beforePlan: func(spec *testCyclesSpec) {
			for i := 0; i < 10; i++ {
				// Deletions are a shrinking of the live size.
				for i, seg := range spec.segments {
					if (spec.cycle+i)%5 == 0 {
						s := seg.(*segment)
						if s.MyLiveSize > 0 {
							s.MyLiveSize -= 1
						}
					}
				}

				spec.segments = append(spec.segments, &segment{
					MyId:       spec.nextSegmentId,
					MyFullSize: 1,
					MyLiveSize: 1,
				})
				spec.nextSegmentId++
			}
		},
	}

	spec.runCycles(t)
}

func TestInsertManyDifferentSizedSegmentsBetweenMerges(t *testing.T) {
	o := &MergePlanOptions{
		MaxSegmentSize:       1000,
		MaxSegmentsPerTier:   3,
		TierGrowth:           3.0,
		SegmentsPerMergeTask: 3,
	}

	spec := testCyclesSpec{
		descrip: "imdssbm",
		verbose: os.Getenv("VERBOSE") == "imdssbm" || os.Getenv("VERBOSE") == "y",
		n:       20,
		o:       o,
		beforePlan: func(spec *testCyclesSpec) {
			for i := 0; i < 10; i++ {
				spec.segments = append(spec.segments, &segment{
					MyId:       spec.nextSegmentId,
					MyFullSize: int64(1 + (i % 5)),
					MyLiveSize: int64(1 + (i % 5)),
				})
				spec.nextSegmentId++
			}
		},
	}

	spec.runCycles(t)
}

func TestManySameSizedSegmentsWithDeletesBetweenMerges(t *testing.T) {
	o := &MergePlanOptions{
		MaxSegmentSize:       1000,
		MaxSegmentsPerTier:   3,
		TierGrowth:           3.0,
		SegmentsPerMergeTask: 3,
	}

	var numPlansWithTasks int

	spec := testCyclesSpec{
		descrip: "mssswdbm",
		verbose: os.Getenv("VERBOSE") == "mssswdbm" || os.Getenv("VERBOSE") == "y",
		n:       20,
		o:       o,
		beforePlan: func(spec *testCyclesSpec) {
			// Deletions are a shrinking of the live size.
			for i, seg := range spec.segments {
				if (spec.cycle+i)%5 == 0 {
					s := seg.(*segment)
					if s.MyLiveSize > 0 {
						s.MyLiveSize -= 1
					}
				}
			}

			for i := 0; i < 10; i++ {
				spec.segments = append(spec.segments, &segment{
					MyId:       spec.nextSegmentId,
					MyFullSize: 1,
					MyLiveSize: 1,
				})
				spec.nextSegmentId++
			}
		},
		afterPlan: func(spec *testCyclesSpec, plan *MergePlan) {
			if plan != nil && len(plan.Tasks) > 0 {
				numPlansWithTasks++
			}
		},
	}

	spec.runCycles(t)

	if numPlansWithTasks <= 0 {
		t.Errorf("expected some plans with tasks")
	}
}

func TestValidateMergePlannerOptions(t *testing.T) {
	o := &MergePlanOptions{
		MaxSegmentSize:       1 << 32,
		MaxSegmentsPerTier:   3,
		TierGrowth:           3.0,
		SegmentsPerMergeTask: 3,
	}
	err := ValidateMergePlannerOptions(o)
	if err != ErrMaxSegmentSizeTooLarge {
		t.Error("Validation expected to fail as the MaxSegmentSize exceeds limit")
	}
}

func TestPlanMaxSegmentSizeLimit(t *testing.T) {
	o := &MergePlanOptions{
		MaxSegmentSize:       20,
		MaxSegmentsPerTier:   5,
		TierGrowth:           3.0,
		SegmentsPerMergeTask: 5,
		FloorSegmentSize:     5,
	}
	segments := makeLinearSegments(20)

	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)

	max := 20
	min := 5
	randomInRange := func() int64 {
		return int64(r.Intn(max-min) + min)
	}
	for i := 1; i < 20; i++ {
		o.MaxSegmentSize = randomInRange()
		plans, err := Plan(segments, o)
		if err != nil {
			t.Errorf("Plan failed, err: %v", err)
		}
		if len(plans.Tasks) == 0 {
			t.Errorf("expected some plans with tasks")
		}

		for _, task := range plans.Tasks {
			var totalLiveSize int64
			for _, segs := range task.Segments {
				totalLiveSize += segs.LiveSize()
			}
			if totalLiveSize >= o.MaxSegmentSize {
				t.Errorf("merged segments size: %d exceeding the MaxSegmentSize"+
					"limit: %d", totalLiveSize, o.MaxSegmentSize)
			}
		}
	}
}

// ----------------------------------------

type testCyclesSpec struct {
	descrip string
	verbose bool

	n int // Number of cycles to run.
	o *MergePlanOptions

	converge bool

	beforePlan func(*testCyclesSpec)
	afterPlan  func(*testCyclesSpec, *MergePlan)

	cycle         int
	segments      []Segment
	nextSegmentId uint64
}

func (spec *testCyclesSpec) runCycles(t *testing.T) {
	numPlansWithTasks := 0

	for spec.cycle < spec.n {
		if spec.verbose {
			emit(spec.descrip, spec.cycle, 0, spec.segments, nil)
		}

		if spec.beforePlan != nil {
			spec.beforePlan(spec)
		}

		if spec.verbose {
			emit(spec.descrip, spec.cycle, 1, spec.segments, nil)
		}

		plan, err := Plan(spec.segments, spec.o)
		if err != nil {
			t.Fatalf("expected no err, got: %v", err)
		}

		if spec.afterPlan != nil {
			spec.afterPlan(spec, plan)
		}

		if spec.verbose {
			emit(spec.descrip, spec.cycle, 2, spec.segments, plan)
		}

		if plan == nil || len(plan.Tasks) == 0 {
			if spec.converge {
				return
			}
			spec.cycle++
			continue
		}

		numPlansWithTasks++
		before := len(spec.segments)

		for _, task := range plan.Tasks {
			spec.segments = removeSegments(spec.segments, task.Segments)

			var totLiveSize int64
			var totFileSize int64
			var hasVector bool
			for _, segment := range task.Segments {
				totLiveSize += segment.LiveSize()
				totFileSize += segment.LiveFileSize()
				if segment.HasVector() {
					hasVector = true
				}
			}

			if totLiveSize > 0 {
				spec.segments = append(spec.segments, &segment{
					MyId:        spec.nextSegmentId,
					MyFullSize:  totLiveSize,
					MyLiveSize:  totLiveSize,
					MyFileSize:  totFileSize,
					MyHasVector: hasVector,
				})
				spec.nextSegmentId++
			}
		}

		if len(spec.segments) >= before {
			t.Errorf("cycle %d: plan produced %d task(s) but the segment count "+
				"did not shrink (%d -> %d)", spec.cycle, len(plan.Tasks), before, len(spec.segments))
		}

		spec.cycle++
	}

	if spec.converge {
		t.Errorf("index did not converge within %d cycles (%d segments remain); "+
			"the planner keeps producing tasks, likely an infinite loop",
			spec.n, len(spec.segments))
	}

	if numPlansWithTasks <= 0 {
		t.Errorf("expected some plans with tasks")
	}
}

func emit(descrip string, cycle int, step int, segments []Segment, plan *MergePlan) {
	if os.Getenv("VERBOSE") == "" {
		return
	}

	suffix := ""
	if plan != nil && len(plan.Tasks) > 0 {
		suffix = "hasPlan"
	}

	fmt.Printf("%s %d.%d ---------- %s\n", descrip, cycle, step, suffix)
	fmt.Printf("%s\n", ToBarChart(descrip, 100, segments, plan))
}

// ----------------------------------------

func TestPlanMaxSegmentFileSize(t *testing.T) {
	tests := []struct {
		segments              []Segment
		o                     *MergePlanOptions
		expectedFinalSegments []Segment
	}{
		{
			[]Segment{
				&segment{ // ineligible
					MyId:       1,
					MyFullSize: 4000,
					MyLiveSize: 3900,

					MyHasVector: true,
					MyFileSize:  4000 * 1000 * 4, // > 2MB
				},
				&segment{ // ineligible
					MyId:       2,
					MyFullSize: 6000,
					MyLiveSize: 5500, // > 5000

					MyHasVector: true,
					MyFileSize:  6000 * 1000 * 4, // > 2MB
				},
				&segment{ // eligible
					MyId:       3,
					MyFullSize: 500,
					MyLiveSize: 490,

					MyHasVector: true,
					MyFileSize:  500 * 1000 * 4,
				},
				&segment{ // eligible
					MyId:       4,
					MyFullSize: 500,
					MyLiveSize: 480,

					MyHasVector: true,
					MyFileSize:  500 * 1000 * 4,
				},
				&segment{ // eligible
					MyId:       5,
					MyFullSize: 500,
					MyLiveSize: 300,

					MyHasVector: true,
					MyFileSize:  500 * 1000 * 4,
				},
				&segment{ // eligible
					MyId:       6,
					MyFullSize: 500,
					MyLiveSize: 400,

					MyHasVector: true,
					MyFileSize:  500 * 1000 * 4,
				},
			},
			&MergePlanOptions{
				MaxSegmentSize: 5000, // number of documents
				// considering vector dimension as 1000
				// vectorBytes = 5000 * 1000 * 4 = 20MB, which is too large
				// So, let's set the fileSize limit to 4MB
				MaxSegmentFileSize:   4000000, // 4MB
				MaxSegmentsPerTier:   1,
				SegmentsPerMergeTask: 2,
				TierGrowth:           2.0,
				ReclaimDeletesWeight: 2,
				FloorSegmentSize:     1,
			},
			[]Segment{
				&segment{
					MyId:       1,
					MyFullSize: 4000,
					MyLiveSize: 3900,

					MyHasVector: true,
					MyFileSize:  4000 * 1000 * 4,
				},
				&segment{
					MyId:       2,
					MyFullSize: 6000,
					MyLiveSize: 5500,

					MyHasVector: true,
					MyFileSize:  6000 * 1000 * 4,
				},
				&segment{
					MyId:       3,
					MyFullSize: 500,
					MyLiveSize: 490,

					MyHasVector: true,
					MyFileSize:  500 * 1000 * 4,
				},
				&segment{
					MyId:       4,
					MyFullSize: 500,
					MyLiveSize: 480,

					MyHasVector: true,
					MyFileSize:  500 * 1000 * 4,
				},
				&segment{
					MyId:       7,
					MyFullSize: 700,
					MyLiveSize: 700,

					MyHasVector: true,
					MyFileSize:  700 * 1000 * 4,
				},
			},
		},
	}

	for testi, test := range tests {
		t.Run(fmt.Sprintf("Test-%d", testi), func(t *testing.T) {
			var nextID uint64
			for _, seg := range test.segments {
				if seg.Id() >= nextID {
					nextID = seg.Id() + 1
				}
			}

			spec := testCyclesSpec{
				descrip:       fmt.Sprintf("pmsfs-%d", testi),
				verbose:       os.Getenv("VERBOSE") == "pmsfs" || os.Getenv("VERBOSE") == "y",
				n:             10,
				o:             test.o,
				converge:      true,
				segments:      slices.Clone(test.segments),
				nextSegmentId: nextID,
			}

			spec.runCycles(t)

			if !reflect.DeepEqual(spec.segments, test.expectedFinalSegments) {
				t.Fatalf("final layout mismatch, got: %+v", spec.segments)
			}
		})
	}
}

func TestSingleTaskMergePlan(t *testing.T) {
	o := DefaultMergePlanOptions
	o.FloorSegmentFileSize = 209715200

	spec := testCyclesSpec{
		descrip:  "stmp",
		verbose:  os.Getenv("VERBOSE") == "stmp" || os.Getenv("VERBOSE") == "y",
		n:        10,
		o:        &o,
		converge: true,
		segments: []Segment{
			&segment{
				MyId:       2,
				MyFullSize: 78059,
				MyLiveSize: 78059,
				MyFileSize: 129475914,
			},
			&segment{
				MyId:       1,
				MyFullSize: 3959,
				MyLiveSize: 3959,
				MyFileSize: 24805725,
			},
		},
		nextSegmentId: 3,
	}

	spec.runCycles(t)

	expectedSegments := []Segment{
		&segment{
			MyId:       3,
			MyFullSize: 78059 + 3959,
			MyLiveSize: 78059 + 3959,
			MyFileSize: 129475914 + 24805725,
		},
	}

	if !reflect.DeepEqual(spec.segments, expectedSegments) {
		t.Fatalf("final layout mismatch, got: %+v", spec.segments)
	}
}

func TestPlanLiveFileSizeEligibility(t *testing.T) {
	o := &MergePlanOptions{
		MaxSegmentFileSize:   4 * 1000 * 1000,
		MaxSegmentSize:       100 * 1000,
		SegmentsPerMergeTask: 2,
		TierGrowth:           2.0,
		FloorSegmentSize:     2000,
	}

	segments := []Segment{
		&segment{
			MyId:        1,
			MyFullSize:  1000,
			MyLiveSize:  500,
			MyHasVector: true,
			MyFileSize:  3 * 1000 * 1000,
		},
		&segment{
			MyId:        2,
			MyFullSize:  1000,
			MyLiveSize:  500,
			MyHasVector: true,
			MyFileSize:  3 * 1000 * 1000,
		},
		&segment{
			MyId:        3,
			MyFullSize:  1000,
			MyLiveSize:  1000,
			MyHasVector: true,
			MyFileSize:  3 * 1000 * 1000,
		},
	}

	spec := testCyclesSpec{
		descrip:       "plfse",
		verbose:       os.Getenv("VERBOSE") == "plfse" || os.Getenv("VERBOSE") == "y",
		n:             10,
		o:             o,
		converge:      true,
		segments:      slices.Clone(segments),
		nextSegmentId: 4,
	}

	spec.runCycles(t)

	expectedSegments := []Segment{
		&segment{
			MyId:        3,
			MyFullSize:  1000,
			MyLiveSize:  1000,
			MyHasVector: true,
			MyFileSize:  3 * 1000 * 1000,
		},
		&segment{
			MyId:        4,
			MyFullSize:  1000,
			MyLiveSize:  1000,
			MyHasVector: true,
			MyFileSize:  3 * 1000 * 1000,
		},
	}

	if !reflect.DeepEqual(spec.segments, expectedSegments) {
		t.Fatalf("final layout mismatch, got: %+v", spec.segments)
	}
}

func TestPlanLiveFileSizeBudget(t *testing.T) {
	o := &MergePlanOptions{
		MaxSegmentsPerTier:   1,
		SegmentsPerMergeTask: 2,
		TierGrowth:           2.0,
		FloorSegmentSize:     1,
		FloorSegmentFileSize: 10 * 1000 * 1000,
		MaxSegmentSize:       100 * 1000,
		MaxSegmentFileSize:   4 * 1000 * 1000 * 1000,
	}

	segments := []Segment{
		&segment{
			MyId:       1,
			MyFullSize: 1000,
			MyLiveSize: 100,
			MyFileSize: 10 * 1000 * 1000,
		},
		&segment{
			MyId:       2,
			MyFullSize: 1000,
			MyLiveSize: 100,
			MyFileSize: 10 * 1000 * 1000,
		},
	}

	spec := testCyclesSpec{
		descrip:       "plfsb",
		verbose:       os.Getenv("VERBOSE") == "plfsb" || os.Getenv("VERBOSE") == "y",
		n:             10,
		o:             o,
		converge:      true,
		segments:      slices.Clone(segments),
		nextSegmentId: 3,
	}

	spec.runCycles(t)

	expectedSegments := []Segment{
		&segment{
			MyId:       3,
			MyFullSize: 200,
			MyLiveSize: 200,
			MyFileSize: 2 * 1000 * 1000,
		},
	}

	if !reflect.DeepEqual(spec.segments, expectedSegments) {
		t.Fatalf("final layout mismatch, got: %+v", spec.segments)
	}
}

func TestEmptySegmentsDoNotForceOverMerge(t *testing.T) {
	o := &MergePlanOptions{
		MaxSegmentsPerTier:   10,
		MaxSegmentSize:       1000000,
		SegmentsPerMergeTask: 10,
		FloorSegmentSize:     1,
		CalcBudget: func(totalSize, firstTierSize int64, o *MergePlanOptions) int {
			return 2
		},
	}

	spec := testCyclesSpec{
		descrip:  "esdnfom",
		verbose:  os.Getenv("VERBOSE") == "esdnfom" || os.Getenv("VERBOSE") == "y",
		n:        10,
		o:        o,
		converge: true,
		segments: []Segment{
			&segment{MyId: 1, MyFullSize: 100, MyLiveSize: 100},
			&segment{MyId: 2, MyFullSize: 100, MyLiveSize: 100},
			&segment{MyId: 3, MyFullSize: 100, MyLiveSize: 0},
		},
		nextSegmentId: 4,
	}

	spec.runCycles(t)

	expectedSegments := []Segment{
		&segment{MyId: 1, MyFullSize: 100, MyLiveSize: 100},
		&segment{MyId: 2, MyFullSize: 100, MyLiveSize: 100},
	}

	if !reflect.DeepEqual(spec.segments, expectedSegments) {
		t.Fatalf("final layout mismatch, got: %+v", spec.segments)
	}
}
