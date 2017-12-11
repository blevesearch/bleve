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
	"math"
	"sort"
)

// A Segment represents the information that the planner needs to
// calculate segment merging.
type Segment interface {
	// Unique id of the segment -- used for sorting.
	Id() uint64

	// Full segment size (the size before any logical deletions).
	FullSize() int64

	// Size of the live data of the segment; i.e., FullSize() minus
	// any logical deletions.
	LiveSize() int64
}

// Plan() will functionally compute a merge plan.  A segment will be
// assigned to at most a single MergeTask in the output MergePlan.  A
// segment not assigned to any MergeTask means the segment should
// remain unmerged.
func Plan(segments []Segment, o *MergePlanOptions) (
	result *MergePlan, err error) {
	if len(segments) <= 1 {
		return nil, nil
	}

	// TODO: PLACEHOLDER implementation for now, that always merges
	// all the candidates.
	return &MergePlan{
		Tasks: []*MergeTask{
			&MergeTask{
				Segments: segments,
			},
		},
	}, nil
}

// A MergePlan is the result of the Plan() API.
//
// The planner doesn’t know how or whether these tasks are executed --
// that’s up to a separate merge execution system, which might execute
// these tasks concurrently or not, and which might execute all the
// tasks or not.
type MergePlan struct {
	Tasks []*MergeTask
}

// A MergeTask represents several segments that should be merged
// together into a single segment.
type MergeTask struct {
	Segments []Segment
}

// The MergePlanOptions is designed to be reusable between planning calls.
type MergePlanOptions struct {
	// Max # segments per logarithmic tier, or max width of any
	// logarithmic “step”.  Smaller values mean more merging but fewer
	// segments.  Should be >= SegmentsPerMergeTask, else you'll have
	// too much merging.
	MaxSegmentsPerTier int

	// Max size of any segment produced after merging.  Actual
	// merging, however, may produce segment sizes different than the
	// planner’s predicted sizes.
	MaxSegmentSize int64

	// The number of segments in any resulting MergeTask.  e.g.,
	// len(result.Tasks[ * ].Segments) == SegmentsPerMergeTask.
	SegmentsPerMergeTask int

	// Small segments are rounded up to this size, i.e., treated as
	// equal (floor) size for consideration.  This is to prevent lots
	// of tiny segments from resulting in a long tail in the index.
	FloorSegmentSize int64

	// Controls how aggressively merges that reclaim more deletions
	// are favored.  Higher values will more aggressively target
	// merges that reclaim deletions, but be careful not to go so high
	// that way too much merging takes place; a value of 3.0 is
	// probably nearly too high.  A value of 0.0 means deletions don't
	// impact merge selection.
	ReclaimDeletesWeight float64

	// Only consider a segment for merging if its delete percentage is
	// over this threshold.
	MinDeletesPct float64

	// Optional, defaults to mergeplan.CalcBudget().
	CalcBudget func(totalSize int64, firstTierSize int64,
		o *MergePlanOptions) (budgetNumSegments int)

	// Optional, defaults to mergeplan.ScoreSegments().
	ScoreSegments func(segments []Segment, o *MergePlanOptions) float64

	// Optional.
	Logger func(string)
}

// Returns the higher of the input or FloorSegmentSize.
func (o *MergePlanOptions) RaiseToFloorSegmentSize(s int64) int64 {
	if s > o.FloorSegmentSize {
		return s
	}
	return o.FloorSegmentSize
}

// Suggested default options.
var DefaultMergePlanOptions = MergePlanOptions{
	MaxSegmentsPerTier:   10,
	MaxSegmentSize:       5000000,
	SegmentsPerMergeTask: 10,
	FloorSegmentSize:     2000,
	ReclaimDeletesWeight: 2.0,
	MinDeletesPct:        10.0,
}

// -------------------------------------------

func plan(segmentsIn []Segment, o *MergePlanOptions) (
	result *MergePlan, err error) {
	if len(segmentsIn) <= 1 {
		return nil, nil
	}

	if o == nil {
		o = &DefaultMergePlanOptions
	}

	segments := append([]Segment(nil), segmentsIn...) // Copy.

	sort.Sort(byLiveSizeDescending(segments))

	var segmentsLiveSize int64

	var minLiveSize int64 = math.MaxInt64

	var eligible []Segment
	var eligibleLiveSize int64

	for _, segment := range segments {
		segmentsLiveSize += segment.LiveSize()

		if minLiveSize > segment.LiveSize() {
			minLiveSize = segment.LiveSize()
		}

		// Only small-enough segments are eligible.
		if segment.LiveSize() < o.MaxSegmentSize/2 {
			eligible = append(eligible, segment)
			eligibleLiveSize += segment.LiveSize()
		}
	}

	minLiveSize = o.RaiseToFloorSegmentSize(minLiveSize)

	calcBudget := o.CalcBudget
	if calcBudget == nil {
		calcBudget = CalcBudget
	}

	budgetNumSegments := CalcBudget(eligibleLiveSize, minLiveSize, o)

	scoreSegments := o.ScoreSegments
	if scoreSegments == nil {
		scoreSegments = ScoreSegments
	}

	rv := &MergePlan{}

	// While we’re over budget, keep looping, which might produce
	// another MergeTask.
	for len(eligible) > budgetNumSegments {
		// Track a current best roster as we examine and score
		// potential rosters of merges.
		var bestRoster []Segment
		var bestRosterScore float64 // Lower score is better.

		for startIdx := 0; startIdx < len(eligible)-o.SegmentsPerMergeTask; startIdx++ {
			var roster []Segment
			var rosterLiveSize int64

			for idx := startIdx; idx < len(eligible) && len(roster) < o.SegmentsPerMergeTask; idx++ {
				rosterCandidate := eligible[idx]

				if rosterLiveSize+rosterCandidate.LiveSize() > o.MaxSegmentSize {
					// NOTE: We continue the loop, to try to “pack”
					// the roster with smaller segments to get closer
					// to the max size; but, we aren't doing full,
					// comprehensive "bin-packing" permutations.
					continue
				}

				roster = append(roster, rosterCandidate)
				rosterLiveSize += rosterCandidate.LiveSize()
			}

			rosterScore := scoreSegments(roster, o)

			if len(bestRoster) <= 0 || rosterScore < bestRosterScore {
				bestRoster = roster
				bestRosterScore = rosterScore
			}
		}

		if len(bestRoster) <= 0 {
			return rv, nil
		}

		rv.Tasks = append(rv.Tasks, &MergeTask{
			Segments: bestRoster,
		})

		eligible = removeSegments(eligible, bestRoster)
	}

	return rv, nil
}

// Compute the number of segments that would be needed to cover the
// totalSize, by climbing up a logarithmic staircase of segment tiers.
func CalcBudget(totalSize int64, firstTierSize int64, o *MergePlanOptions) (
	budgetNumSegments int) {
	tierSize := firstTierSize

	for totalSize > 0 {
		segmentsInTier := float64(totalSize) / float64(tierSize)
		if segmentsInTier < float64(o.MaxSegmentsPerTier) {
			budgetNumSegments += int(math.Ceil(segmentsInTier))
			break
		}

		budgetNumSegments += o.MaxSegmentsPerTier
		totalSize -= int64(o.MaxSegmentsPerTier) * tierSize
		tierSize *= int64(o.SegmentsPerMergeTask)
	}

	return budgetNumSegments
}

// removeSegments() keeps the ordering of the result segments stable.
func removeSegments(segments []Segment, toRemove []Segment) (rv []Segment) {
OUTER:
	for _, segment := range segments {
		for _, r := range toRemove {
			if segment == r {
				continue OUTER
			}
		}
		rv = append(rv, segment)
	}
	return rv
}

func ScoreSegments(segments []Segment, o *MergePlanOptions) float64 {
	return 0 // TODO. Bogus score.
}
