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

//go:build vectors
// +build vectors

package scorch

import (
	"encoding/json"
	"fmt"
	"sync/atomic"
	"testing"

	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// These tests cover the scorch half of the two phase kNN search: that phase one
// runs once per query vector against the trained index, and that each segment
// is then steered onto the pre-assigned path or the ordinary one depending on
// whether it shares the trained centroid layout.

// newPreassigned builds a plausible phase one result. src stands in for the
// trained index and is only ever compared by identity.
func newPreassigned(src interface{}) *segment.PreassignedCentroids {
	return &segment.PreassignedCentroids{
		Source:    src,
		IDs:       []int64{3, 1, 2, 0},
		Distances: []float32{0.5, 1.5, 2.5, 3.5},
		Nprobe:    2,
	}
}

// optimizeVRForTest wires up an OptimizeVR over a trainer holding trainedSeg.
func optimizeVRForTest(t *testing.T, trainedSeg segment.Segment,
	vrs ...*IndexSnapshotVectorReader) *OptimizeVR {
	t.Helper()
	s := newMinimalScorchForTrainer(t)
	if trainedSeg != nil {
		s.trainer = &vectorTrainer{
			parent:       s,
			config:       map[string]interface{}{},
			trainedIndex: &SegmentSnapshot{segment: trainedSeg},
		}
	}
	return &OptimizeVR{
		snapshot: &IndexSnapshot{parent: s},
		vrs:      map[string][]*IndexSnapshotVectorReader{"vec": vrs},
	}
}

func TestRankCentroidsPopulatesPerReader(t *testing.T) {
	pre := newPreassigned("trained")
	var calls int
	trainedSeg := &mockTrainedSeg{
		searchCentroidsFn: func(field string, qVector []float32) (
			*segment.PreassignedCentroids, error) {
			calls++
			if field != "vec" {
				t.Errorf("expected field %q, got %q", "vec", field)
			}
			return pre, nil
		},
	}

	vr1 := &IndexSnapshotVectorReader{field: "vec", vector: []float32{1, 2}, k: 5}
	vr2 := &IndexSnapshotVectorReader{field: "vec", vector: []float32{3, 4}, k: 5}
	o := optimizeVRForTest(t, trainedSeg, vr1, vr2)

	o.rankCentroids()

	// one coarse quantizer search per query vector, not per segment
	if calls != 2 {
		t.Errorf("expected one centroid search per reader (2), got %d", calls)
	}
	if o.preassigned[vr1] != pre || o.preassigned[vr2] != pre {
		t.Error("expected both readers to carry the phase one ranking")
	}
	if got := atomic.LoadUint64(&o.snapshot.parent.stats.TotKNNCentroidRankings); got != 2 {
		t.Errorf("expected 2 centroid rankings recorded, got %d", got)
	}
}

func TestRankCentroidsSkipsUnusableResults(t *testing.T) {
	tests := []struct {
		name string
		fn   func(string, []float32) (*segment.PreassignedCentroids, error)
	}{
		{
			name: "no trained index for the field",
			fn: func(string, []float32) (*segment.PreassignedCentroids, error) {
				return nil, nil
			},
		},
		{
			name: "coarse quantizer search failed",
			fn: func(string, []float32) (*segment.PreassignedCentroids, error) {
				return nil, fmt.Errorf("quantizer unavailable")
			},
		},
		{
			name: "ranking is empty",
			fn: func(string, []float32) (*segment.PreassignedCentroids, error) {
				return &segment.PreassignedCentroids{Source: "trained"}, nil
			},
		},
		{
			name: "ranking has no source to verify against",
			fn: func(string, []float32) (*segment.PreassignedCentroids, error) {
				p := newPreassigned(nil)
				return p, nil
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vr := &IndexSnapshotVectorReader{field: "vec", vector: []float32{1, 2}, k: 5}
			o := optimizeVRForTest(t, &mockTrainedSeg{searchCentroidsFn: tc.fn}, vr)

			o.rankCentroids()

			if len(o.preassigned) != 0 {
				t.Errorf("expected no ranking to be recorded, got %d", len(o.preassigned))
			}
		})
	}
}

func TestRankCentroidsWithoutTrainer(t *testing.T) {
	vr := &IndexSnapshotVectorReader{field: "vec", vector: []float32{1, 2}, k: 5}
	o := optimizeVRForTest(t, nil, vr)

	o.rankCentroids()

	if len(o.preassigned) != 0 {
		t.Errorf("expected no ranking without a trainer, got %d", len(o.preassigned))
	}
}

func TestRankCentroidsSkipsDeletedField(t *testing.T) {
	trainedSeg := &mockTrainedSeg{
		searchCentroidsFn: func(string, []float32) (*segment.PreassignedCentroids, error) {
			t.Error("must not search centroids for a deleted field")
			return nil, nil
		},
	}
	vr := &IndexSnapshotVectorReader{field: "vec", vector: []float32{1, 2}, k: 5}
	o := optimizeVRForTest(t, trainedSeg, vr)
	o.snapshot.updatedFields = map[string]*index.UpdateFieldInfo{
		"vec": {Deleted: true},
	}

	o.rankCentroids()

	if len(o.preassigned) != 0 {
		t.Errorf("expected no ranking for a deleted field, got %d", len(o.preassigned))
	}
}

// ---------------------------------------------------------------------------
// searchSegment path selection
// ---------------------------------------------------------------------------

func TestSearchSegmentPathSelection(t *testing.T) {
	tests := []struct {
		name string
		// whether phase one produced a ranking for the reader
		ranked bool
		// what the segment says about its centroid layout
		shares bool
		// whether the index implements the pre-assigned interface at all
		preassignable bool

		wantCall            string
		wantPreassignedStat uint64
		wantUnassignedStat  uint64
	}{
		{
			name:                "shared layout takes the pre-assigned path",
			ranked:              true,
			shares:              true,
			preassignable:       true,
			wantCall:            "SearchPreassigned",
			wantPreassignedStat: 1,
		},
		{
			name:               "differing layout falls back",
			ranked:             true,
			shares:             false,
			preassignable:      true,
			wantCall:           "Search",
			wantUnassignedStat: 1,
		},
		{
			name:               "no ranking means the ordinary path",
			ranked:             false,
			shares:             true,
			preassignable:      true,
			wantCall:           "Search",
			wantUnassignedStat: 1,
		},
		{
			name:               "index that cannot take pre-assigned centroids",
			ranked:             true,
			shares:             true,
			preassignable:      false,
			wantCall:           "Search",
			wantUnassignedStat: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			vr := &IndexSnapshotVectorReader{field: "vec", vector: []float32{1, 2}, k: 5}
			o := optimizeVRForTest(t, nil, vr)
			if tc.ranked {
				o.preassigned = map[*IndexSnapshotVectorReader]*segment.PreassignedCentroids{
					vr: newPreassigned("trained"),
				}
			}

			base := &stubVectorIndex{shares: tc.shares}
			var idx segment.VectorIndex = base
			if !tc.preassignable {
				idx = &plainVectorIndex{inner: base}
			}

			if _, err := o.searchSegment(idx, 0, vr); err != nil {
				t.Fatalf("searchSegment: %v", err)
			}

			if base.called != tc.wantCall {
				t.Errorf("expected %s to be called, got %s", tc.wantCall, base.called)
			}
			stats := &o.snapshot.parent.stats
			if got := atomic.LoadUint64(&stats.TotKNNPreassignedSegmentSearches); got != tc.wantPreassignedStat {
				t.Errorf("pre-assigned searches: expected %d, got %d", tc.wantPreassignedStat, got)
			}
			if got := atomic.LoadUint64(&stats.TotKNNUnassignedSegmentSearches); got != tc.wantUnassignedStat {
				t.Errorf("unassigned searches: expected %d, got %d", tc.wantUnassignedStat, got)
			}
		})
	}
}

func TestSearchSegmentUsesFilteredVariants(t *testing.T) {
	vr := &IndexSnapshotVectorReader{
		field:            "vec",
		vector:           []float32{1, 2},
		k:                5,
		eligibleSelector: &stubEligibleSelector{},
	}
	o := optimizeVRForTest(t, nil, vr)
	o.preassigned = map[*IndexSnapshotVectorReader]*segment.PreassignedCentroids{
		vr: newPreassigned("trained"),
	}

	idx := &stubVectorIndex{shares: true}
	if _, err := o.searchSegment(idx, 0, vr); err != nil {
		t.Fatalf("searchSegment: %v", err)
	}
	if idx.called != "SearchWithFilterPreassigned" {
		t.Errorf("expected the filtered pre-assigned search, got %s", idx.called)
	}

	// and without a ranking, the ordinary filtered search
	o.preassigned = nil
	idx.called = ""
	if _, err := o.searchSegment(idx, 0, vr); err != nil {
		t.Fatalf("searchSegment: %v", err)
	}
	if idx.called != "SearchWithFilter" {
		t.Errorf("expected the ordinary filtered search, got %s", idx.called)
	}
}

// ---------------------------------------------------------------------------
// stubs
// ---------------------------------------------------------------------------

// stubVectorIndex records which of the four search entry points was taken.
type stubVectorIndex struct {
	shares bool
	called string
}

func (s *stubVectorIndex) Search(qVector []float32, k int64, params json.RawMessage) (
	segment.VecPostingsList, error) {
	s.called = "Search"
	return &stubVecPostingsList{}, nil
}

func (s *stubVectorIndex) SearchWithFilter(qVector []float32, k int64,
	eligibleList index.EligibleDocumentList, params json.RawMessage) (
	segment.VecPostingsList, error) {
	s.called = "SearchWithFilter"
	return &stubVecPostingsList{}, nil
}

func (s *stubVectorIndex) SharesCentroidLayout(pre *segment.PreassignedCentroids) (bool, error) {
	return s.shares, nil
}

func (s *stubVectorIndex) SearchPreassigned(qVector []float32, k int64,
	pre *segment.PreassignedCentroids, params json.RawMessage) (
	segment.VecPostingsList, error) {
	s.called = "SearchPreassigned"
	return &stubVecPostingsList{}, nil
}

func (s *stubVectorIndex) SearchWithFilterPreassigned(qVector []float32, k int64,
	eligibleList index.EligibleDocumentList, pre *segment.PreassignedCentroids,
	params json.RawMessage) (segment.VecPostingsList, error) {
	s.called = "SearchWithFilterPreassigned"
	return &stubVecPostingsList{}, nil
}

func (s *stubVectorIndex) Close()       {}
func (s *stubVectorIndex) Size() uint64 { return 0 }

func (s *stubVectorIndex) ObtainKCentroidCardinalitiesFromIVFIndex(limit int, descending bool) (
	[]index.CentroidCardinality, error) {
	return nil, nil
}

// plainVectorIndex implements only segment.VectorIndex, standing in for a
// segment implementation that predates the pre-assigned methods. It delegates
// so the test can still see which entry point was taken.
type plainVectorIndex struct {
	inner *stubVectorIndex
}

func (p *plainVectorIndex) Search(qVector []float32, k int64, params json.RawMessage) (
	segment.VecPostingsList, error) {
	return p.inner.Search(qVector, k, params)
}

func (p *plainVectorIndex) SearchWithFilter(qVector []float32, k int64,
	eligibleList index.EligibleDocumentList, params json.RawMessage) (
	segment.VecPostingsList, error) {
	return p.inner.SearchWithFilter(qVector, k, eligibleList, params)
}

func (p *plainVectorIndex) Close()       {}
func (p *plainVectorIndex) Size() uint64 { return 0 }

func (p *plainVectorIndex) ObtainKCentroidCardinalitiesFromIVFIndex(limit int, descending bool) (
	[]index.CentroidCardinality, error) {
	return nil, nil
}

type stubVecPostingsList struct{}

func (s *stubVecPostingsList) Iterator(prealloc segment.VecPostingsIterator) segment.VecPostingsIterator {
	return nil
}
func (s *stubVecPostingsList) Size() int             { return 0 }
func (s *stubVecPostingsList) Count() uint64         { return 0 }
func (s *stubVecPostingsList) ResetBytesRead(uint64) {}
func (s *stubVecPostingsList) BytesRead() uint64     { return 0 }
func (s *stubVecPostingsList) BytesWritten() uint64  { return 0 }

type stubEligibleSelector struct{}

func (s *stubEligibleSelector) SegmentEligibleDocuments(segmentID int) index.EligibleDocumentList {
	return &stubEligibleList{}
}
func (s *stubEligibleSelector) AddEligibleDocumentMatch(id index.IndexInternalID) error { return nil }

type stubEligibleList struct{}

func (s *stubEligibleList) Count() uint64 { return 1 }
func (s *stubEligibleList) Iterator() index.EligibleDocumentIterator {
	return &stubEligibleIterator{}
}

type stubEligibleIterator struct{ done bool }

func (s *stubEligibleIterator) Next() (uint64, bool) {
	if s.done {
		return 0, false
	}
	s.done = true
	return 0, true
}
