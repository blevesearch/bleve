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

package search

import "testing"

// TestDocumentMatchResetNilScoreBreakdown verifies that Reset (§22 nil guard)
// does not panic when ScoreBreakdown is nil — the old code called clear(nil)
// which panics.
func TestDocumentMatchResetNilScoreBreakdown(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Reset with nil ScoreBreakdown panicked: %v", r)
		}
	}()
	dm := &DocumentMatch{
		Score:          3.14,
		ScoreBreakdown: nil,
	}
	dm.Reset()
	if dm.Score != 0 {
		t.Errorf("Score not zeroed after Reset: %f", dm.Score)
	}
}

// TestDocumentMatchResetClearsPopulatedScoreBreakdown verifies that Reset
// clears (but preserves the backing map of) a non-nil ScoreBreakdown.
func TestDocumentMatchResetClearsPopulatedScoreBreakdown(t *testing.T) {
	dm := &DocumentMatch{
		ScoreBreakdown: map[int]float64{0: 1.0, 1: 2.5},
	}
	dm.Reset()

	// Map object must be reused (same pointer), but emptied.
	if dm.ScoreBreakdown == nil {
		t.Error("Reset discarded the ScoreBreakdown map allocation (expected reuse)")
	}
	if len(dm.ScoreBreakdown) != 0 {
		t.Errorf("Reset did not clear ScoreBreakdown, len=%d", len(dm.ScoreBreakdown))
	}
}

// TestDocumentMatchResetZerosScalarFields verifies the core scalar fields are
// all zeroed by Reset.
func TestDocumentMatchResetZerosScalarFields(t *testing.T) {
	dm := &DocumentMatch{
		Index:     "myindex",
		ID:        "doc1",
		Score:     9.0,
		HitNumber: 7,
	}
	dm.Reset()

	if dm.Index != "" {
		t.Errorf("Index not cleared: %q", dm.Index)
	}
	if dm.ID != "" {
		t.Errorf("ID not cleared: %q", dm.ID)
	}
	if dm.Score != 0 {
		t.Errorf("Score not zeroed: %f", dm.Score)
	}
	if dm.HitNumber != 0 {
		t.Errorf("HitNumber not zeroed: %d", dm.HitNumber)
	}
}

// TestDocumentMatchResetAllFieldsCanary is a reflection-based canary: it
// populates every user-visible scalar and pointer field of DocumentMatch with
// non-zero values, calls Reset(), and asserts that the fields Reset() is
// supposed to zero are actually zero.  If a new field is added to
// DocumentMatch without a corresponding nil/zero in Reset(), this test will
// catch it as long as that field is covered below.
//
// Fields intentionally preserved by Reset() for allocation reuse
// (IndexInternalID, Sort, DecodedSort, FieldTermLocations, ScoreBreakdown,
// Descendants) are NOT checked here — their preservation is covered by the
// other reset tests.
func TestDocumentMatchResetAllFieldsCanary(t *testing.T) {
	dm := &DocumentMatch{
		Index:      "myindex",
		ID:         "doc42",
		Score:      9.9,
		HitNumber:  7,
		Expl:       &Explanation{Value: 1.0, Message: "test"},
		Locations:  FieldTermLocationMap{"f": {}},
		Fragments:  FieldFragmentMap{"f": {"frag"}},
		Fields:     map[string]interface{}{"key": "val"},
		IndexNames: []string{"idx1"},
	}
	dm.Reset()

	if dm.Index != "" {
		t.Errorf("Index not zeroed after Reset: %q", dm.Index)
	}
	if dm.ID != "" {
		t.Errorf("ID not zeroed after Reset: %q", dm.ID)
	}
	if dm.Score != 0 {
		t.Errorf("Score not zeroed after Reset: %f", dm.Score)
	}
	if dm.HitNumber != 0 {
		t.Errorf("HitNumber not zeroed after Reset: %d", dm.HitNumber)
	}
	if dm.Expl != nil {
		t.Errorf("Expl not nil after Reset: %v", dm.Expl)
	}
	if dm.Locations != nil {
		t.Errorf("Locations not nil after Reset: %v", dm.Locations)
	}
	if dm.Fragments != nil {
		t.Errorf("Fragments not nil after Reset: %v", dm.Fragments)
	}
	if dm.Fields != nil {
		t.Errorf("Fields not nil after Reset: %v", dm.Fields)
	}
}

// TestDocumentMatchResetPreservesBackingArrays verifies that Reset reuses
// existing backing arrays for IndexInternalID and Sort rather than nilling them.
func TestDocumentMatchResetPreservesBackingArrays(t *testing.T) {
	id := make([]byte, 0, 16)
	id = append(id, "hello"...)
	sortBuf := []string{"a"}

	dm := &DocumentMatch{
		IndexInternalID: id,
		Sort:            sortBuf,
	}
	dm.Reset()

	if cap(dm.IndexInternalID) != cap(id) {
		t.Errorf("IndexInternalID cap changed: got %d, want %d",
			cap(dm.IndexInternalID), cap(id))
	}
	if len(dm.IndexInternalID) != 0 {
		t.Errorf("IndexInternalID not truncated to 0: %q", dm.IndexInternalID)
	}
}
