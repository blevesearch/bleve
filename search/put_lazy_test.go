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

// TestPutLazyZeroesIDAndScore verifies that PutLazy (§22) zeroes only
// IndexInternalID and Score, leaving other fields (FieldTermLocations) intact
// — callers must guarantee those were never set, but PutLazy itself must not
// corrupt the backing slice.
func TestPutLazyZeroesIDAndScore(t *testing.T) {
	dmp := NewDocumentMatchPool(5, 0)
	dm := dmp.Get()

	dm.IndexInternalID = append(dm.IndexInternalID, []byte("abc")...)
	dm.Score = 9.99
	dm.HitNumber = 42 // PutLazy does NOT reset this — not its contract

	dmp.PutLazy(dm)

	// Get it back.
	reused := dmp.Get()

	if len(reused.IndexInternalID) != 0 {
		t.Errorf("PutLazy: IndexInternalID not zeroed, got %q", reused.IndexInternalID)
	}
	if reused.Score != 0 {
		t.Errorf("PutLazy: Score not zeroed, got %f", reused.Score)
	}
}

// TestPutLazyNilSafe verifies PutLazy(nil) does not panic.
func TestPutLazyNilSafe(t *testing.T) {
	dmp := NewDocumentMatchPool(5, 0)
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("PutLazy(nil) panicked: %v", r)
		}
	}()
	dmp.PutLazy(nil)
}

// TestPutLazyReturnedToPool verifies the object is actually added to the pool
// so a subsequent Get() does not call TooSmall.
func TestPutLazyReturnedToPool(t *testing.T) {
	dmp := NewDocumentMatchPool(1, 0)
	tooSmallCalled := false
	dmp.TooSmall = func(_ *DocumentMatchPool) *DocumentMatch {
		tooSmallCalled = true
		return &DocumentMatch{}
	}

	dm := dmp.Get() // drains the pool
	dm.Score = 5.0
	dmp.PutLazy(dm) // return it

	_ = dmp.Get() // should reuse without calling TooSmall
	if tooSmallCalled {
		t.Error("TooSmall called after PutLazy — object was not added to pool")
	}
}
