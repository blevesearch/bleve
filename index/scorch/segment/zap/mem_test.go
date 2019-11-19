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

package zap

import (
	"os"
	"testing"
)

func TestStats(t *testing.T) {
	_ = os.RemoveAll("/tmp/scorch.zap")

	testSeg, _, _ := buildTestSegment()
	err := PersistSegmentBase(testSeg, "/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error persisting segment: %v", err)
	}

	if stats := Stats(); stats.MmapCurrentBytes != 0 {
		t.Fatalf("unexpected mmap current bytes: %v", stats.MmapCurrentBytes)
	}

	segment, err := Open("/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := segment.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	if stats := Stats(); stats.MmapCurrentBytes == 0 {
		t.Fatalf("unexpected mmap current bytes: %v", stats.MmapCurrentBytes)
	}

	cerr := segment.Close()
	if cerr != nil {
		t.Fatalf("error closing segment: %v", cerr)
	}

	if stats := Stats(); stats.MmapCurrentBytes != 0 {
		t.Fatalf("unexpected mmap current bytes: %v", stats.MmapCurrentBytes)
	}
}

func TestMmapLimit(t *testing.T) {
	_ = os.RemoveAll("/tmp/scorch.zap")

	testSeg, _, _ := buildTestSegment()
	err := PersistSegmentBase(testSeg, "/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error persisting segment: %v", err)
	}

	if stats := Stats(); stats.MmapCurrentBytes != 0 {
		t.Fatalf("unexpected mmap current bytes: %v", stats.MmapCurrentBytes)
	}

	MmapMaxBytes = 1
	defer func() {
		MmapMaxBytes = 0
	}()

	segment, err := Open("/tmp/scorch.zap")
	if err != nil {
		t.Fatalf("error opening segment: %v", err)
	}
	defer func() {
		cerr := segment.Close()
		if cerr != nil {
			t.Fatalf("error closing segment: %v", cerr)
		}
	}()

	if stats := Stats(); stats.MmapCurrentBytes != 0 {
		t.Fatalf("unexpected mmap current bytes: %v", stats.MmapCurrentBytes)
	}

	cerr := segment.Close()
	if cerr != nil {
		t.Fatalf("error closing segment: %v", cerr)
	}

	if stats := Stats(); stats.MmapCurrentBytes != 0 {
		t.Fatalf("unexpected mmap current bytes: %v", stats.MmapCurrentBytes)
	}
}
