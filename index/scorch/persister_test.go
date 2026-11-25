//  Copyright (c) 2026 Vadim Lazukov
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

package scorch

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// writeSegmentFile creates a real zap segment file with the given name
// inside the scorch path and returns its full path.
func writeSegmentFile(t *testing.T, s *Scorch, fileName string) string {
	t.Helper()
	seg, _, err := s.segPlugin.New([]index.Document{genDoc(t)})
	if err != nil {
		t.Fatalf("failed to create in-memory segment: %v", err)
	}
	path := filepath.Join(s.path, fileName)
	_, _, err = s.segPlugin.Merge([]segment.Segment{seg}, []*roaring.Bitmap{nil},
		path, make(chan struct{}), s)
	if err != nil {
		t.Fatalf("failed to write segment file %s: %v", path, err)
	}
	return path
}

// captureLog redirects the default logger's output into a buffer for the
// duration of fn and returns everything that was logged.
func captureLog(t *testing.T, fn func()) string {
	t.Helper()
	var buf bytes.Buffer
	out := log.Writer()
	flags := log.Flags()
	log.SetOutput(&buf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(out)
		log.SetFlags(flags)
	}()
	fn()
	return buf.String()
}

// newScorchForCleanupTest builds a scorch instance with an open rootBolt
// but without any of the asynchronous loops running.
func newScorchForCleanupTest(t *testing.T) *Scorch {
	t.Helper()
	cfg := CreateConfig(t.Name())
	if err := InitTest(cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = DestroyTest(cfg) })
	dirPath := cfg["path"].(string)
	if err := os.Mkdir(dirPath, 0o755); err != nil {
		t.Fatal(err)
	}
	analysisQueue := index.NewAnalysisQueue(1)
	idx, err := NewScorch(Name, cfg, analysisQueue)
	if err != nil {
		t.Fatalf("failed to create Scorch: %v", err)
	}
	s, ok := idx.(*Scorch)
	if !ok {
		t.Fatalf("expected *Scorch, got %T", idx)
	}
	s.path = dirPath
	if err = s.openBolt(); err != nil {
		t.Fatalf("failed to open bolt: %v", err)
	}
	return s
}

// removeOldZapFiles must not attempt the removal of files that are still
// referenced by the live root snapshot: the flush that introduced such a
// segment into the root may not have committed its rootBolt entry yet, so
// the file isn't listed as live. On platforms without POSIX unlink
// semantics (e.g. Windows) the file is locked by the root's own mmap and
// the removal would fail (and be logged) on every GC cycle.
// See go-gitea/gitea#39623 and blevesearch/bleve#1627, #1778.
func TestRemoveOldZapFilesSkipsRootReferenced(t *testing.T) {
	s := newScorchForCleanupTest(t)
	defer func() { _ = s.Close() }()

	const rootFileName = "000000000001.zap"
	rootSegPath := writeSegmentFile(t, s, rootFileName)
	rootSeg, err := s.segPlugin.OpenUsing(rootSegPath, s.segmentConfig)
	if err != nil {
		t.Fatalf("failed to open segment %s: %v", rootSegPath, err)
	}
	defer func() { _ = rootSeg.Close() }()

	// a stale file that nothing references anymore: still expected to be
	// garbage collected
	const orphanFileName = "0000000000ff.zap"
	orphanPath := writeSegmentFile(t, s, orphanFileName)

	// swap in a root that references only the first file
	s.rootLock.Lock()
	s.root = &IndexSnapshot{
		parent:  s,
		epoch:   2,
		segment: []*SegmentSnapshot{{id: 1, segment: rootSeg}},
	}
	s.rootLock.Unlock()

	logged := captureLog(t, func() {
		if err := s.removeOldZapFiles(); err != nil {
			t.Fatalf("removeOldZapFiles: %v", err)
		}
	})

	if _, statErr := os.Stat(rootSegPath); statErr != nil {
		t.Fatalf("root-referenced segment file must not be removed: %v", statErr)
	}
	if _, statErr := os.Stat(orphanPath); !os.IsNotExist(statErr) {
		t.Fatalf("unreferenced segment file should have been removed, stat err: %v",
			statErr)
	}
	if strings.Contains(logged, "got err removing file") {
		t.Fatalf("expected no removal errors in the log, got: %s", logged)
	}
}

// a zap file whose removal keeps failing (a non-empty directory with a .zap
// extension stands in for any unremovable path, e.g. a file held open by an
// in-flight reader on Windows) must have its removal error logged only once,
// while the removal itself keeps being retried on the following GC cycles.
func TestRemoveOldZapFilesLogsRemovalErrorOnce(t *testing.T) {
	s := newScorchForCleanupTest(t)
	defer func() { _ = s.Close() }()

	unremovable := filepath.Join(s.path, "0000000000aa.zap")
	if err := os.Mkdir(unremovable, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(unremovable, "dummy"), []byte("x"),
		0o644); err != nil {
		t.Fatal(err)
	}

	var logged string
	for i := 0; i < 2; i++ {
		logged += captureLog(t, func() {
			if err := s.removeOldZapFiles(); err != nil {
				t.Fatalf("removeOldZapFiles: %v", err)
			}
		})
	}

	if n := strings.Count(logged, "got err removing file: 0000000000aa.zap"); n != 1 {
		t.Fatalf("expected the removal error to be logged exactly once, got %d: %s",
			n, logged)
	}
}
