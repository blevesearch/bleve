//  Copyright (c) 2025 Couchbase, Inc.
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

package vfs

import (
	"io"
	"testing"

	apivfs "github.com/blevesearch/bleve_index_api/vfs"
)

// directoryTestSuite runs a standard set of tests against any Directory implementation.
func directoryTestSuite(t *testing.T, dir apivfs.Directory) {
	t.Run("CreateAndRead", func(t *testing.T) {
		testData := []byte("test data")
		testFile := "test.dat"

		// Create and write
		w, err := dir.Create(testFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}

		if _, err := w.Write(testData); err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}

		if err := w.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		// Read and verify
		r, err := dir.Open(testFile)
		if err != nil {
			t.Fatalf("Failed to open file: %v", err)
		}
		defer r.Close()

		readData, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("Failed to read data: %v", err)
		}

		if string(readData) != string(testData) {
			t.Errorf("Data mismatch: got %q, want %q", readData, testData)
		}

		// Clean up
		if err := dir.Remove(testFile); err != nil {
			t.Fatalf("Failed to remove file: %v", err)
		}
	})

	t.Run("Stat", func(t *testing.T) {
		testData := []byte("stat test")
		testFile := "stat.dat"

		// Create file
		w, err := dir.Create(testFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		if _, err := w.Write(testData); err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		// Stat file
		fi, err := dir.Stat(testFile)
		if err != nil {
			t.Fatalf("Failed to stat file: %v", err)
		}

		if fi.Size() != int64(len(testData)) {
			t.Errorf("Size mismatch: got %d, want %d", fi.Size(), len(testData))
		}

		if fi.IsDir() {
			t.Error("Expected file, got directory")
		}

		// Clean up
		if err := dir.Remove(testFile); err != nil {
			t.Fatalf("Failed to remove file: %v", err)
		}
	})

	t.Run("Rename", func(t *testing.T) {
		oldName := "old.dat"
		newName := "new.dat"
		testData := []byte("rename test")

		// Create file
		w, err := dir.Create(oldName)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		if _, err := w.Write(testData); err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		// Rename
		if err := dir.Rename(oldName, newName); err != nil {
			t.Fatalf("Failed to rename file: %v", err)
		}

		// Verify new file exists and has correct content
		r, err := dir.Open(newName)
		if err != nil {
			t.Fatalf("Failed to open renamed file: %v", err)
		}
		defer r.Close()

		readData, err := io.ReadAll(r)
		if err != nil {
			t.Fatalf("Failed to read renamed file: %v", err)
		}

		if string(readData) != string(testData) {
			t.Errorf("Data mismatch after rename")
		}

		// Clean up
		if err := dir.Remove(newName); err != nil {
			t.Fatalf("Failed to remove file: %v", err)
		}
	})

	t.Run("Remove", func(t *testing.T) {
		testFile := "remove.dat"

		// Create file
		w, err := dir.Create(testFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		if _, err := w.Write([]byte("remove test")); err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		// Remove file
		if err := dir.Remove(testFile); err != nil {
			t.Fatalf("Failed to remove file: %v", err)
		}

		// Verify file doesn't exist
		if _, err := dir.Stat(testFile); err == nil {
			t.Error("File still exists after remove")
		}
	})

	t.Run("OpenAt", func(t *testing.T) {
		testData := []byte("random access test data with some content")
		testFile := "openat.dat"

		// Create file
		w, err := dir.Create(testFile)
		if err != nil {
			t.Fatalf("Failed to create file: %v", err)
		}
		if _, err := w.Write(testData); err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Failed to close writer: %v", err)
		}

		// Open for random access
		rac, err := dir.OpenAt(testFile)
		if err != nil {
			t.Fatalf("Failed to open file for random access: %v", err)
		}
		defer rac.Close()

		// Test reading at different positions
		buf := make([]byte, 6)

		// Read from beginning
		n, err := rac.ReadAt(buf, 0)
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read at position 0: %v", err)
		}
		if n != 6 {
			t.Errorf("Expected to read 6 bytes, got %d", n)
		}
		if string(buf) != "random" {
			t.Errorf("Data mismatch at position 0: got %q, want %q", buf, "random")
		}

		// Read from middle
		n, err = rac.ReadAt(buf, 7)
		if err != nil && err != io.EOF {
			t.Fatalf("Failed to read at position 7: %v", err)
		}
		if n != 6 {
			t.Errorf("Expected to read 6 bytes, got %d", n)
		}
		if string(buf) != "access" {
			t.Errorf("Data mismatch at position 7: got %q, want %q", buf, "access")
		}

		// Test AsFd returns valid file descriptor
		fd := rac.AsFd()
		if fd == 0 {
			t.Error("Expected valid file descriptor, got 0")
		}

		// Clean up
		if err := dir.Remove(testFile); err != nil {
			t.Fatalf("Failed to remove file: %v", err)
		}
	})
}

func TestDirectoryCompliance_FSDirectory(t *testing.T) {
	tmpDir := t.TempDir()
	dir, err := NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FSDirectory: %v", err)
	}

	// Lock directory for tests
	if err := dir.Lock(); err != nil {
		t.Fatalf("Failed to lock directory: %v", err)
	}
	defer dir.Unlock()

	directoryTestSuite(t, dir)
}
