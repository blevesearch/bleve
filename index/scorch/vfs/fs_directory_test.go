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
	"os"
	"path/filepath"
	"testing"
)

func TestFSDirectory_BasicOperations(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create FSDirectory
	dir, err := NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FSDirectory: %v", err)
	}

	// Test Lock/Unlock
	if err := dir.Lock(); err != nil {
		t.Fatalf("Failed to lock directory: %v", err)
	}
	defer dir.Unlock()

	// Test Create and Write
	testData := []byte("Hello, Firebug!")
	testFile := "test.txt"

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

	// Test Open and Read
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
		t.Errorf("Read data mismatch: got %q, want %q", readData, testData)
	}

	// Test Stat
	fi, err := dir.Stat(testFile)
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	if fi.Name() != testFile {
		t.Errorf("File name mismatch: got %q, want %q", fi.Name(), testFile)
	}

	if fi.Size() != int64(len(testData)) {
		t.Errorf("File size mismatch: got %d, want %d", fi.Size(), len(testData))
	}

	// Test Rename
	newName := "renamed.txt"
	if err := dir.Rename(testFile, newName); err != nil {
		t.Fatalf("Failed to rename file: %v", err)
	}

	// Verify renamed file exists
	if _, err := dir.Stat(newName); err != nil {
		t.Fatalf("Failed to stat renamed file: %v", err)
	}

	// Verify old file doesn't exist
	if _, err := dir.Stat(testFile); !os.IsNotExist(err) {
		t.Errorf("Old file still exists after rename")
	}

	// Test Remove
	if err := dir.Remove(newName); err != nil {
		t.Fatalf("Failed to remove file: %v", err)
	}

	// Verify file is removed
	if _, err := dir.Stat(newName); !os.IsNotExist(err) {
		t.Errorf("File still exists after remove")
	}
}

func TestFSDirectory_DirectoryOperations(t *testing.T) {
	tmpDir := t.TempDir()

	dir, err := NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FSDirectory: %v", err)
	}

	// Test MkdirAll
	subdir := filepath.Join("a", "b", "c")
	if err := dir.MkdirAll(subdir, 0755); err != nil {
		t.Fatalf("Failed to create subdirectories: %v", err)
	}

	// Create some test files
	testFiles := []string{"file1.txt", "file2.txt", "file3.dat"}
	for _, name := range testFiles {
		w, err := dir.Create(name)
		if err != nil {
			t.Fatalf("Failed to create file %s: %v", name, err)
		}
		if _, err := w.Write([]byte("test")); err != nil {
			t.Fatalf("Failed to write to file %s: %v", name, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Failed to close file %s: %v", name, err)
		}
	}

	// Test ReadDir
	entries, err := dir.ReadDir(".")
	if err != nil {
		t.Fatalf("Failed to read directory: %v", err)
	}

	// Check that we have the expected number of entries
	// (3 files + 1 subdirectory + possibly lock file)
	if len(entries) < 4 {
		t.Errorf("Expected at least 4 entries, got %d", len(entries))
	}

	// Verify our test files are in the list
	found := make(map[string]bool)
	for _, entry := range entries {
		found[entry.Name()] = true
	}

	for _, name := range testFiles {
		if !found[name] {
			t.Errorf("File %s not found in directory listing", name)
		}
	}
}

func TestFSDirectory_Locking(t *testing.T) {
	tmpDir := t.TempDir()

	dir1, err := NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create first FSDirectory: %v", err)
	}

	// Acquire lock with first directory
	if err := dir1.Lock(); err != nil {
		t.Fatalf("Failed to lock first directory: %v", err)
	}

	// Try to acquire lock with second directory (should fail)
	dir2, err := NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create second FSDirectory: %v", err)
	}

	if err := dir2.Lock(); err == nil {
		t.Error("Expected lock to fail, but it succeeded")
		dir2.Unlock()
	}

	// Release first lock
	if err := dir1.Unlock(); err != nil {
		t.Fatalf("Failed to unlock first directory: %v", err)
	}

	// Now second directory should be able to acquire lock
	if err := dir2.Lock(); err != nil {
		t.Fatalf("Failed to lock second directory after first was released: %v", err)
	}

	if err := dir2.Unlock(); err != nil {
		t.Fatalf("Failed to unlock second directory: %v", err)
	}
}

func TestFSDirectory_ConcurrentReads(t *testing.T) {
	tmpDir := t.TempDir()

	dir, err := NewFSDirectory(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create FSDirectory: %v", err)
	}

	// Create test file
	testFile := "concurrent.txt"
	testData := []byte("Concurrent read test data")

	w, err := dir.Create(testFile)
	if err != nil {
		t.Fatalf("Failed to create file: %v", err)
	}
	if _, err := w.Write(testData); err != nil {
		t.Fatalf("Failed to write to file: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Failed to close file: %v", err)
	}

	// Perform concurrent reads
	const numReaders = 10
	done := make(chan bool, numReaders)

	for i := 0; i < numReaders; i++ {
		go func() {
			defer func() { done <- true }()

			r, err := dir.Open(testFile)
			if err != nil {
				t.Errorf("Failed to open file: %v", err)
				return
			}
			defer r.Close()

			data, err := io.ReadAll(r)
			if err != nil {
				t.Errorf("Failed to read file: %v", err)
				return
			}

			if string(data) != string(testData) {
				t.Errorf("Data mismatch in concurrent read")
			}
		}()
	}

	// Wait for all readers to complete
	for i := 0; i < numReaders; i++ {
		<-done
	}
}
