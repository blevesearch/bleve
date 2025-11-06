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
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// FSDirectory is a Directory implementation that uses the local filesystem.
type FSDirectory struct {
	basePath string
	lockFile *os.File
	mu       sync.RWMutex
}

// NewFSDirectory creates a new filesystem-based Directory at the given path.
func NewFSDirectory(path string) (*FSDirectory, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("failed to get absolute path: %w", err)
	}

	return &FSDirectory{
		basePath: absPath,
	}, nil
}

// Open opens the named file for reading.
func (d *FSDirectory) Open(name string) (io.ReadCloser, error) {
	fullPath := d.FullPath(name)
	return os.Open(fullPath)
}

// Create creates or truncates the named file for writing.
func (d *FSDirectory) Create(name string) (io.WriteCloser, error) {
	fullPath := d.FullPath(name)

	// Ensure parent directory exists
	dir := filepath.Dir(fullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create parent directory: %w", err)
	}

	f, err := os.Create(fullPath)
	if err != nil {
		return nil, err
	}

	return &fsWriteCloser{File: f}, nil
}

// Remove removes the named file.
func (d *FSDirectory) Remove(name string) error {
	fullPath := d.FullPath(name)
	return os.Remove(fullPath)
}

// Rename renames (moves) oldpath to newpath.
func (d *FSDirectory) Rename(oldpath, newpath string) error {
	oldFullPath := d.FullPath(oldpath)
	newFullPath := d.FullPath(newpath)

	// Ensure parent directory of new path exists
	dir := filepath.Dir(newFullPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create parent directory: %w", err)
	}

	return os.Rename(oldFullPath, newFullPath)
}

// Stat returns FileInfo describing the named file.
func (d *FSDirectory) Stat(name string) (FileInfo, error) {
	fullPath := d.FullPath(name)
	fi, err := os.Stat(fullPath)
	if err != nil {
		return nil, err
	}
	return &fsFileInfo{FileInfo: fi}, nil
}

// ReadDir reads the named directory and returns a list of directory entries.
func (d *FSDirectory) ReadDir(name string) ([]FileInfo, error) {
	fullPath := d.FullPath(name)
	entries, err := os.ReadDir(fullPath)
	if err != nil {
		return nil, err
	}

	result := make([]FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue // skip entries we can't stat
		}
		result = append(result, &fsFileInfo{FileInfo: info})
	}
	return result, nil
}

// MkdirAll creates a directory named path, along with any necessary parents.
func (d *FSDirectory) MkdirAll(path string, perm fs.FileMode) error {
	fullPath := d.FullPath(path)
	return os.MkdirAll(fullPath, perm)
}

// Sync is a no-op for FSDirectory as file syncs happen on close.
func (d *FSDirectory) Sync() error {
	// For filesystem directories, we don't need to do anything special here.
	// Individual file syncs happen when files are closed.
	return nil
}

// Lock acquires an exclusive lock on the directory.
func (d *FSDirectory) Lock() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.lockFile != nil {
		return fmt.Errorf("directory is already locked")
	}

	lockPath := filepath.Join(d.basePath, "write.lock")

	// Ensure base directory exists
	if err := os.MkdirAll(d.basePath, 0755); err != nil {
		return fmt.Errorf("failed to create base directory: %w", err)
	}

	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to create lock file: %w", err)
	}

	// Try to acquire an exclusive lock (non-blocking)
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		f.Close()
		return fmt.Errorf("failed to acquire lock (another process may have the index open): %w", err)
	}

	d.lockFile = f
	return nil
}

// Unlock releases the lock acquired by Lock.
func (d *FSDirectory) Unlock() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.lockFile == nil {
		return nil // not locked
	}

	// Release the lock
	if err := syscall.Flock(int(d.lockFile.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	// Close and remove the lock file
	lockPath := d.lockFile.Name()
	if err := d.lockFile.Close(); err != nil {
		return fmt.Errorf("failed to close lock file: %w", err)
	}

	// Try to remove the lock file (best effort, ignore errors)
	_ = os.Remove(lockPath)

	d.lockFile = nil
	return nil
}

// FullPath returns the full filesystem path for a given name.
func (d *FSDirectory) FullPath(name string) string {
	if filepath.IsAbs(name) {
		return name
	}
	return filepath.Join(d.basePath, name)
}

// fsWriteCloser wraps os.File to implement WriteCloser with Sync.
type fsWriteCloser struct {
	*os.File
}

func (w *fsWriteCloser) Sync() error {
	return w.File.Sync()
}

// fsFileInfo wraps os.FileInfo to implement our FileInfo interface.
type fsFileInfo struct {
	os.FileInfo
}

// Ensure FSDirectory implements Directory
var _ Directory = (*FSDirectory)(nil)
