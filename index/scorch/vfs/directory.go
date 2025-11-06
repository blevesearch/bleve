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
	"io/fs"
	"time"
)

// Directory abstracts the filesystem operations required by Scorch.
// This interface enables Scorch to use different storage backends
// (local filesystem, S3, etc.) without modification.
//
// Implementations must be safe for concurrent use by multiple goroutines.
type Directory interface {
	// Open opens the named file for reading. The caller must close the
	// returned ReadCloser when done.
	Open(name string) (io.ReadCloser, error)

	// Create creates or truncates the named file for writing. If the file
	// already exists, it is truncated. The caller must close the returned
	// WriteCloser when done.
	Create(name string) (io.WriteCloser, error)

	// Remove removes the named file.
	Remove(name string) error

	// Rename renames (moves) oldpath to newpath. If newpath already exists
	// and is not a directory, Rename replaces it.
	Rename(oldpath, newpath string) error

	// Stat returns FileInfo describing the named file.
	Stat(name string) (FileInfo, error)

	// ReadDir reads the named directory and returns a list of directory entries.
	ReadDir(name string) ([]FileInfo, error)

	// MkdirAll creates a directory named path, along with any necessary
	// parents, and returns nil, or else returns an error.
	MkdirAll(path string, perm fs.FileMode) error

	// Sync commits the current contents of the directory to stable storage.
	// This is a hint that implementations can use to optimize durability.
	Sync() error

	// Lock acquires an exclusive lock on the directory. This is used to
	// prevent multiple processes from opening the same index simultaneously.
	// Must be called before any other operations.
	Lock() error

	// Unlock releases the lock acquired by Lock.
	Unlock() error
}

// FileInfo describes a file and is returned by Stat and ReadDir.
type FileInfo interface {
	Name() string       // base name of the file
	Size() int64        // length in bytes
	Mode() fs.FileMode  // file mode bits
	ModTime() time.Time // modification time
	IsDir() bool        // abbreviation for Mode().IsDir()
}

// WriteCloser extends io.WriteCloser with a Sync method.
type WriteCloser interface {
	io.WriteCloser
	Sync() error
}
