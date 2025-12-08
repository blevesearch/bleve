//go:build windows

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
	"os"

	"golang.org/x/sys/windows"
)

// flock acquires an advisory lock on a file using LockFileEx.
// This is the Windows implementation.
// Following the bbolt pattern: uses byte-range lock at offset -1..0
func flock(f *os.File, exclusive bool) error {
	// Flags for immediate failure if lock cannot be acquired
	flags := uint32(windows.LOCKFILE_FAIL_IMMEDIATELY)
	if exclusive {
		flags |= windows.LOCKFILE_EXCLUSIVE_LOCK
	}

	// Use byte-range -1..0 as the lock range (bbolt pattern)
	// This avoids conflicts with actual file content
	var m1 uint32 = (1 << 32) - 1 // -1 in a uint32

	err := windows.LockFileEx(
		windows.Handle(f.Fd()),
		flags,
		0,        // reserved, must be 0
		1,        // number of bytes to lock (low DWORD)
		0,        // number of bytes to lock (high DWORD)
		&windows.Overlapped{
			Offset:     m1,
			OffsetHigh: m1,
		},
	)

	return err
}

// funlock releases the advisory lock on a file.
func funlock(f *os.File) error {
	var m1 uint32 = (1 << 32) - 1

	return windows.UnlockFileEx(
		windows.Handle(f.Fd()),
		0,        // reserved, must be 0
		1,        // number of bytes to unlock (low DWORD)
		0,        // number of bytes to unlock (high DWORD)
		&windows.Overlapped{
			Offset:     m1,
			OffsetHigh: m1,
		},
	)
}
