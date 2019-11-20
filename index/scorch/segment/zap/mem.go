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
	"io"
	"io/ioutil"
	"sync/atomic"

	mmap "github.com/edsrzf/mmap-go"
)

var (
	mmapCurrentBytes int64

	// Ignore mmap failures and fallback to regular file access.
	MmapIgnoreErrors bool

	// Optional, maximum number of bytes to mmap before fallback to regular file access.
	MmapMaxBytes int64
)

type zapStats struct {
	// Total number of bytes mapped into memory with mmap.
	MmapCurrentBytes int64
}

type mmapOwner interface {
	readMM(int64, int64) []byte
}

// Stats returns memory usage details for open zap segments in this process.
func Stats() zapStats {
	return zapStats{
		MmapCurrentBytes: atomic.LoadInt64(&mmapCurrentBytes),
	}
}

func (s *SegmentBase) readMem(x, y uint64) []byte {
	if s.mem == nil {
		return s.mmapOwner.readMM(int64(x), int64(y))
	}
	return s.mem[x:y]
}

func (s *Segment) readMM(x, y int64) []byte {
	if s.mm == nil {
		data, _ := ioutil.ReadAll(io.NewSectionReader(s.f, x, y-x))
		return data
	}
	return s.mm[x:y]
}

func (s *Segment) loadMmap() error {
	if MmapMaxBytes > 0 &&
		atomic.LoadInt64(&mmapCurrentBytes)+int64(s.mmSize) > MmapMaxBytes {
		return nil
	}

	if s.mm != nil {
		return nil
	}

	mm, err := mmap.Map(s.f, mmap.RDONLY, 0)
	if err == nil {
		atomic.AddInt64(&mmapCurrentBytes, int64(s.mmSize))
	} else if MmapIgnoreErrors {
		return nil
	} else {
		return err
	}

	s.mm = mm
	s.SegmentBase.mem = mm[0 : s.mmSize-FooterSize]
	return nil
}

func (s *Segment) unloadMmap() error {
	if s.mm == nil {
		return nil
	}
	err := s.mm.Unmap()
	if err == nil {
		atomic.AddInt64(&mmapCurrentBytes, -int64(s.mmSize))
	}
	return err
}
