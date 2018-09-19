//  Copyright (c) 2018 Couchbase, Inc.
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
	"sync"
	"time"
	"sync/atomic"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/index/scorch/segment"
	"github.com/blevesearch/bleve/index/scorch/segment/zap"
	"fmt"
)

var defaultMaxSize uint64 = 64 * 1000 * 1000   // 64M

type MemTable struct {
	readOnly   bool
	ids        []string
	results    []*index.AnalysisResult
	numUpdates uint64
	numDeletes uint64
	numPlainTextBytes uint64
	totalSize  uint64
	maxSize    uint64
	internalOps map[string][]byte
}

func NewMemTable(maxSize uint64) *MemTable {
	return &MemTable{maxSize: maxSize, internalOps: make(map[string][]byte)}
}

func (m *MemTable) Reset() {
	m.readOnly = false
	if m.ids != nil {
		m.ids = m.ids[:0]
	}
	if m.results != nil {
		m.results = m.results[:0]
	}
	m.numPlainTextBytes = 0
	m.numDeletes = 0
	m.numUpdates = 0
	m.totalSize = 0
	m.internalOps = make(map[string][]byte)
}

func (m *MemTable) AddDocuments(ids []string, results []*index.AnalysisResult, internalOps map[string][]byte, numUpdates, numDeletes, numPlainTextBytes, totalAnalysisSize uint64) bool {
	if m.readOnly {
		return false
	}
	m.ids = append(m.ids, ids...)
	m.results = append(m.results, results...)
	m.numUpdates += numUpdates
	m.numDeletes += numDeletes
	m.numPlainTextBytes += numPlainTextBytes
	m.totalSize += totalAnalysisSize
	if m.totalSize > m.maxSize {
		m.readOnly = true
	}
	for k, v := range internalOps {
		m.internalOps[k] = v
	}
	return true
}

func (m *MemTable) Merge(mem *MemTable) bool {
	if m.readOnly {
		return false
	}
	m.ids = append(m.ids, mem.ids...)
	m.results = append(m.results, mem.results...)
	m.numPlainTextBytes += mem.numPlainTextBytes
	m.numDeletes += mem.numDeletes
	m.numUpdates += mem.numUpdates
	m.totalSize += mem.totalSize
	if m.totalSize > m.maxSize {
		m.readOnly = true
	}
	for k, v := range mem.internalOps {
		m.internalOps[k] = v
	}
	return true
}

func (m *MemTable) ReadOnly() bool {
	r := m.readOnly
	return r
}

func (m *MemTable) SetReadOnly() {
	if !m.readOnly {
		m.readOnly = true
	}
}

func (m *MemTable) Size() uint64 {
	return m.totalSize
}

var (
	memPool = sync.Pool{New: func() interface{} {
		return NewMemTable(defaultMaxSize)
	}}
)

func getMemTable() *MemTable {
	return memPool.Get().(*MemTable)
}

func putMemTable(m *MemTable) {
	m.Reset()
	memPool.Put(m)
}

func (s *Scorch) pushToFlushList(m *MemTable) {
	for {
		s.memLock.Lock()
		if s.immutableMemTables.Len() > 10 {
			s.memLock.Unlock()
			fmt.Println("memtbale full !!!!, need flush")
			time.Sleep(time.Millisecond * 10)
			continue
		}
		s.immutableMemTables.PushBack(s.activeMemTable)
		s.memLock.Unlock()
		break
	}
}

func (s *Scorch) writeLoop() {
	defer s.asyncTasks.Done()
	timer := time.NewTimer(time.Second)
OUTER:
	for {
		// TODO stats
		select {
		case <-s.closeCh:
			break OUTER
		case memTable :=<-s.memTableQueue:
			for !s.activeMemTable.Merge(memTable) {
				s.pushToFlushList(s.activeMemTable)
				s.activeMemTable = getMemTable()
				timer.Reset(time.Second)
			}
			putMemTable(memTable)
			if s.activeMemTable.ReadOnly() {
				s.pushToFlushList(s.activeMemTable)
				s.activeMemTable = getMemTable()
				timer.Reset(time.Second)
			}
		case <-timer.C:
			memTable := s.activeMemTable
			if memTable.Size() > 0 {
				memTable.SetReadOnly()
				s.activeMemTable = getMemTable()
				s.pushToFlushList(memTable)
			}
			timer.Reset(time.Second)
		}
	}
}

func (s *Scorch) flushLoop() {
	defer s.asyncTasks.Done()
OUTER:
	for {
		// TODO stats
		select {
		case <-s.closeCh:
			break OUTER
		default:
		}
		var memTable *MemTable
		s.memLock.Lock()
		e := s.immutableMemTables.Front()
		if e != nil {
			val := s.immutableMemTables.Remove(e)
			memTable = val.(*MemTable)
		}
		s.memLock.Unlock()
		if memTable == nil {
			time.Sleep(time.Millisecond * 10)
			continue
		}
		indexStart := time.Now()
		// notify handlers that we're about to introduce a segment
		s.fireEvent(EventKindBatchIntroductionStart, 0)

		var newSegment segment.Segment
		var bufBytes uint64
		var err error
		if len(memTable.results) > 0 {
			newSegment, bufBytes, err = zap.AnalysisResultsToSegmentBase(memTable.results, DefaultChunkFactor)
			if err != nil {
				goto End
			}
			atomic.AddUint64(&s.iStats.newSegBufBytesAdded, bufBytes)
		} else {
			atomic.AddUint64(&s.stats.TotBatchesEmpty, 1)
		}

		err = s.prepareSegment(newSegment, memTable.ids, memTable.internalOps)
	End:
		if err != nil {
			if newSegment != nil {
				_ = newSegment.Close()
			}
			atomic.AddUint64(&s.stats.TotOnErrors, 1)
		} else {
			atomic.AddUint64(&s.stats.TotUpdates, memTable.numUpdates)
			atomic.AddUint64(&s.stats.TotDeletes, memTable.numDeletes)
			atomic.AddUint64(&s.stats.TotBatches, 1)
			atomic.AddUint64(&s.stats.TotIndexedPlainTextBytes, memTable.numPlainTextBytes)
		}

		atomic.AddUint64(&s.iStats.newSegBufBytesRemoved, bufBytes)
		atomic.AddUint64(&s.stats.TotIndexTime, uint64(time.Since(indexStart)))
		putMemTable(memTable)
	}
}