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

package scorch

import (
	"encoding/json"
	"io/ioutil"
	"sync/atomic"
)

// Stats tracks statistics about the index, fields that are
// prefixed like CurXxxx are gauges (can go up and down),
// and fields that are prefixed like TotXxxx are monotonically
// increasing counters.
type Stats struct {
	TotUpdates               uint64
	TotDeletes               uint64
	TotBatches               uint64
	TotEmptyBatches          uint64
	TotOnErrors              uint64
	TotAnalysisTime          uint64
	TotIndexTime             uint64
	TotIndexedPlainTextBytes uint64

	TotIndexSnapshotBeg uint64
	TotIndexSnapshotEnd uint64

	TotIntroducedBatchSegments uint64
	TotIntroducedMergeSegments uint64
	TotIntroducedItems         uint64

	TotTermSearchersStarted  uint64
	TotTermSearchersFinished uint64

	TotPersistedItems    uint64
	TotPersistedSegments uint64
	TotPersisterPause    uint64

	TotMemoryMergeOpsDone   uint64
	TotMergedMemorySegments uint64
	TotMergedFileSegments   uint64
	TotFileMergeOpsDone     uint64

	TotRollbackOpsDone uint64

	CurInProgressMemoryMerges uint64
	CurInProgressFileMerges   uint64

	CurMemoryBytes uint64

	i *Scorch
}

func (s *Stats) statsMap() (map[string]interface{}, error) {
	m := map[string]interface{}{}
	m["TotUpdates"] = atomic.LoadUint64(&s.TotUpdates)
	m["TotDeletes"] = atomic.LoadUint64(&s.TotDeletes)
	m["TotBatches"] = atomic.LoadUint64(&s.TotBatches)
	m["TotEmptyBatches"] = atomic.LoadUint64(&s.TotEmptyBatches)
	m["TotOnErrors"] = atomic.LoadUint64(&s.TotOnErrors)
	m["TotAnalysisTime"] = atomic.LoadUint64(&s.TotAnalysisTime)
	m["TotIndexSnapshotBeg"] = atomic.LoadUint64(&s.TotIndexSnapshotBeg)
	m["TotIndexSnapshotEnd"] = atomic.LoadUint64(&s.TotIndexSnapshotEnd)

	m["TotTermSearchersStarted"] = atomic.LoadUint64(&s.TotTermSearchersStarted)
	m["TotTermSearchersFinished"] = atomic.LoadUint64(&s.TotTermSearchersFinished)
	m["TotIndexedPlainTextBytes"] = atomic.LoadUint64(&s.TotIndexedPlainTextBytes)
	m["TotIntroducedItems"] = atomic.LoadUint64(&s.TotIntroducedItems)
	m["TotPersistedItems"] = atomic.LoadUint64(&s.TotPersistedItems)

	m["TotMemoryMergeOpsDone"] = atomic.LoadUint64(&s.TotMemoryMergeOpsDone)
	m["TotFileMergeOpsDone"] = atomic.LoadUint64(&s.TotFileMergeOpsDone)
	m["TotIntroducedBatchSegments"] = atomic.LoadUint64(&s.TotIntroducedBatchSegments)
	m["TotIntroducedMergeSegments"] = atomic.LoadUint64(&s.TotIntroducedMergeSegments)
	m["TotPersistedSegments"] = atomic.LoadUint64(&s.TotPersistedSegments)
	m["TotRollbackOpsDone"] = atomic.LoadUint64(&s.TotRollbackOpsDone)
	m["CurInProgressFileMerges"] = atomic.LoadUint64(&s.CurInProgressFileMerges)
	m["CurInProgressMemoryMerges"] = atomic.LoadUint64(&s.CurInProgressMemoryMerges)
	m["TotPersisterPause"] = atomic.LoadUint64(&s.TotPersisterPause)
	m["TotMergedMemorySegments"] = atomic.LoadUint64(&s.TotMergedMemorySegments)
	m["TotMergedFileSegments"] = atomic.LoadUint64(&s.TotMergedFileSegments)
	m["CurMemoryBytes"] = s.i.MemoryUsed()

	if s.i.path != "" {
		finfos, err := ioutil.ReadDir(s.i.path)
		if err != nil {
			return nil, err
		}

		var numFilesOnDisk, numBytesUsedDisk uint64

		for _, finfo := range finfos {
			if !finfo.IsDir() {
				numBytesUsedDisk += uint64(finfo.Size())
				numFilesOnDisk++
			}
		}

		m["TotOnDiskBytes"] = numBytesUsedDisk
		m["TotOnDiskFiles"] = numFilesOnDisk
	}

	return m, nil
}

// MarshalJSON implements json.Marshaler
func (s *Stats) MarshalJSON() ([]byte, error) {
	m, err := s.statsMap()
	if err != nil {
		return nil, err
	}
	return json.Marshal(m)
}
