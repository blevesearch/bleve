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
	"sync/atomic"
)

// Stats tracks statistics about the index
type Stats struct {
	analysisTime, indexTime uint64
	numItemsToPersist       uint64
	i                       *Scorch
}

// FIXME wire up these other stats again
func (s *Stats) statsMap() map[string]interface{} {
	m := map[string]interface{}{}
	// m["updates"] = atomic.LoadUint64(&i.updates)
	// m["deletes"] = atomic.LoadUint64(&i.deletes)
	// m["batches"] = atomic.LoadUint64(&i.batches)
	// m["errors"] = atomic.LoadUint64(&i.errors)
	m["analysis_time"] = atomic.LoadUint64(&s.analysisTime)
	m["index_time"] = atomic.LoadUint64(&s.indexTime)
	// m["term_searchers_started"] = atomic.LoadUint64(&i.termSearchersStarted)
	// m["term_searchers_finished"] = atomic.LoadUint64(&i.termSearchersFinished)
	// m["num_plain_text_bytes_indexed"] = atomic.LoadUint64(&i.numPlainTextBytesIndexed)
	m["num_items_to_persist"] = atomic.LoadUint64(&s.i.root.numItems)

	return m
}

// MarshalJSON implements json.Marshaler
func (s *Stats) MarshalJSON() ([]byte, error) {
	m := s.statsMap()
	return json.Marshal(m)
}
