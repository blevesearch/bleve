//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package metrics

import "encoding/json"

type stats struct {
	s      *Store
	ostats json.Marshaler
}

func (s *stats) MarshalJSON() ([]byte, error) {
	ms := map[string]interface{}{}

	ms["metrics"] = map[string]interface{}{
		"reader_get":             TimerMap(s.s.TimerReaderGet),
		"reader_prefix_iterator": TimerMap(s.s.TimerReaderPrefixIterator),
		"reader_range_iterator":  TimerMap(s.s.TimerReaderRangeIterator),
		"writer_execute_batch":   TimerMap(s.s.TimerWriterExecuteBatch),
		"iterator_seek":          TimerMap(s.s.TimerIteratorSeek),
		"iterator_next":          TimerMap(s.s.TimerIteratorNext),
		"batch_merge":            TimerMap(s.s.TimerBatchMerge),
	}

	if s.ostats != nil {
		ms["kv"] = s.ostats
	}

	return json.Marshal(ms)
}
