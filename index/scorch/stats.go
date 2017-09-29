package scorch

import (
	"encoding/json"
	"sync/atomic"
)

// Stats tracks statistics about the index
type Stats struct {
	analysisTime, indexTime uint64
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

	return m
}

// MarshalJSON implements json.Marshaler
func (s *Stats) MarshalJSON() ([]byte, error) {
	m := s.statsMap()
	return json.Marshal(m)
}
