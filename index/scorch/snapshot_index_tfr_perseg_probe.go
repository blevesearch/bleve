//  Copyright (c) 2026 Couchbase, Inc.
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
	"fmt"

	segment "github.com/blevesearch/scorch_segment_api/v2"
)

// Probe looks up one doc of the segment in the term's postings, and says whether
// the term has it, with its frequency and norm. It is for the few docs that a
// search has settled on (to explain them, say), not for reading: it starts a
// cursor of its own, with frequencies and norms whether or not the reader was
// made for them, so it can be asked about any doc, in any order, before or after
// the reader has been read, but not while the reader is being read. The reader
// is left where a new one would be.
func (r *PerSegmentIndexSnapshotTermFieldReader) Probe(doc uint32) (freq uint32, norm float32,
	ok bool, err error) {
	provider, isProvider := r.pl.(segment.BlockCursorProvider)
	if !isProvider {
		return 0, 0, false, fmt.Errorf("scorch: the postings of segment %d can't be read by block", r.segmentIndex)
	}
	cursor, err := provider.BlockPostingsIterator(true, true, r.cursor)
	if err != nil {
		return 0, 0, false, err
	}
	r.cursor = cursor
	r.blockMax, _ = cursor.(segment.BlockMaxCursor)
	r.n, r.pos = 0, 0

	// the entries below the doc are dropped, so the doc is the first one if it's there
	n, err := cursor.SeekBlock(uint64(doc), &r.blk)
	if err != nil {
		return 0, 0, false, err
	}
	if n == 0 || r.blk.Docs[0] != doc {
		return 0, 0, false, nil
	}
	return r.blk.Freqs[0], r.blk.Norms[0], true, nil
}
