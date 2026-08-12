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

package searcher

import (
	"bytes"
	"encoding/binary"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// compareIDs orders two index-internal document IDs.
//
// Scorch encodes these as 8-byte big-endian doc numbers, and the comparison
// sits in the inner loop of every disjunction/conjunction merge — profiling a
// 700-clause wildcard disjunction showed ~10% of total query time inside
// bytes.Compare's assembly routine alone. Handling the fixed 8-byte case
// inline turns it into a single integer comparison. Other ID widths (e.g.
// upsidedown's) fall back to the generic byte comparison.
func compareIDs(a, b index.IndexInternalID) int {
	if len(a) == 8 && len(b) == 8 {
		x := binary.BigEndian.Uint64(a)
		y := binary.BigEndian.Uint64(b)
		if x < y {
			return -1
		}
		if x > y {
			return 1
		}
		return 0
	}
	return bytes.Compare(a, b)
}

// idKey decodes a document match's internal ID into a uint64 sort key. The
// second result reports whether the ID had the fixed 8-byte width that makes
// integer comparison equivalent to byte comparison; callers must fall back to
// compareIDs when it is false.
func idKey(dm *search.DocumentMatch) (uint64, bool) {
	if dm == nil || len(dm.IndexInternalID) != 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(dm.IndexInternalID), true
}

// equalIDs reports whether two index-internal document IDs are equal, with the
// same 8-byte fast path as compareIDs.
func equalIDs(a, b index.IndexInternalID) bool {
	if len(a) == 8 && len(b) == 8 {
		return binary.BigEndian.Uint64(a) == binary.BigEndian.Uint64(b)
	}
	return bytes.Equal(a, b)
}
