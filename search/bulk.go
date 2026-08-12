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

package search

// BlockSize is how many postings a bulk scorer produces per call. Large enough
// to amortise the call across the block, small enough that the arrays stay in
// cache.
const BlockSize = 256

// DocScoreBlock carries a block of scored documents as flat arrays, so a scan
// need not materialise a DocumentMatch for a document that will lose.
type DocScoreBlock struct {
	IDs    []uint64
	Scores []float64

	// scratch used by producers to decode into
	Freqs []uint64
	Norms []float64
}

func NewDocScoreBlock() *DocScoreBlock {
	return &DocScoreBlock{
		IDs:    make([]uint64, BlockSize),
		Scores: make([]float64, BlockSize),
		Freqs:  make([]uint64, BlockSize),
		Norms:  make([]float64, BlockSize),
	}
}

// BulkSearcher is implemented by searchers that can produce scored documents a
// block at a time. Collectors use it to bypass the per-document object
// pipeline; searchers that do not implement it keep working through Next().
type BulkSearcher interface {
	// CanScoreBlock reports whether the block path is usable for this searcher
	// as currently configured.
	CanScoreBlock() bool
	// ScoreBlock fills blk and returns how many documents were written, or 0
	// once the searcher is exhausted.
	ScoreBlock(blk *DocScoreBlock) (int, error)
}
