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
	"github.com/blevesearch/bleve/v2/search"
)

// blockTermFieldReader is the reader-side capability TermSearcher needs; scorch
// implements it on IndexSnapshotTermFieldReader.
type blockTermFieldReader interface {
	NextBlock(docNums []uint64, freqs []uint64, norms []float64) (int, error)
}

// ScoreBlock pulls a block of postings straight out of the segment and scores
// them into flat arrays.
//
// The generic path costs a TermFieldDoc fill, a pooled DocumentMatch, and a
// collector callback for every document — measured at roughly half of a term
// scan, and paid for the ~99.99% of documents that never enter the top-N.
func (s *TermSearcher) ScoreBlock(blk *search.DocScoreBlock) (int, error) {
	br, ok := s.reader.(blockTermFieldReader)
	if !ok {
		return 0, nil
	}
	n, err := br.NextBlock(blk.IDs, blk.Freqs, blk.Norms)
	if err != nil || n == 0 {
		return n, err
	}
	s.scorer.ScoreBulk(blk.Freqs[:n], blk.Norms[:n], blk.Scores[:n])
	return n, nil
}

// CanScoreBlock reports whether this searcher's reader and options allow the
// block path.
func (s *TermSearcher) CanScoreBlock() bool {
	br, ok := s.reader.(blockTermFieldReader)
	if !ok {
		return false
	}
	type blockCapable interface{ SupportsBlocks() bool }
	if bc, ok := br.(blockCapable); ok && !bc.SupportsBlocks() {
		return false
	}
	return s.scorer.CanScoreBulk()
}

var _ search.BulkSearcher = (*TermSearcher)(nil)
