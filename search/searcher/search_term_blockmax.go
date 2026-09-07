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

// blockMaxReader is the reader-side capability the block-max WAND skip loop
// needs; scorch implements both methods on IndexSnapshotTermFieldReader.
//
// BlockMax reports an upper bound on the reader's scoring contribution for
// every document up to and including lastDoc, without decoding anything;
// ShallowAdvance moves past that span the same way, touching only a skip
// structure.
type blockMaxReader interface {
	// BlockMax reports (term freq, norm factor) chosen so that scoring it
	// through the caller's own formula bounds every document up to and
	// including lastDoc, without decoding anything, plus how many documents
	// that span covers. docCount lets a caller that skips the span keep an
	// exact hit count without knowing anything about the segment's block
	// size. See zapx's postings_format.go for how that pair is chosen and
	// what it assumes about the scorer -- UsesBM25 is the corresponding
	// guard on this side.
	BlockMax() (maxTF uint64, maxNormFactor float64, lastDoc uint64, docCount int, ok bool)
	// ShallowAdvance moves past the span BlockMax just described, touching
	// only a skip structure.
	ShallowAdvance(target uint64) error
}

// skipUncompetitiveBlocks advances the reader past any block whose maximum
// possible score cannot beat the current threshold, without decoding or
// scoring anything in it -- the core of block-max WAND: for a term that's
// common enough to make most of its postings irrelevant to a small top-K,
// this is the difference between decoding every one of them and decoding only
// the ones near a competitive score.
//
// Every document a block covers is a real match of the query (it is, after
// all, in that term's postings) -- skipping it does not make it stop
// counting toward the query's total hit count, so each skip's docCount is
// tallied in skippedDocCount for the collector to fold back in later (see
// search.SkippedForCompetitiveScore), keeping that count exact rather than
// merely a lower bound.
//
// Safe to call unconditionally: a no-op whenever there is no threshold yet
// (SetMinCompetitiveScore hasn't been called), the scorer can't make use of
// one (Explain needs a real score for every candidate, not just the survivors
// -- see TermQueryScorer.CanScoreBulk), the scorer isn't actually running BM25
// (see TermQueryScorer.UsesBM25 -- zapx's stored bound carries no guarantee
// under any other formula), or the reader has nothing useful to report a
// bound for (a 1-hit term, a conjunction-narrowed iterator, the tail of a
// postings list, live deletions, or a segment implementation without this
// capability). In every such case the caller should just proceed with its
// normal fetch.
func (s *TermSearcher) skipUncompetitiveBlocks() error {
	if !s.hasMinCompetitiveScore || !s.scorer.CanScoreBulk() || !s.scorer.UsesBM25() {
		return nil
	}
	bm, ok := s.reader.(blockMaxReader)
	if !ok {
		return nil
	}
	for {
		maxTF, maxNorm, lastDoc, docCount, ok := bm.BlockMax()
		if !ok {
			return nil
		}
		bound := s.scorer.MaxScore(maxTF, maxNorm)
		if bound > s.minCompetitiveScore {
			return nil
		}
		s.skippedDocCount += uint64(docCount)
		if err := bm.ShallowAdvance(lastDoc + 1); err != nil {
			return err
		}
	}
}
