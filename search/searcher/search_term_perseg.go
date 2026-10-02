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
	"context"
	"errors"
	"math"
	"reflect"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/scorer"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
	segment "github.com/blevesearch/scorch_segment_api/v2"
)

var reflectStaticSizePerSegmentTermSearcher int

func init() {
	var ts PerSegmentTermSearcher
	reflectStaticSizePerSegmentTermSearcher = int(reflect.TypeOf(ts).Size())
}

// PerSegmentTermSearcher is the searcher of a term for the per segment search
// path. It is to the segment-oblivious TermSearcher what the per segment
// collector is to the TopN one: it neither hides the segments nor builds a
// DocumentMatch per posting.
//
// It is a search.PerSegmentSearcher, which is the generic way in which a
// collector can drain it, a match at a time with NextMatch. As it knows that it is a lone term, it is also a
// search.OptimizedPerSegmentSearcher, whose CollectOptimized does the
// collection itself:
//
//   - when scored: the best k of every segment, scoring a block per SIMD
//     kernel call and, once the heap is full, passing over the blocks that the
//     skip data says can't beat its worst hit without decoding them. The total is
//     exact for a segment without deletions; with deletions, a segment that
//     skipped a block counts what it visited and the search is marked as pruned;
//   - when not scored: the first k matches in doc order, and the exact total,
//     which costs nothing for a segment without deletions.
type PerSegmentTermSearcher struct {
	readers []*scorch.PerSegmentIndexSnapshotTermFieldReader
	scorer  *scorer.PerSegmentTermScorer
	scored  bool
	count   uint64

	// what an explanation of a match shows
	field string
	term  string

	// where NextMatch is: the segment, and in it the block of postings being given
	// out, with its scores
	nextSeg int
	blk     *segment.PostingsBlock
	blkN    int
	blkPos  int
	scores  [scorer.PerSegmentBlockLen]float32
}

var _ search.PerSegmentSearcher = (*PerSegmentTermSearcher)(nil)
var _ search.OptimizedPerSegmentSearcher = (*PerSegmentTermSearcher)(nil)

// PerSegmentIndexReader is an index reader that can also hand out a term's
// postings segment by segment. The query checks that its reader is one before
// asking for a per segment searcher.
type PerSegmentIndexReader interface {
	index.IndexReader
	scorch.PerSegmentIndexReader
}

// NewPerSegmentTermSearcher returns the per segment searcher of the term. It
// returns search.ErrPerSegmentUnsupported if the term can't be searched that way,
// in which case NewTermSearcher is what has to be used: the index reader isn't one
// that hands out postings by segment, or its segments can't be read that way, or
// the search needs more than the per segment path gives (term vectors, synonyms).
// A search without scores is fine.
func NewPerSegmentTermSearcher(ctx context.Context, indexReader index.IndexReader,
	term string, field string, boost float64, options search.SearcherOptions) (
	*PerSegmentTermSearcher, error) {
	psReader, ok := indexReader.(PerSegmentIndexReader)
	if !ok {
		return nil, search.ErrPerSegmentUnsupported
	}
	// An explanation is not built while searching: the collector asks for the
	// explanations of the hits it returns once it has them (ExplainMatch). Term
	// vectors are another matter.
	if options.IncludeTermVectors {
		return nil, search.ErrPerSegmentUnsupported
	}
	scored := options.Score != "none"
	// synonyms turn a term search into a disjunction
	if fts, ok := ctx.Value(search.FieldTermSynonymMapKey).(search.FieldTermSynonymMap); ok {
		if _, exists := fts[field]; exists {
			return nil, search.ErrPerSegmentUnsupported
		}
	}
	if isTermQuery(ctx) {
		ctx = context.WithValue(ctx, search.QueryTypeKey, search.Term)
	}

	readers, err := psReader.PerSegmentTermFieldReader(ctx, []byte(term), field, scored)
	if err != nil {
		if errors.Is(err, scorch.ErrPerSegmentUnsupported) {
			return nil, search.ErrPerSegmentUnsupported
		}
		return nil, err
	}
	closeReaders := func() {
		for _, r := range readers {
			if r != nil {
				_ = r.Close()
			}
		}
	}

	// same bookkeeping as for any other leaf term searcher
	if err := search.RecordTermSearcher(ctx); err != nil {
		closeReaders()
		return nil, err
	}

	var similarityModel string
	if similarityModelCallback, ok := ctx.Value(search.
		GetScoringModelCallbackKey).(search.GetScoringModelCallbackFn); ok {
		similarityModel = similarityModelCallback()
	}

	var count uint64
	var avgDocLength float64
	switch similarityModel {
	case index.BM25Scoring:
		count, avgDocLength, err = bm25ScoreMetrics(ctx, field, indexReader)
	default:
		count, err = tfIDFScoreMetrics(indexReader)
	}
	if err != nil {
		closeReaders()
		return nil, err
	}

	var docTerm uint64
	for _, r := range readers {
		if r != nil {
			docTerm += r.Count()
		}
	}

	return &PerSegmentTermSearcher{
		readers: readers,
		scorer:  scorer.NewPerSegmentTermScorer(boost, count, docTerm, avgDocLength),
		scored:  scored,
		count:   docTerm,
		field:   field,
		term:    term,
	}, nil
}

// Readers are the term's readers, one entry per segment of the index, nil for
// each segment without the term. A composite per segment searcher drives them
// itself.
func (s *PerSegmentTermSearcher) Readers() []*scorch.PerSegmentIndexSnapshotTermFieldReader {
	return s.readers
}

// Scorer is the scorer of the term's postings.
func (s *PerSegmentTermSearcher) Scorer() *scorer.PerSegmentTermScorer { return s.scorer }

// IsScored reports whether the search wants scores.
func (s *PerSegmentTermSearcher) IsScored() bool { return s.scored }

// NextMatch implements search.PerSegmentSearcher. The postings are read a block at a
// time, and a block is scored in one go by the SIMD kernel when it's read.
func (s *PerSegmentTermSearcher) NextMatch() (search.PerSegmentMatch, bool, error) {
	for s.nextSeg < len(s.readers) {
		r := s.readers[s.nextSeg]
		if r == nil {
			s.nextSeg++
			continue
		}
		if s.blkPos < s.blkN {
			m := search.PerSegmentMatch{Seg: s.nextSeg, Doc: r.Offset() + uint64(s.blk.Docs[s.blkPos])}
			if s.scored {
				m.Score = s.scores[s.blkPos]
			}
			s.blkPos++
			return m, true, nil
		}
		blk, n, err := r.NextBlock()
		if err != nil {
			return search.PerSegmentMatch{}, false, err
		}
		if n == 0 {
			s.nextSeg++
			s.blkN, s.blkPos = 0, 0
			continue
		}
		s.blk, s.blkN, s.blkPos = blk, n, 0
		if s.scored {
			s.scorer.ScoreBlock(&blk.Freqs, &blk.Norms, n, &s.scores)
		}
	}
	return search.PerSegmentMatch{}, false, nil
}

// CanCollectOptimized implements search.OptimizedPerSegmentSearcher. A lone
// term always can: both with scores and without.
func (s *PerSegmentTermSearcher) CanCollectOptimized() bool { return true }

// CollectOptimized implements search.OptimizedPerSegmentSearcher.
func (s *PerSegmentTermSearcher) CollectOptimized(ctx context.Context,
	sink search.PerSegmentSink) error {
	if s.scored {
		return s.collectScored(ctx, sink)
	}
	return s.collectUnscored(ctx, sink)
}

// checkDoneEvery is how many matches are collected between looks at the
// context.
const checkDoneEvery = 1024

// collectScored scores the matches of every segment a block at a time, keeping
// the best k of them.
//
// Once the heap is full a block is only worth reading if it can beat the worst
// hit in it, and the skip data tells what a block can score at most without
// decoding it: a block that can't is passed over (the lone term version of
// block-max WAND). Whatever is skipped can't change the top hits or the max
// score; it only isn't visited.
//
// The total stays exact for a segment without deletions, as the term's count
// is then the number of its matches. With deletions it can't be known without
// walking the postings, so a segment that skipped a block counts only what it
// visited, and the collection is marked as pruned: the total is a lower bound.
func (s *PerSegmentTermSearcher) collectScored(ctx context.Context,
	sink search.PerSegmentSink) error {
	var scores [scorer.PerSegmentBlockLen]float32
	var sinceCheck int
	// a scorer whose idf is negative has no bounds; nor does a collection that
	// wants no hits
	prune := s.scorer.Prunable() && sink.Limit() > 0
	for seg, r := range s.readers {
		if r == nil {
			continue
		}
		h := sink.Heap(seg)
		offset := r.Offset()
		var total uint64 // matches visited
		var ord uint32   // the place of the next match among the segment's
		skipping := prune && r.HasBlockMax()
		exact := skipping && !r.HasDeletions()
		skipped := false
		var target uint32 // the next block holds the first match >= target
		// the bound of the last block looked at: neighbors tend to have the same
		var bound struct {
			freq    uint32
			norm    float32
			bounded bool
			ub      float32
			valid   bool
		}

		for {
			var bd segment.BlockBounds
			if skipping {
				var ok bool
				bd, ok = r.BoundsAt(target)
				if !ok {
					break
				}
				if !bound.valid || bound.freq != bd.MaxFreq || bound.norm != bd.MaxNorm ||
					bound.bounded != bd.FreqBounded {
					bound.freq, bound.norm, bound.bounded = bd.MaxFreq, bd.MaxNorm, bd.FreqBounded
					bound.ub = s.scorer.UpperBound(bd.MaxFreq, bd.MaxNorm, bd.FreqBounded)
					bound.valid = true
				}
				if thr, full := h.Threshold(); full && bound.ub <= thr {
					skipped = true
					if bd.LastDoc == math.MaxUint32 {
						break
					}
					// a block that is passed over has all of its 128 matches or,
					// if it is the last, nothing follows it
					ord += scorer.PerSegmentBlockLen
					target = bd.LastDoc + 1
					continue
				}
			}

			var blk *segment.PostingsBlock
			var n int
			var err error
			if skipping {
				blk, n, err = r.SeekBlock(target)
			} else {
				blk, n, err = r.NextBlock()
			}
			if err != nil {
				return err
			}
			if n == 0 {
				break
			}
			if skipping {
				// the block read is the one that was bounded unless all of its
				// postings were deleted, in which case it's a later one
				target = max(bd.LastDoc, blk.Docs[n-1])
				if target == math.MaxUint32 {
					skipping = false // nothing can follow
				} else {
					target++
				}
			}

			if sinceCheck >= checkDoneEvery {
				sinceCheck = 0
				select {
				case <-ctx.Done():
					search.RecordSearchCost(ctx, search.AbortM, 0)
					return ctx.Err()
				default:
				}
			}
			sinceCheck += n

			blockMax := s.scorer.ScoreBlock(&blk.Freqs, &blk.Norms, n, &scores)
			blockOrd := ord
			ord += uint32(n)
			total += uint64(n)
			sink.ObserveMaxScore(blockMax)

			// A full heap only takes a better score than its worst: postings
			// come in ascending doc order, so one that ties it ranks below it.
			// A block whose best score doesn't beat it has nothing to offer.
			thr, full := h.Threshold()
			if full && blockMax <= thr {
				continue
			}
			for i := 0; i < n; i++ {
				sc := scores[i]
				if full && sc <= thr {
					continue
				}
				h.Offer(search.PerSegmentHit{
					Score: sc,
					Doc:   offset + uint64(blk.Docs[i]),
					Ord:   blockOrd + uint32(i),
					Seg:   uint32(seg),
				})
				thr, full = h.Threshold()
			}
		}
		switch {
		case exact:
			sink.AddTotal(seg, r.Count())
		default:
			sink.AddTotal(seg, total)
			if skipped {
				sink.MarkPruned()
			}
		}
	}
	return nil
}

// collectUnscored takes the first k matches in doc order, segment after
// segment, and counts all of them. Without scores, which matches are kept
// comes down to doc order alone, and the count of a segment doesn't need its
// postings to be walked, unless there are deletions among them.
func (s *PerSegmentTermSearcher) collectUnscored(ctx context.Context,
	sink search.PerSegmentSink) error {
	k := sink.Limit()
	var taken int
	for seg, r := range s.readers {
		if r == nil {
			continue
		}
		select {
		case <-ctx.Done():
			search.RecordSearchCost(ctx, search.AbortM, 0)
			return ctx.Err()
		default:
		}

		live, err := r.LiveCount()
		if err != nil {
			return err
		}
		sink.AddTotal(seg, live)

		// once there are k, whatever follows is only counted
		h := sink.Heap(seg)
		offset := r.Offset()
		var ord uint32
		for taken < k && live > 0 {
			blk, n, err := r.NextBlock()
			if err != nil {
				return err
			}
			if n == 0 {
				break
			}
			for i := 0; i < n && taken < k; i++ {
				h.Offer(search.PerSegmentHit{
					Doc: offset + uint64(blk.Docs[i]),
					Ord: ord,
					Seg: uint32(seg),
				})
				ord++
				taken++
			}
		}
	}
	return nil
}

func (s *PerSegmentTermSearcher) Size() int {
	return reflectStaticSizePerSegmentTermSearcher + size.SizeOfPtr +
		len(s.readers)*size.SizeOfPtr
}

func (s *PerSegmentTermSearcher) Count() uint64 {
	return s.count
}

func (s *PerSegmentTermSearcher) Weight() float64 {
	return s.scorer.Weight()
}

func (s *PerSegmentTermSearcher) SetQueryNorm(qnorm float64) {
	s.scorer.SetQueryNorm(qnorm)
}

func (s *PerSegmentTermSearcher) Close() error {
	// A closed reader is back in a pool, and may be some other query's at once:
	// the searcher lets go of them, so that nothing can reach one through it.
	readers := s.readers
	s.readers = nil
	var rv error
	for _, r := range readers {
		if r == nil {
			continue
		}
		if err := r.Close(); err != nil && rv == nil {
			rv = err
		}
	}
	return rv
}

func (s *PerSegmentTermSearcher) Min() int {
	return 0
}
