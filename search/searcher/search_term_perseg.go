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
	"reflect"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/scorer"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizePerSegmentTermSearcher int

func init() {
	var ts PerSegmentTermSearcher
	reflectStaticSizePerSegmentTermSearcher = int(reflect.TypeOf(ts).Size())
}

// ErrPerSegmentSearcherIterated is returned by the Next and Advance of a
// PerSegmentTermSearcher: its postings are consumed segment by segment through
// PerSegmentReaders, never one DocumentMatch at a time.
var ErrPerSegmentSearcherIterated = errors.New(
	"searcher: a per segment term searcher can't be iterated, it has to be read through its per segment readers")

// PerSegmentTermSearcher is the searcher of a term for the per segment search
// path. It is to the segment-oblivious TermSearcher what the per segment
// collector is to the TopN one: it neither hides the segments nor builds a
// DocumentMatch per posting.
//
// It is a search.PerSegmentSearcher, which is the generic way in which a
// collector can drain it, one scored block of a segment at a time with
// NextBlock. As it knows that it is a lone term, it is also a
// search.OptimizedPerSegmentSearcher, whose CollectOptimized does the
// collection itself:
//
//   - when scored: the best k of every segment, scoring a block per SIMD
//     kernel call and looking at the heap only for blocks that can contribute
//     to it;
//   - when not scored: the first k matches in doc order, and the exact total,
//     which costs nothing for a segment without deletions.
//
// It implements search.Searcher only so that it can be returned by
// Query.Searcher; Next and Advance fail.
type PerSegmentTermSearcher struct {
	readers []*scorch.PerSegmentIndexSnapshotTermFieldReader
	scorer  *scorer.PerSegmentTermScorer
	scored  bool
	count   uint64

	// the segment NextBlock is on
	nextSeg int
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

// NewPerSegmentTermSearcher returns the per segment searcher of the term, or
// nil (and no error) if the term search can't be done that way, in which case
// NewTermSearcher is what has to be used.
//
// Only a request-level opt in (search.PerSegmentSearchKey in the context) makes
// it eligible, on top of what's needed to be served correctly by scalars alone:
// segments that can be read that way, and a search that needs neither
// explanations nor locations. A search without scores is fine.
func NewPerSegmentTermSearcher(ctx context.Context, indexReader PerSegmentIndexReader,
	term string, field string, boost float64, options search.SearcherOptions) (
	*PerSegmentTermSearcher, error) {
	if ctx == nil {
		return nil, nil
	}
	if enabled, _ := ctx.Value(search.PerSegmentSearchKey).(bool); !enabled {
		return nil, nil
	}
	if options.Explain || options.IncludeTermVectors {
		return nil, nil
	}
	scored := options.Score != "none"
	// synonyms turn a term search into a disjunction
	if fts, ok := ctx.Value(search.FieldTermSynonymMapKey).(search.FieldTermSynonymMap); ok {
		if _, exists := fts[field]; exists {
			return nil, nil
		}
	}
	if isTermQuery(ctx) {
		ctx = context.WithValue(ctx, search.QueryTypeKey, search.Term)
	}

	readers, err := indexReader.PerSegmentTermFieldReader(ctx, []byte(term), field, scored)
	if err != nil {
		if errors.Is(err, scorch.ErrPerSegmentUnsupported) {
			return nil, nil
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

// NextBlock implements search.PerSegmentSearcher.
func (s *PerSegmentTermSearcher) NextBlock(b *search.PerSegmentScoredBlock) (int, error) {
	for s.nextSeg < len(s.readers) {
		r := s.readers[s.nextSeg]
		if r == nil {
			s.nextSeg++
			continue
		}
		blk, n, err := r.NextBlock()
		if err != nil {
			return 0, err
		}
		if n == 0 {
			s.nextSeg++
			continue
		}

		b.Seg = s.nextSeg
		b.Offset = r.Offset()
		copy(b.Docs[:n], blk.Docs[:n])
		if s.scored {
			b.MaxScore = s.scorer.ScoreBlock(&blk.Freqs, &blk.Norms, n, &b.Scores)
		} else {
			clear(b.Scores[:n])
			b.MaxScore = 0
		}
		return n, nil
	}
	return 0, nil
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

// collectScored scores every segment's matches a block at a time, keeping the
// best k of each.
func (s *PerSegmentTermSearcher) collectScored(ctx context.Context,
	sink search.PerSegmentSink) error {
	var scores [scorer.PerSegmentBlockLen]float32
	var sinceCheck int
	for seg, r := range s.readers {
		if r == nil {
			continue
		}
		h := sink.Heap(seg)
		offset := r.Offset()
		var total uint64

		for {
			blk, n, err := r.NextBlock()
			if err != nil {
				return err
			}
			if n == 0 {
				break
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
			ord := uint32(total)
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
					Ord:   ord + uint32(i),
					Seg:   uint32(seg),
				})
				thr, full = h.Threshold()
			}
		}
		sink.AddTotal(seg, total)
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

func (s *PerSegmentTermSearcher) Next(ctx *search.SearchContext) (*search.DocumentMatch, error) {
	return nil, ErrPerSegmentSearcherIterated
}

func (s *PerSegmentTermSearcher) Advance(ctx *search.SearchContext,
	ID index.IndexInternalID) (*search.DocumentMatch, error) {
	return nil, ErrPerSegmentSearcherIterated
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

func (s *PerSegmentTermSearcher) DocumentMatchPoolSize() int {
	return 0
}
