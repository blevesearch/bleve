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

import (
	"context"
	"errors"
)

// The per segment search path: a Searcher that produces its matches one at a
// time, tagged with the segment they belong to, and a collector that consumes
// them. Nothing here knows about DocumentMatches, locations or any index types,
// only doc numbers and scores.
//
// The collector is generic. All it does is call NextMatch until the searcher is
// exhausted, put what it gets in the heap of the segment the match came from,
// and, at the end, merge those heaps into the top hits.
//
// A searcher that knows how to do better than that for the kind of search it
// is (a lone term, say) can offer an optimized path: OptimizedPerSegmentSearcher.
// The collector then hands it a PerSegmentSink and leaves the whole collection
// to it, so that query specific tricks live with the searcher and don't bloat
// the collector.

// PerSegmentBlockLen is the number of postings in a block of the postings that
// the per segment path reads and scores together.
const PerSegmentBlockLen = 128

// PerSegmentMatch is a match of a search, found by a PerSegmentSearcher.
type PerSegmentMatch struct {
	// Seg is the index of the segment the match is from.
	Seg int
	// Doc is the number of the doc across the index (its number in the segment
	// plus the offset of the segment).
	Doc uint64
	// Score is the match's score, 0 for a search that has no scores.
	Score float32
}

// PerSegmentSearcher is the searcher of the per segment path: a search that is
// consumed a match at a time, and that knows which segment each match is from. It
// is not a Searcher, which is consumed a DocumentMatch at a time and knows of no
// segments: the two are built differently (see query.PerSegmentQuery) and read
// differently, and a collector of one can't read the other.
//
// This is the generic contract; it is all that a per segment collector needs. The
// other methods are what the searchers need of each other: the weights that make
// the query norm, and the counts and minimum that the composites look at.
type PerSegmentSearcher interface {
	// NextMatch returns the next match, and false once the searcher is
	// exhausted. The matches of a segment come together, in ascending doc
	// number order, and the segments come in index order.
	NextMatch() (match PerSegmentMatch, ok bool, err error)

	// Close releases what the searcher holds. It has to be called, once the
	// searcher has been read, and nothing may be done with it after.
	Close() error

	// Weight and SetQueryNorm are those of Searcher: the share of the query norm
	// that the searcher is responsible for, and the setting of the norm.
	Weight() float64
	SetQueryNorm(float64)

	// Count is the number of matches the searcher has, at most. Min is how many of
	// its clauses at least have to match, if it has clauses to be required.
	Count() uint64
	Min() int

	// Size is an estimate of the memory the searcher needs.
	Size() int
}

// ErrPerSegmentUnsupported is returned by the building of a per segment searcher
// that can't be done: the query, or the index, is not of a kind that the per
// segment path serves. A search that gets it is served by the regular path.
var ErrPerSegmentUnsupported = errors.New("search: the per segment path can't serve this search")

// OptimizedPerSegmentSearcher is implemented by searchers that can collect
// faster than by being drained match after match.
type OptimizedPerSegmentSearcher interface {
	PerSegmentSearcher

	// CanCollectOptimized reports whether the searcher can do the collection
	// itself. If it can't, the collector falls back to NextMatch.
	CanCollectOptimized() bool

	// CollectOptimized collects every match of the searcher into the sink:
	// the best Limit() of each segment in the segment's heap, and the number
	// of matches and the highest score. It leaves the searcher exhausted.
	CollectOptimized(ctx context.Context, sink PerSegmentSink) error
}

// PerSegmentSink is what the collector offers a searcher's optimized path.
// What gets collected is the same as with NextMatch: the best hits, and totals.
type PerSegmentSink interface {
	// Limit is how many hits the collection can use in all.
	Limit() int

	// Heap is the heap that the hits of segment seg are offered to. The hits
	// of all the segments may go to the same heap, so Threshold is a bound
	// across segments. Segments have to be collected one after the other, in
	// index order, and the hits of a segment offered in ascending doc number
	// order: that is what lets a hit which ties the worst kept one be turned
	// down without comparing doc numbers.
	Heap(seg int) *PerSegmentHeap

	// Threshold is the score a hit has to beat to be kept, if the heap is full
	// and so there is one. It only ever goes up.
	Threshold() (score float32, full bool)

	// AddTotal adds n to the number of matches of segment seg. Every match
	// that was visited has to be counted, offered to the heap or not. The Ord
	// of a hit that is offered is its 0-based position among those.
	AddTotal(seg int, n uint64)

	// ObserveMaxScore reports the highest score of a block (or a segment).
	ObserveMaxScore(score float32)

	// MarkPruned records that the collection didn't visit every match, because
	// it could tell that they could not make the top hits. The total is then a
	// lower bound, not the number of matches. Max score and the top hits are
	// still exact.
	MarkPruned()
}

// DrainPerSegmentSearcher is the generic collection: ask the searcher for its
// next match until there are none, offering each to the heap of its segment.
// Every match is visited and counted.
func DrainPerSegmentSearcher(ctx context.Context, searcher PerSegmentSearcher,
	sink PerSegmentSink, checkDoneEvery uint64) error {
	var (
		seg      = -1 // the segment being read, whose matches are counted
		h        *PerSegmentHeap
		thr      float32
		full     bool
		total    uint64  // matches of the segment so far
		maxScore float32 // the highest score of its matches
		seen     uint64
	)
	// what is known of a segment is told to the sink when it's done with
	flush := func() {
		if seg >= 0 {
			sink.AddTotal(seg, total)
			sink.ObserveMaxScore(maxScore)
		}
	}
	for {
		m, ok, err := searcher.NextMatch()
		if err != nil {
			return err
		}
		if !ok {
			flush()
			return nil
		}

		if seen%checkDoneEvery == 0 {
			select {
			case <-ctx.Done():
				RecordSearchCost(ctx, AbortM, 0)
				return ctx.Err()
			default:
			}
		}
		seen++

		if m.Seg != seg {
			flush()
			seg, total, maxScore = m.Seg, 0, 0
			h = sink.Heap(seg)
			thr, full = h.Threshold()
		}
		ord := uint32(total)
		total++
		if m.Score > maxScore {
			maxScore = m.Score
		}

		// Matches come in ascending doc order, so one that ties the worst hit
		// kept ranks below it: only a better score gets in.
		if full && m.Score <= thr {
			continue
		}
		h.Offer(PerSegmentHit{Score: m.Score, Doc: m.Doc, Ord: ord, Seg: uint32(seg)})
		thr, full = h.Threshold()
	}
}

// PerSegmentHit is a candidate hit of a segment.
type PerSegmentHit struct {
	Score float32
	Doc   uint64 // global doc number
	// Ord is the 0-based position of the hit among the matches of its segment
	Ord uint32
	Seg uint32
}

// Worse reports whether a ranks below b: a lower score, or the same score and
// a later doc number (the order in which the TopN collector breaks ties).
func (a PerSegmentHit) Worse(b PerSegmentHit) bool {
	if a.Score != b.Score {
		return a.Score < b.Score
	}
	return a.Doc > b.Doc
}

// PerSegmentHeap is a min-heap of the best k hits offered to it: its root is
// the worst of them.
type PerSegmentHeap struct {
	hits []PerSegmentHit
	k    int
}

// perSegmentHeapPreAlloc caps what the heap allocates up front.
const perSegmentHeapPreAlloc = 1000

func NewPerSegmentHeap(k int) *PerSegmentHeap {
	backing := k
	if backing > perSegmentHeapPreAlloc {
		backing = perSegmentHeapPreAlloc
	}
	return &PerSegmentHeap{hits: make([]PerSegmentHit, 0, backing), k: k}
}

// Len is how many hits are kept.
func (h *PerSegmentHeap) Len() int { return len(h.hits) }

// Hits are the hits kept, in no particular order.
func (h *PerSegmentHeap) Hits() []PerSegmentHit { return h.hits }

// Threshold returns the score that a hit has to beat to be kept, if the heap
// is full and so there is one. A hit which merely ties it ranks below it, as
// long as hits are offered in ascending doc number order.
func (h *PerSegmentHeap) Threshold() (score float32, full bool) {
	if h.k == 0 {
		return 0, true
	}
	if len(h.hits) < h.k {
		return 0, false
	}
	return h.hits[0].Score, true
}

// Offer adds the hit if it ranks among the best k so far.
func (h *PerSegmentHeap) Offer(hit PerSegmentHit) {
	if h.k == 0 {
		return
	}
	if len(h.hits) < h.k {
		h.hits = append(h.hits, hit)
		h.siftUp(len(h.hits) - 1)
		return
	}
	if h.hits[0].Worse(hit) {
		h.hits[0] = hit
		h.siftDown(0)
	}
}

func (h *PerSegmentHeap) siftUp(i int) {
	hs := h.hits
	for i > 0 {
		parent := (i - 1) / 2
		if !hs[i].Worse(hs[parent]) {
			break
		}
		hs[i], hs[parent] = hs[parent], hs[i]
		i = parent
	}
}

func (h *PerSegmentHeap) siftDown(i int) {
	hs := h.hits
	n := len(hs)
	for {
		l := 2*i + 1
		if l >= n {
			return
		}
		worst := l
		if r := l + 1; r < n && hs[r].Worse(hs[l]) {
			worst = r
		}
		if !hs[worst].Worse(hs[i]) {
			return
		}
		hs[i], hs[worst] = hs[worst], hs[i]
		i = worst
	}
}
