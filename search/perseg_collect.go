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
)

// The per segment search path: a Searcher that produces its matches a block at
// a time, tagged with the segment they belong to, and a collector that
// consumes them. Nothing here knows about DocumentMatches, locations or any
// index types, only doc numbers and scores.
//
// The collector is generic. All it does is call NextBlock until the searcher
// is exhausted, put what it gets in the heap of the segment the block came
// from, and, at the end, merge those heaps into the top hits.
//
// A searcher that knows how to do better than that for the kind of search it
// is (a lone term, say) can offer an optimized path: OptimizedPerSegmentSearcher.
// The collector then hands it a PerSegmentSink and leaves the whole collection
// to it, so that query specific tricks live with the searcher and don't bloat
// the collector.

// PerSegmentBlockLen is the most matches that a PerSegmentScoredBlock holds.
const PerSegmentBlockLen = 128

// PerSegmentScoredBlock is a batch of the matches of one segment.
type PerSegmentScoredBlock struct {
	// Seg is the index of the segment the matches are from, and Offset is what
	// has to be added to one of its local doc numbers to make it unique across
	// the index.
	Seg    int
	Offset uint64

	// Docs are segment local doc numbers, strictly ascending.
	Docs   [PerSegmentBlockLen]uint32
	Scores [PerSegmentBlockLen]float32

	// MaxScore is the highest of the first n Scores.
	MaxScore float32
}

// PerSegmentSearcher is a Searcher that is consumed a block of matches at a
// time. This is the generic contract; it is all that a per segment collector
// needs. Next and Advance of such a searcher fail.
type PerSegmentSearcher interface {
	Searcher

	// NextBlock fills b with the next matches and returns how many there are;
	// 0 means that the searcher is exhausted. The matches of a segment come in
	// consecutive blocks, in ascending doc number order, and the segments come
	// in index order. A search that has no scores gives all of them a score of
	// 0.
	NextBlock(b *PerSegmentScoredBlock) (n int, err error)
}

// OptimizedPerSegmentSearcher is implemented by searchers that can collect
// faster than by being drained block after block.
type OptimizedPerSegmentSearcher interface {
	PerSegmentSearcher

	// CanCollectOptimized reports whether the searcher can do the collection
	// itself. If it can't, the collector falls back to NextBlock.
	CanCollectOptimized() bool

	// CollectOptimized collects every match of the searcher into the sink:
	// the best Limit() of each segment in the segment's heap, and the number
	// of matches and the highest score. It leaves the searcher exhausted.
	CollectOptimized(ctx context.Context, sink PerSegmentSink) error
}

// PerSegmentSink is what the collector offers a searcher's optimized path.
// What gets collected is the same as with NextBlock: the best hits, and totals.
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
// next block until there are none, offering each match to the heap of its
// segment. Every match is visited and counted.
func DrainPerSegmentSearcher(ctx context.Context, searcher PerSegmentSearcher,
	sink PerSegmentSink, checkDoneEvery uint64) error {
	var blk PerSegmentScoredBlock
	var totals []uint64
	var sinceCheck uint64
	for {
		n, err := searcher.NextBlock(&blk)
		if err != nil {
			return err
		}
		if n == 0 {
			return nil
		}

		if sinceCheck >= checkDoneEvery {
			sinceCheck = 0
			select {
			case <-ctx.Done():
				RecordSearchCost(ctx, AbortM, 0)
				return ctx.Err()
			default:
			}
		}
		sinceCheck += uint64(n)

		seg := blk.Seg
		h := sink.Heap(seg)
		for len(totals) <= seg {
			totals = append(totals, 0)
		}
		ord := uint32(totals[seg])
		totals[seg] += uint64(n)
		sink.AddTotal(seg, uint64(n))
		sink.ObserveMaxScore(blk.MaxScore)

		// Matches come in ascending doc order, so one that ties the worst hit
		// kept ranks below it: only a better score gets in. A block whose best
		// score doesn't beat it has nothing to offer.
		thr, full := h.Threshold()
		if full && blk.MaxScore <= thr {
			continue
		}
		for i := 0; i < n; i++ {
			sc := blk.Scores[i]
			if full && sc <= thr {
				continue
			}
			h.Offer(PerSegmentHit{
				Score: sc,
				Doc:   blk.Offset + uint64(blk.Docs[i]),
				Ord:   ord + uint32(i),
				Seg:   uint32(seg),
			})
			thr, full = h.Threshold()
		}
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
