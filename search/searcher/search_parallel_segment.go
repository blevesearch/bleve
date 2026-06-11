// Copyright (c) 2024 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package searcher

// §7 Parallel segment search (Approach A)
//
// When EnableParallelSegmentSearch is true and the index has at least
// ParallelSegmentSearchMinSegs segments, DisjunctionSliceSearcher fans out to
// min(GOMAXPROCS, 8) goroutines, each running a full WAND/MAXSCORE search over
// a disjoint segment group. Results are merged and returned in score order.
//
// Cross-goroutine WAND efficiency: a shared atomic threshold is updated
// whenever any goroutine's local top-K fills; other goroutines pick it up on
// the next Next() call so high-scoring segments broadcast a tight threshold
// early, pruning the remainder of the index.
//
// Scoring correctness: shard TermSearchers reuse the same TermQueryScorer as
// the originals (same IDF, same query weights) so scores are comparable across
// shards and the final merge is correct.

import (
	"context"
	"math"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2/search"
)

// EnableParallelSegmentSearch activates parallel segment search for
// DisjunctionSliceSearcher. Disabled by default; enable for serial
// latency-focused workloads where per-query goroutine overhead pays off.
var EnableParallelSegmentSearch = false

// ParallelSegmentSearchMinSegs is the minimum number of index segments required
// to activate parallel search. Below this the goroutine overhead dominates.
var ParallelSegmentSearchMinSegs = 6

// ParallelSegmentSearchShardK is the per-shard top-K collector limit. Each
// goroutine collects at most this many results; the shared atomic threshold
// ensures WAND prunes aggressively once any shard's heap is full.
//
// For correctness, K must be ≥ the query's count (top-N limit); otherwise
// shards may miss results. For optimal WAND pruning, set K close to the
// query count: the heap fills faster, threshold rises sooner, and WAND can
// eliminate candidates before other goroutines see them.
var ParallelSegmentSearchShardK = 100

// sharedThreshold is a lock-free monotonically increasing float64 shared
// across goroutines. Any shard can raise it; no shard can lower it.
// IEEE 754 positive floats have the same ordering as their uint64 bit patterns,
// so we can use integer CAS for the update.
type sharedThreshold struct {
	bits uint64 // atomic; stores float64 via math.Float64bits
}

func (st *sharedThreshold) Get() float64 {
	return math.Float64frombits(atomic.LoadUint64(&st.bits))
}

// Update atomically raises the threshold to v when v > current value.
func (st *sharedThreshold) Update(v float64) {
	newBits := math.Float64bits(v)
	for {
		old := atomic.LoadUint64(&st.bits)
		if old >= newBits { // IEEE 754: positive float ordering == uint64 ordering
			return
		}
		if atomic.CompareAndSwapUint64(&st.bits, old, newBits) {
			return
		}
	}
}

// dmMinHeap is a min-heap of DocumentMatch by Score for per-shard top-K.
type dmMinHeap []*search.DocumentMatch

// heapPush appends m and sifts up to maintain min-heap invariant.
func (h *dmMinHeap) heapPush(m *search.DocumentMatch) {
	*h = append(*h, m)
	i := len(*h) - 1
	for i > 0 {
		p := (i - 1) / 2
		if (*h)[p].Score <= (*h)[i].Score {
			break
		}
		(*h)[p], (*h)[i] = (*h)[i], (*h)[p]
		i = p
	}
}

// heapPop removes and returns the minimum-score element.
func (h *dmMinHeap) heapPop() *search.DocumentMatch {
	s := *h
	n := len(s)
	min := s[0]
	s[0] = s[n-1]
	s[n-1] = nil
	*h = s[:n-1]
	// sift down
	i, end := 0, n-1
	for {
		l := 2*i + 1
		if l >= end {
			break
		}
		j := l
		if r := l + 1; r < end && (*h)[r].Score < (*h)[l].Score {
			j = r
		}
		if (*h)[i].Score <= (*h)[j].Score {
			break
		}
		(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
		i = j
	}
	return min
}

// pushBounded adds m to the heap capped at k. Returns the evicted entry (if
// any) and the new heap minimum score once full.
func (h *dmMinHeap) pushBounded(m *search.DocumentMatch, k int) (evicted *search.DocumentMatch, minScore float64) {
	h.heapPush(m)
	if h.Len() > k {
		evicted = h.heapPop()
	}
	if h.Len() == k {
		minScore = (*h)[0].Score
	}
	return evicted, minScore
}

func (h dmMinHeap) Len() int { return len(h) }

// shouldRunParallel returns (true, shardK) when all conditions for parallel
// segment search are met. The ctx value for ParallelSegmentSearchKey overrides
// the global EnableParallelSegmentSearch and ParallelSegmentSearchShardK:
// 0 disables, ≥2 enables with that shardK; absent means use global flags.
// shardK is only meaningful when the bool return is true.
func shouldRunParallel(s *DisjunctionSliceSearcher) (bool, int) {
	shardK := ParallelSegmentSearchShardK

	if v, ok := s.ctx.Value(search.ParallelSegmentSearchKey).(int); ok {
		if v <= 0 {
			return false, 0
		}
		shardK = v
	} else if !EnableParallelSegmentSearch {
		return false, 0
	}

	if runtime.GOMAXPROCS(0) < 2 {
		return false, 0
	}
	if len(s.searchers) == 0 {
		return false, 0
	}
	// All sub-searchers must be *TermSearcher with a stored term (set by
	// newTermSearcherFromReader; nil for synonym/unadorned paths).
	for _, sr := range s.searchers {
		ts, ok := sr.(*TermSearcher)
		if !ok || ts.term == nil {
			return false, 0
		}
	}
	// Enough segments to justify goroutine overhead.
	n := s.searchers[0].(*TermSearcher).NumSegments()
	if n < ParallelSegmentSearchMinSegs {
		return false, 0
	}
	return true, shardK
}

// runParallelSegmentSearch fans the search across P goroutines, each handling
// a contiguous range of segments. Returns all collected results merged and
// sorted by score descending. shardK is the per-shard top-K collector limit.
func runParallelSegmentSearch(
	ctx context.Context,
	s *DisjunctionSliceSearcher,
	shardK int,
) ([]*search.DocumentMatch, error) {
	numSegs := s.searchers[0].(*TermSearcher).NumSegments()
	p := runtime.GOMAXPROCS(0)
	if p > numSegs {
		p = numSegs
	}
	if p > 8 {
		p = 8
	}
	segsPerShard := (numSegs + p - 1) / p

	// Create all shard DSSes sequentially to prevent concurrent SetQueryNorm
	// writes on shared TermQueryScorer objects.
	type shardDSS struct {
		dss *DisjunctionSliceSearcher
	}
	shards := make([]shardDSS, 0, p)

	for g := 0; g < p; g++ {
		start := g * segsPerShard
		end := start + segsPerShard
		if end > numSegs {
			end = numSegs
		}
		if start >= end {
			break
		}
		shardSrs := make([]search.Searcher, len(s.searchers))
		var createErr error
		for i, sr := range s.searchers {
			ts := sr.(*TermSearcher)
			shardTS, err := ts.ForSegmentRange(ctx, start, end)
			if err != nil {
				for j := 0; j < i; j++ {
					_ = shardSrs[j].Close()
				}
				createErr = err
				break
			}
			shardSrs[i] = shardTS
		}
		if createErr != nil {
			for _, sw := range shards {
				_ = sw.dss.Close()
			}
			return nil, createErr
		}
		dss, err := newDisjunctionSliceSearcher(ctx, s.indexReader, shardSrs,
			float64(s.min), s.options, false)
		if err != nil {
			for _, sr := range shardSrs {
				_ = sr.Close()
			}
			for _, sw := range shards {
				_ = sw.dss.Close()
			}
			return nil, err
		}
		shards = append(shards, shardDSS{dss: dss})
	}

	type shardResult struct {
		matches []*search.DocumentMatch
		err     error
	}
	results := make([]shardResult, len(shards))
	var shared sharedThreshold

	var wg sync.WaitGroup
	for g := range shards {
		wg.Add(1)
		go func(g int, dss *DisjunctionSliceSearcher) {
			defer wg.Done()
			matches, err := runShardSearch(ctx, dss, &shared, shardK)
			_ = dss.Close()
			results[g] = shardResult{matches: matches, err: err}
		}(g, shards[g].dss)
	}
	wg.Wait()

	var total int
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		total += len(r.matches)
	}
	all := make([]*search.DocumentMatch, 0, total)
	for _, r := range results {
		all = append(all, r.matches...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Score > all[j].Score })
	return all, nil
}

// runShardSearch runs a full WAND/MAXSCORE search on shardDSS, collecting at
// most k results. Copies each result so the caller owns memory independent of
// the shard's DocumentMatchPool. k=count gives the tightest per-shard WAND threshold.
func runShardSearch(
	ctx context.Context,
	shardDSS *DisjunctionSliceSearcher,
	shared *sharedThreshold,
	k int,
) ([]*search.DocumentMatch, error) {
	searchCtx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(shardDSS.DocumentMatchPoolSize()+k+2, 0),
	}

	var h dmMinHeap

	for {
		// Sync threshold from other goroutines before each Next() call.
		if st := shared.Get(); st > searchCtx.ScoreThreshold {
			searchCtx.ScoreThreshold = st
		}

		m, err := shardDSS.Next(searchCtx)
		if err != nil {
			return nil, err
		}
		if m == nil {
			break
		}

		evicted, minScore := h.pushBounded(m, k)
		if evicted != nil {
			searchCtx.DocumentMatchPool.Put(evicted)
		}
		if minScore > searchCtx.ScoreThreshold {
			searchCtx.ScoreThreshold = minScore
			shared.Update(minScore)
		}
	}

	// Copy results: IndexInternalID is a []byte that points into the pool's
	// backing store. Deep-copy it so the caller's results are self-contained.
	results := make([]*search.DocumentMatch, h.Len())
	for i, dm := range h {
		cp := *dm
		cp.IndexInternalID = append([]byte(nil), dm.IndexInternalID...)
		results[i] = &cp
		searchCtx.DocumentMatchPool.Put(dm)
	}
	sort.Slice(results, func(i, j int) bool { return results[i].Score > results[j].Score })
	return results, nil
}
