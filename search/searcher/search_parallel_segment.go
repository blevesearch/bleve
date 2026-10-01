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
//
// Root-only: the fan-out is served through Next() from a score-ordered cache,
// which cannot honor the parent-driven Advance(ID) protocol that
// ConjunctionSearcher / BooleanSearcher / PhraseSearcher rely on. §7 therefore
// only runs for the request's root searcher, which the caller marks with
// MarkParallelSegmentSearchRoot; every nested DSS takes the serial path.

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/blevesearch/bleve/v2/search"
)

// EnableParallelSegmentSearch activates parallel segment search for
// DisjunctionSliceSearcher. Disabled by default; enable for serial
// latency-focused workloads where per-query goroutine overhead pays off.
// When true, §33 adaptive guards (concurrency gate + DF-based shard guard)
// still apply unless the caller sets ParallelSegmentSearchKey explicitly.
// Atomic so it can be toggled at runtime (e.g. via a manager option) while
// queries are in flight.
var EnableParallelSegmentSearch atomic.Bool

// ParallelSegmentSearchMinSegs is the minimum number of index segments required
// to activate parallel search. Below this the goroutine overhead dominates.
// Compiled default; see ParallelSegmentSearchMinSegsOverride for runtime tuning.
var ParallelSegmentSearchMinSegs = 6

// ParallelSegmentSearchMinSegsOverride, when > 0, replaces
// ParallelSegmentSearchMinSegs at runtime (safe to Store while queries are in
// flight). 0 means "use the compiled default".
var ParallelSegmentSearchMinSegsOverride atomic.Int32

// ParallelSegmentSearchShardK is the minimum (floor) for the per-shard top-K
// collector limit. §35: the actual shardK is max(TopK, floor) where TopK is
// the query's count (SearchRequest.Size+From). This makes the per-shard heap
// fill after TopK docs so the shared WAND threshold rises as fast as it would
// in a serial search, while the floor prevents degenerate heaps for tiny counts
// (e.g. count=1 → shardK=floor so each shard retains enough candidates for a
// correct final merge).
var ParallelSegmentSearchShardK = 10

// ParallelSegmentSearchMaxCount is the maximum query count (top-K limit) for
// which parallel segment search is allowed. When SearchRequest.Size+From exceeds
// this value, shouldRunParallel returns false and the search falls back to the
// serial path. This prevents correctness issues (shardK < count would cause
// shards to discard candidates the final merge needs) and avoids the goroutine
// overhead on large-K queries where WAND pruning is inherently weaker.
// Set to 0 to disable the cap (parallel runs for any count).
var ParallelSegmentSearchMaxCount = 100

// ParallelSegmentSearchMinDFPerSeg is the §33 DF-based shard guard threshold.
// Parallel search is skipped when totalDF/numSegs falls below this value,
// indicating too few candidates per shard to amortize goroutine overhead.
// Tune via benchmark: entity queries have ~0–10 DF/seg; text queries ~100–1000.
// Compiled default; see ParallelSegmentSearchMinDFPerSegOverride for runtime tuning.
var ParallelSegmentSearchMinDFPerSeg uint64 = 150

// ParallelSegmentSearchMinDFPerSegOverride, when > 0, replaces
// ParallelSegmentSearchMinDFPerSeg at runtime (safe to Store while queries are
// in flight). 0 means "use the compiled default", so the effective way to
// disable the DF floor at runtime is to Store(1), not 0.
var ParallelSegmentSearchMinDFPerSegOverride atomic.Int64

// ParallelSegmentSearchMaxConcurrentOverride, when > 0, replaces the computed
// §33 concurrency cap (GOMAXPROCS / p, where p is the shard count the fan-out
// would use) at runtime (safe to Store while queries are in flight). 0 means
// "use the computed default". Like the other adaptive guards it is bypassed
// by an explicit ctx key.
var ParallelSegmentSearchMaxConcurrentOverride atomic.Int32

// ParallelSegmentSearchCounters counts shouldRunParallel outcomes. Exactly one
// counter other than Evaluated is incremented per call. When the feature is
// off (flag off and no ctx key, the default production state) that is
// DeclinedDisabled alone: a single atomic add. Otherwise Evaluated is also
// incremented, so the invariant is
//
//	Evaluated == Ran + DeclinedNested + DeclinedRequest + DeclinedTopK + DeclinedNoCPU +
//	             DeclinedNonTerm + DeclinedSegs + DeclinedDF + DeclinedConcurrency
//
// with DeclinedDisabled counted separately. Plain atomic adds on the hot
// path: no allocations, no locks.
type ParallelSegmentSearchCounters struct {
	// Evaluated counts evaluations with the feature enabled (flag or ctx key).
	Evaluated atomic.Uint64
	// Ran counts decisions to fan out: it is incremented before
	// runParallelSegmentSearch executes, so a fan-out that then fails (and
	// surfaces an error from Next) still counts here.
	Ran                 atomic.Uint64
	DeclinedDisabled    atomic.Uint64 // ctx key <= 0, or global flag off (Evaluated not incremented)
	DeclinedNested      atomic.Uint64 // DSS is not the request's root searcher
	DeclinedRequest     atomic.Uint64 // root, but the request shape is ineligible (non-score sort, facets, search_after/before, knn)
	DeclinedTopK        atomic.Uint64 // TopK > ParallelSegmentSearchMaxCount
	DeclinedNoCPU       atomic.Uint64 // GOMAXPROCS < 2
	DeclinedNonTerm     atomic.Uint64 // no searchers, or a child that is not a *TermSearcher with a term
	DeclinedSegs        atomic.Uint64 // numSegs < effective min segs
	DeclinedDF          atomic.Uint64 // totalDF < numSegs * effective min DF/seg
	DeclinedConcurrency atomic.Uint64 // concurrency gate at capacity
}

// ParallelSegmentSearchStats is the process-wide instance of
// ParallelSegmentSearchCounters, exported for stats/metrics plumbing.
var ParallelSegmentSearchStats ParallelSegmentSearchCounters

// ParallelSegmentSearchEventHook, when non-nil, is called once per
// shouldRunParallel outcome other than "disabled" (which would be far too
// noisy with the feature off). event is one of: "ran", "declined_nested",
// "declined_request", "declined_topk", "declined_nocpu", "declined_nonterm", "declined_segs",
// "declined_df", "declined_concurrency". "ran" means "decided to fan out":
// it fires before runParallelSegmentSearch executes, so a fan-out that then
// fails still reports "ran". numSegs and totalDF are the
// best-known values at that point and may be 0 when not yet computed; topK
// is the query's SearcherOptions.TopK.
//
// The hook runs synchronously on the query path: it must be cheap and
// non-blocking (no I/O, no locks that can contend).
var ParallelSegmentSearchEventHook func(event string, numSegs int, totalDF uint64, topK int)

// MarkParallelSegmentSearchRoot marks s as the request's root searcher,
// making it eligible for §7 parallel segment search. Returns true when s is a
// *DisjunctionSliceSearcher and was marked, false otherwise (any other
// searcher type is left untouched).
//
// §7 only fans out for the root searcher: it answers Next() from a
// score-ordered cache of the merged shard results, which cannot serve the
// parent-driven Advance(ID) protocol that conjunction/boolean/phrase parents
// use to align their children. Callers (the request executor) must mark
// exactly the searcher they will drain with Next() and nothing beneath it.
func MarkParallelSegmentSearchRoot(s search.Searcher) bool {
	dss, ok := s.(*DisjunctionSliceSearcher)
	if !ok {
		return false
	}
	dss.parallelRoot = true
	return true
}

// MarkParallelSegmentSearchIneligible marks s as the request's root searcher
// whose request shape cannot be served by the fan-out (each shard keeps only
// its top-K by score, so a non-score sort, facets, search_after/before or KNN
// hybrid ranking would see a truncated candidate set). Such a DSS declines
// with "declined_request" instead of "declined_nested", so the counters tell
// the two apart. Returns false for any other searcher type.
func MarkParallelSegmentSearchIneligible(s search.Searcher) bool {
	dss, ok := s.(*DisjunctionSliceSearcher)
	if !ok {
		return false
	}
	dss.parallelRoot = true
	dss.parallelIneligible = true
	return true
}

// declineParallel records a non-"disabled" decline: bumps counter, fires the
// event hook, and returns the (false, 0) pair shouldRunParallel hands back.
func declineParallel(counter *atomic.Uint64, event string, numSegs int, totalDF uint64, topK int) (bool, int) {
	counter.Add(1)
	if hook := ParallelSegmentSearchEventHook; hook != nil {
		hook(event, numSegs, totalDF, topK)
	}
	return false, 0
}

// parallelSearchesActive is the §33 concurrency gate counter. It tracks how
// many parallel segment searches are currently running across all goroutines.
// Approximate: the Load→Add sequence is not atomic, so a 1–2 over-count is
// possible at high QPS. This is intentional — we want soft bounding, not a
// mutex on the hot path.
var parallelSearchesActive atomic.Int32

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

// estimateDF estimates the effective candidate count for the DF guard.
// All sub-searchers must already be verified as *TermSearcher before calling.
//
// For plain disjunctions (min ≤ 1), sum of all term DFs is a correct
// conservative upper bound (union ≤ sum).
//
// For MSM queries (min > 1), the raw sum massively over-estimates because a
// matching doc must appear in at least min distinct postings. Under an
// independence model the expected count is:
//
//	E ≈ C(N, min) × (avgDF / N_docs)^(min-1) × avgDF
//
// This shrinks rapidly with min and accurately reflects the real candidate
// density, so the DF guard correctly rejects parallel for sparse MSM queries
// where goroutine overhead would dominate over any parallel speedup.
func estimateDF(s *DisjunctionSliceSearcher) uint64 {
	n := len(s.searchers)
	var total uint64
	for _, sr := range s.searchers {
		total += uint64(sr.(*TermSearcher).Count())
	}

	if s.min <= 1 {
		return total
	}

	// MSM path: apply the independence-model correction.
	nDocs, err := s.indexReader.DocCount()
	if err != nil || nDocs == 0 {
		return total // fall back to the plain-disjunction bound
	}

	avgD := float64(total) / float64(n)
	p := avgD / float64(nDocs)
	est := msmBinomCoeff(n, s.min) * math.Pow(p, float64(s.min-1)) * avgD
	if est < 1 {
		return 1
	}
	if uint64(est) > total {
		return total
	}
	return uint64(est)
}

// msmBinomCoeff returns C(n, k) as float64. Only used for MSM term counts
// (small n and k), so there is no overflow risk within float64 precision.
func msmBinomCoeff(n, k int) float64 {
	if k > n || k < 0 {
		return 0
	}
	if k > n-k {
		k = n - k
	}
	b := 1.0
	for i := 0; i < k; i++ {
		b *= float64(n-i) / float64(i+1)
	}
	return b
}

// shouldRunParallel returns (true, shardK) when all conditions for parallel
// segment search are met. The ctx value for ParallelSegmentSearchKey overrides
// the global EnableParallelSegmentSearch and ParallelSegmentSearchShardK:
// 0 disables, ≥2 enables with that shardK; absent means use global flags.
// shardK is only meaningful when the bool return is true.
//
// The root-only guard (s.parallelRoot, see MarkParallelSegmentSearchRoot)
// applies unconditionally: the ctx key bypasses only the adaptive guards.
//
// When no explicit override is set, two §33 adaptive guards apply:
//   - DF-based shard guard: skip if totalDF/numSegs < ParallelSegmentSearchMinDFPerSeg
//     (prevents goroutine overhead from dominating on low-DF entity queries)
//   - Concurrency gate: skip if too many parallel searches are already active
//     (prevents goroutine oversubscription at high QPS)
//
// Every call increments exactly one outcome counter in
// ParallelSegmentSearchStats; when the feature is enabled (flag or ctx key)
// it also increments Evaluated, and fires ParallelSegmentSearchEventHook for
// every outcome except "disabled". The disabled path costs a single atomic
// add so the default production state stays cheap.
func shouldRunParallel(s *DisjunctionSliceSearcher, sctx *search.SearchContext) (bool, int) {
	// §35: dynamic shardK = max(query.count, floor). Heap fills after TopK docs
	// → shared threshold rises to the TopK-th best score → §34 global WAND
	// ceilings prune as aggressively as the serial path. Floor prevents
	// degenerate heaps for very small counts.
	topK := s.options.TopK
	shardK := ParallelSegmentSearchShardK // floor
	if topK > shardK {
		shardK = topK
	}
	explicitOverride := false

	if v, ok := s.ctx.Value(search.ParallelSegmentSearchKey).(int); ok {
		if v <= 0 {
			ParallelSegmentSearchStats.DeclinedDisabled.Add(1)
			return false, 0
		}
		shardK = v
		explicitOverride = true
	} else if !EnableParallelSegmentSearch.Load() {
		ParallelSegmentSearchStats.DeclinedDisabled.Add(1)
		return false, 0
	}
	ParallelSegmentSearchStats.Evaluated.Add(1)

	// Root-only guard: never bypassed by the ctx key. A nested DSS would be
	// driven by its parent's Advance(ID), which the score-ordered parallel
	// cache cannot serve. This also stops the shard DSSes created inside
	// runParallelSegmentSearch (unmarked, same ctx) from recursing.
	if !s.parallelRoot {
		return declineParallel(&ParallelSegmentSearchStats.DeclinedNested, "declined_nested", 0, 0, topK)
	}
	// Request-level ineligibility (see MarkParallelSegmentSearchIneligible) is
	// a correctness condition, so the explicit ctx override does not bypass it.
	if s.parallelIneligible {
		return declineParallel(&ParallelSegmentSearchStats.DeclinedRequest, "declined_request", 0, 0, topK)
	}

	// §35 count cap: for large-K queries WAND pruning is weaker and goroutine
	// overhead dominates; fall back to serial. Explicit override bypasses this
	// so BENCH_PARALLEL_SEARCH=N can still force parallel for testing.
	if !explicitOverride && ParallelSegmentSearchMaxCount > 0 &&
		topK > ParallelSegmentSearchMaxCount {
		return declineParallel(&ParallelSegmentSearchStats.DeclinedTopK, "declined_topk", 0, 0, topK)
	}

	if runtime.GOMAXPROCS(0) < 2 {
		return declineParallel(&ParallelSegmentSearchStats.DeclinedNoCPU, "declined_nocpu", 0, 0, topK)
	}
	if len(s.searchers) == 0 {
		return declineParallel(&ParallelSegmentSearchStats.DeclinedNonTerm, "declined_nonterm", 0, 0, topK)
	}
	// All sub-searchers must be *TermSearcher with a stored term (set by
	// newTermSearcherFromReader; nil for synonym/unadorned paths).
	for _, sr := range s.searchers {
		ts, ok := sr.(*TermSearcher)
		if !ok || ts.term == nil {
			return declineParallel(&ParallelSegmentSearchStats.DeclinedNonTerm, "declined_nonterm", 0, 0, topK)
		}
	}
	// Enough segments to justify goroutine overhead.
	numSegs := s.searchers[0].(*TermSearcher).NumSegments()
	minSegs := ParallelSegmentSearchMinSegs
	if v := ParallelSegmentSearchMinSegsOverride.Load(); v > 0 {
		minSegs = int(v)
	}
	if numSegs < minSegs {
		return declineParallel(&ParallelSegmentSearchStats.DeclinedSegs, "declined_segs", numSegs, 0, topK)
	}

	var totalDF uint64
	if !explicitOverride {
		// §33 DF-based shard guard: skip when candidates are too sparse to
		// amortize goroutine setup cost. Checked before the atomic load.
		totalDF = estimateDF(s)
		minDFPerSeg := ParallelSegmentSearchMinDFPerSeg
		if v := ParallelSegmentSearchMinDFPerSegOverride.Load(); v > 0 {
			minDFPerSeg = uint64(v)
		}
		if totalDF < uint64(numSegs)*minDFPerSeg {
			return declineParallel(&ParallelSegmentSearchStats.DeclinedDF, "declined_df", numSegs, totalDF, topK)
		}

		// §33 concurrency gate: prevent oversubscription at high QPS.
		// Compute the p that runParallelSegmentSearch would use, then allow at
		// most GOMAXPROCS/p concurrent parallel searches.
		gmp := runtime.GOMAXPROCS(0)
		p := gmp
		if p > numSegs {
			p = numSegs
		}
		if p > 8 {
			p = 8
		}
		maxConcurrent := int32(gmp / p)
		if maxConcurrent < 1 {
			maxConcurrent = 1
		}
		if v := ParallelSegmentSearchMaxConcurrentOverride.Load(); v > 0 {
			maxConcurrent = v
		}
		if parallelSearchesActive.Load() >= maxConcurrent {
			return declineParallel(&ParallelSegmentSearchStats.DeclinedConcurrency, "declined_concurrency", numSegs, totalDF, topK)
		}
	}
	ParallelSegmentSearchStats.Ran.Add(1)
	if hook := ParallelSegmentSearchEventHook; hook != nil {
		hook("ran", numSegs, totalDF, topK)
	}
	return true, shardK
}

// runParallelSegmentSearch fans the search across P goroutines, each handling
// a contiguous range of segments. Returns all collected results merged and
// sorted by score descending. shardK is the per-shard top-K collector limit.
//
// The bool result is true when the merged results are a lower bound on the
// true match set: either a shard's MAXSCORE path pruned candidates (WAND), or
// a shard's top-shardK heap evicted one. The caller maps it onto
// SearchContext.WANDPruned so SearchResult.Total is reported with the right
// TotalRelation.
func runParallelSegmentSearch(
	ctx context.Context,
	s *DisjunctionSliceSearcher,
	shardK int,
	requestWAND bool,
) ([]*search.DocumentMatch, bool, error) {
	parallelSearchesActive.Add(1)
	defer parallelSearchesActive.Add(-1)

	numSegs := s.searchers[0].(*TermSearcher).NumSegments()
	p := runtime.GOMAXPROCS(0)
	if p > numSegs {
		p = numSegs
	}
	if p > 8 {
		p = 8
	}
	segsPerShard := (numSegs + p - 1) / p

	// §34: pre-compute global per-term MaxImpact from the original full-index
	// TermSearchers (all segments). Shard TFRs cover only 2/15 segments so their
	// MaxImpact() is lower, making MAXSCORE partitioning ineffective against a
	// cross-shard threshold that the highest-scoring shard broadcast. Global
	// ceilings are a correct upper bound on any shard doc's score and keep the
	// essential/non-essential partition as tight as the serial WAND path.
	//
	// Only enable WAND when the request allows it (requestWAND mirrors
	// SearchContext.WANDEnabled which is false for ScoreModeComplete).
	// With ScoreModeComplete the caller wants exact scores; skip the MaxImpact
	// reads and globalMI allocation entirely.
	var globalMI []float64
	canWAND := false
	if requestWAND {
		globalMI = make([]float64, len(s.searchers))
		canWAND = true
		for i, sr := range s.searchers {
			mi := sr.(*TermSearcher).MaxImpact()
			if mi >= math.MaxFloat64 {
				canWAND = false
				break
			}
			globalMI[i] = mi
		}
	}

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
			return nil, false, createErr
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
			return nil, false, err
		}
		// Shards never fan out: skip shouldRunParallel entirely so they do
		// not recurse or pollute ParallelSegmentSearchStats (Evaluated /
		// DeclinedNested) on every root run.
		dss.parallelDecided = true
		if canWAND {
			dss.injectGlobalWANDCeilings(globalMI)
		}
		shards = append(shards, shardDSS{dss: dss})
	}

	type shardResult struct {
		matches    []*search.DocumentMatch
		wandPruned bool // MAXSCORE skipped candidates
		truncated  bool // top-shardK heap evicted at least one candidate
		err        error
	}
	results := make([]shardResult, len(shards))
	var shared sharedThreshold

	var wg sync.WaitGroup
	for g := range shards {
		wg.Add(1)
		go func(g int, dss *DisjunctionSliceSearcher) {
			defer wg.Done()
			// A panic on this goroutine would bypass every recover() up the
			// caller's stack (net/http's included) and kill the process —
			// convert it to a per-shard error instead.
			defer func() {
				if r := recover(); r != nil {
					results[g] = shardResult{err: fmt.Errorf(
						"parallel segment search: shard %d panicked: %v\n%s",
						g, r, debug.Stack())}
				}
			}()
			defer func() { _ = dss.Close() }()
			matches, wandPruned, truncated, err := runShardSearch(ctx, dss, &shared, shardK, canWAND)
			results[g] = shardResult{matches: matches, wandPruned: wandPruned, truncated: truncated, err: err}
		}(g, shards[g].dss)
	}
	wg.Wait()

	var total int
	var pruned bool
	for _, r := range results {
		if r.err != nil {
			return nil, false, r.err
		}
		total += len(r.matches)
		pruned = pruned || r.wandPruned || r.truncated
	}
	all := make([]*search.DocumentMatch, 0, total)
	for _, r := range results {
		all = append(all, r.matches...)
	}
	// Score descending, ties broken by docID ascending (the order the serial
	// path emits them), so the merged order is deterministic across runs.
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].Score != all[j].Score {
			return all[i].Score > all[j].Score
		}
		return all[i].IndexInternalID.Compare(all[j].IndexInternalID) < 0
	})
	return all, pruned, nil
}

// runShardSearch runs a full WAND/MAXSCORE search on shardDSS, collecting at
// most k results. Copies each result so the caller owns memory independent of
// the shard's DocumentMatchPool. k=count gives the tightest per-shard WAND threshold.
// wandEnabled mirrors the caller's canWAND flag: when true the shard SearchContext
// has WANDEnabled=true so the MAXSCORE path activates using the injected global ceilings.
//
// Returns (matches, wandPruned, truncated, err): wandPruned is the shard's
// SearchContext.WANDPruned; truncated is true when the top-k heap evicted at
// least one candidate, i.e. the shard matched more than k docs.
func runShardSearch(
	ctx context.Context,
	shardDSS *DisjunctionSliceSearcher,
	shared *sharedThreshold,
	k int,
	wandEnabled bool,
) ([]*search.DocumentMatch, bool, bool, error) {
	searchCtx := &search.SearchContext{
		DocumentMatchPool: search.NewDocumentMatchPool(shardDSS.DocumentMatchPoolSize()+k+2, 0),
		WANDEnabled:       wandEnabled,
	}

	var h dmMinHeap
	var truncated bool

	var iters uint64
	for {
		// Honor query cancellation/timeouts: without this a cancelled query's
		// shards run to completion, holding cores after the client is gone.
		if iters%1024 == 0 {
			select {
			case <-ctx.Done():
				for _, dm := range h {
					searchCtx.DocumentMatchPool.Put(dm)
				}
				return nil, false, false, ctx.Err()
			default:
			}
		}
		iters++

		// Sync threshold from other goroutines before each Next() call.
		if st := shared.Get(); st > searchCtx.ScoreThreshold {
			searchCtx.ScoreThreshold = st
		}

		m, err := shardDSS.Next(searchCtx)
		if err != nil {
			return nil, false, false, err
		}
		if m == nil {
			break
		}

		evicted, minScore := h.pushBounded(m, k)
		if evicted != nil {
			truncated = true
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
	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Score != results[j].Score {
			return results[i].Score > results[j].Score
		}
		return results[i].IndexInternalID.Compare(results[j].IndexInternalID) < 0
	})
	return results, searchCtx.WANDPruned, truncated, nil
}
