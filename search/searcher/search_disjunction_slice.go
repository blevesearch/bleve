//  Copyright (c) 2018 Couchbase, Inc.
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
	"encoding/binary"
	"math"
	"reflect"
	"sort"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/scorer"
	"github.com/blevesearch/bleve/v2/size"
	index "github.com/blevesearch/bleve_index_api"
)

var reflectStaticSizeDisjunctionSliceSearcher int

func init() {
	var ds DisjunctionSliceSearcher
	reflectStaticSizeDisjunctionSliceSearcher = int(reflect.TypeOf(ds).Size())
}

type DisjunctionSliceSearcher struct {
	indexReader            index.IndexReader
	searchers              []search.Searcher
	originalPos            []int
	numSearchers           int
	queryNorm              float64
	retrieveScoreBreakdown bool
	// lazyMode is true when all sub-searchers support the §9 lazy BM25 path
	// (i.e. len(lazySearchers) == numSearchers). Stored here (offset 81, cache
	// line 1) rather than computed from len(lazySearchers) (offset 360, cache
	// line 5, cold) to avoid the cold-line load on every nextMAXSCORE call.
	// One bool fits in the 7-byte padding gap between retrieveScoreBreakdown
	// and currs — struct size stays 384 bytes.
	lazyMode bool
	currs    []*search.DocumentMatch
	scorer                 *scorer.DisjunctionQueryScorer
	min                    int
	matching               []*search.DocumentMatch
	matchingIdxs           []int
	initialized            bool
	bytesRead              uint64

	// wandMaxImpacts holds the per-sub-searcher MaxImpact() value, computed
	// once in initWANDMaxImpacts() and reused for every candidate.
	//
	// Without this cache wandAboveThreshold paid a Go type-assertion plus an
	// interface dispatch per matching term per candidate (~7 ns each), even
	// though MaxImpact() is constant for the lifetime of a query.
	//
	// Nil = not yet initialised.  Non-nil but zero-length
	// (maxImpactFallback) = WAND cannot be applied for this query
	// (non-BM25 scorer, or at least one term returned math.MaxFloat64).
	//
	// See zapx/inverted_text_cache.go for the full cache-hierarchy diagram.
	wandMaxImpacts []float64

	// maxscoreOrder is an argsort of wandMaxImpacts ascending (lowest MaxImpact
	// first).  maxscoreOrder[i] is an index into s.searchers / s.currs /
	// s.wandMaxImpacts.
	//
	// Non-nil only when wandMaxImpacts is also non-nil and non-empty.
	// Populated by initWANDMaxImpacts alongside wandMaxImpacts.
	maxscoreOrder []int

	// pivotIdx is the first index in maxscoreOrder whose suffix-sum of
	// MaxImpact values exceeds ctx.ScoreThreshold.  Searchers at indices
	// maxscoreOrder[pivotIdx:] are *essential* (drive candidate generation);
	// those at maxscoreOrder[:pivotIdx] are *non-essential* (only checked for
	// bonus score contribution).
	//
	//   pivotIdx == 0               → all terms essential; falls back to WAND
	//   pivotIdx == len(searchers)  → no terms essential; nothing can beat
	//                                  threshold; return nil immediately
	//
	// Recomputed by computeMAXSCOREPivot whenever ctx.ScoreThreshold changes.
	pivotIdx      int
	lastThreshold float64 // threshold value when pivotIdx was last computed

	// minIDBuf holds the minimum essential docID for the current MAXSCORE
	// iteration.  Kept as a struct field (not a local variable) so that
	// minID = minIDBuf[:8] does NOT escape to the heap — a local [8]byte
	// that's sliced and passed to an interface method triggers escape analysis
	// and allocates once per candidate.
	minIDBuf [8]byte

	// segSkippers is non-nil when all sub-searchers support per-segment
	// operations (§15: per-segment score ceiling).  Indexed by position in
	// s.searchers (same indexing as wandMaxImpacts).
	segSkippers []segmentSkipper

	// segCeilings[i] is the sum of per-term max BM25 impacts for segment i.
	// Any document in segment i scores at most segCeilings[i].  When
	// segCeilings[i] ≤ ctx.ScoreThreshold the entire segment can be skipped.
	// Computed once in initWANDMaxImpacts() alongside wandMaxImpacts.
	segCeilings []float64

	// minSegCeiling is min(segCeilings).  §15 can only skip a segment when
	// ctx.ScoreThreshold ≥ minSegCeiling; when the threshold is below this
	// value the SegmentIndexOf call (a sort.Search) is skipped entirely.
	minSegCeiling float64

	// segSkipBuf is reused storage for FirstDocIDOfSegment calls (actual skips).
	segSkipBuf [8]byte

	// §15 segment boundary cache: avoids calling SegmentIndexOf (sort.Search)
	// on every candidate.  When minIDVal is within [cachedSegStart, cachedSegEnd)
	// the cached segIdx is reused directly.  Invalidated (cachedSegEnd = 0) when
	// §15 is initialized so the first call always refreshes the cache.
	cachedSegIdx   int
	cachedSegStart uint64
	cachedSegEnd   uint64 // exclusive; math.MaxUint64 for the last segment
	segCacheBuf    [8]byte

	// lazySearchers holds concrete *TermSearcher pointers for the §9 lazy BM25
	// path. Using the concrete type instead of the lazyTermSearcher interface
	// eliminates vtable dispatch for nextDocIDOnly/advanceDocIDOnly (cost 206/207,
	// non-inlinable) and allows scoreCurrentDoc (cost 64) to be inlined by the
	// caller. Non-nil only when all sub-searchers are *TermSearcher.
	lazySearchers []*TermSearcher

	// §7 parallel segment search. options and ctx are stored so that shard
	// sub-searchers can be created in runParallelSegmentSearch. parallelResults
	// is set on the first Next() call when parallel mode is active; subsequent
	// calls drain it in score-descending order.
	options        search.SearcherOptions
	ctx            context.Context
	parallelResults []*search.DocumentMatch
	parallelPos     int
}

// maxImpactFallback is a non-nil zero-length sentinel stored in
// wandMaxImpacts when WAND cannot be applied for the current query.
var maxImpactFallback = make([]float64, 0)

func newDisjunctionSliceSearcher(ctx context.Context, indexReader index.IndexReader,
	qsearchers []search.Searcher, min float64, options search.SearcherOptions,
	limit bool) (
	*DisjunctionSliceSearcher, error,
) {
	if limit && tooManyClauses(len(qsearchers)) {
		return nil, tooManyClausesErr("", len(qsearchers))
	}

	var searchers OrderedSearcherList
	var originalPos []int
	var retrieveScoreBreakdown bool
	if ctx != nil {
		retrieveScoreBreakdown, _ = ctx.Value(search.IncludeScoreBreakdownKey).(bool)
	}

	if retrieveScoreBreakdown {
		// needed only when kNN is in picture
		sortedSearchers := &OrderedPositionalSearcherList{
			searchers: make([]search.Searcher, len(qsearchers)),
			index:     make([]int, len(qsearchers)),
		}
		for i, searcher := range qsearchers {
			sortedSearchers.searchers[i] = searcher
			sortedSearchers.index[i] = i
		}
		sort.Sort(sortedSearchers)
		searchers = sortedSearchers.searchers
		originalPos = sortedSearchers.index
	} else {
		searchers = make(OrderedSearcherList, len(qsearchers))
		copy(searchers, qsearchers)
		sort.Sort(searchers)
	}

	rv := DisjunctionSliceSearcher{
		indexReader:            indexReader,
		searchers:              searchers,
		originalPos:            originalPos,
		numSearchers:           len(searchers),
		currs:                  make([]*search.DocumentMatch, len(searchers)),
		scorer:                 scorer.NewDisjunctionQueryScorer(options),
		min:                    int(min),
		retrieveScoreBreakdown: retrieveScoreBreakdown,

		matching:      make([]*search.DocumentMatch, len(searchers)),
		matchingIdxs:  make([]int, len(searchers)),
		lazySearchers: make([]*TermSearcher, len(searchers)),
		options:       options,
		ctx:           ctx,
	}
	rv.computeQueryNorm()
	return &rv, nil
}

func (s *DisjunctionSliceSearcher) computeQueryNorm() {
	// first calculate sum of squared weights
	sumOfSquaredWeights := 0.0
	for _, searcher := range s.searchers {
		sumOfSquaredWeights += searcher.Weight()
	}
	// now compute query norm from this
	s.queryNorm = 1.0 / math.Sqrt(sumOfSquaredWeights)
	// finally tell all the downstream searchers the norm
	for _, searcher := range s.searchers {
		searcher.SetQueryNorm(s.queryNorm)
	}
}

func (s *DisjunctionSliceSearcher) Size() int {
	sizeInBytes := reflectStaticSizeDisjunctionSliceSearcher + size.SizeOfPtr +
		s.scorer.Size()

	for _, entry := range s.searchers {
		sizeInBytes += entry.Size()
	}

	for _, entry := range s.currs {
		if entry != nil {
			sizeInBytes += entry.Size()
		}
	}

	for _, entry := range s.matching {
		if entry != nil {
			sizeInBytes += entry.Size()
		}
	}

	sizeInBytes += len(s.matchingIdxs) * size.SizeOfInt
	sizeInBytes += len(s.originalPos) * size.SizeOfInt

	return sizeInBytes
}

func (s *DisjunctionSliceSearcher) initSearchers(ctx *search.SearchContext) error {
	var err error
	// get all searchers pointing at their first match
	for i, searcher := range s.searchers {
		if s.currs[i] != nil {
			ctx.DocumentMatchPool.Put(s.currs[i])
		}
		s.currs[i], err = searcher.Next(ctx)
		if err != nil {
			return err
		}
	}

	err = s.updateMatches()
	if err != nil {
		return err
	}

	s.initialized = true
	return nil
}

func (s *DisjunctionSliceSearcher) updateMatches() error {
	matching := s.matching[:0]
	matchingIdxs := s.matchingIdxs[:0]

	for i := 0; i < len(s.currs); i++ {
		curr := s.currs[i]
		if curr == nil {
			continue
		}

		if len(matching) > 0 {
			cmp := curr.IndexInternalID.Compare(matching[0].IndexInternalID)
			if cmp > 0 {
				continue
			}

			if cmp < 0 {
				matching = matching[:0]
				matchingIdxs = matchingIdxs[:0]
			}
		}
		matching = append(matching, curr)
		matchingIdxs = append(matchingIdxs, i)
	}

	s.matching = matching
	s.matchingIdxs = matchingIdxs

	return nil
}

// wandImpacter is the optional interface implemented by TermSearcher.
type wandImpacter interface {
	MaxImpact() float64
}

// lazyTermSearcher is implemented by TermSearcher for deferred BM25 scoring in
// the MAXSCORE hot path (§9): the disjunction searcher pre-fetches docIDs
// cheaply and calls scoreCurrentDoc only for candidates that survive the WAND
// threshold check, skipping BM25 computation for all pruned candidates.
type lazyTermSearcher interface {
	nextDocIDOnly(ctx *search.SearchContext) (*search.DocumentMatch, error)
	advanceDocIDOnly(ctx *search.SearchContext, ID index.IndexInternalID) (*search.DocumentMatch, error)
	scoreCurrentDoc(rv *search.DocumentMatch)
}

// segmentSkipper is the optional interface implemented by TermSearcher when
// its underlying TFR supports per-segment operations (§15).
type segmentSkipper interface {
	NumSegments() int
	MaxImpactForSegment(segIdx int) float64
	SegmentIndexOf(id index.IndexInternalID) int
	FirstDocIDOfSegment(segIdx int, buf []byte) index.IndexInternalID
}

// initWANDMaxImpacts pre-computes each searcher's MaxImpact() into the
// wandMaxImpacts slice so the per-candidate hot path only does array reads
// and float additions with no interface dispatch or type assertions.
// Also builds maxscoreOrder (argsort of wandMaxImpacts ascending) for MAXSCORE.
// Sets wandMaxImpacts to maxImpactFallback if WAND cannot be applied.
func (s *DisjunctionSliceSearcher) initWANDMaxImpacts() {
	mi := make([]float64, len(s.searchers))
	for i, searcher := range s.searchers {
		wi, ok := searcher.(wandImpacter)
		if !ok {
			s.wandMaxImpacts = maxImpactFallback
			return
		}
		v := wi.MaxImpact()
		if v >= math.MaxFloat64 {
			s.wandMaxImpacts = maxImpactFallback
			return
		}
		mi[i] = v
	}
	s.wandMaxImpacts = mi

	// Build argsort for MAXSCORE: indices sorted by MaxImpact ascending.
	order := make([]int, len(s.searchers))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool {
		return mi[order[a]] < mi[order[b]]
	})
	s.maxscoreOrder = order
	s.pivotIdx = 0
	s.lastThreshold = 0

	// Build per-segment ceiling array (§15) if all searchers support it.
	skippers := make([]segmentSkipper, len(s.searchers))
	allSupport := true
	for i, searcher := range s.searchers {
		sk, ok := searcher.(segmentSkipper)
		if !ok || sk.NumSegments() == 0 {
			allSupport = false
			break
		}
		skippers[i] = sk
	}
	if allSupport {
		numSegs := skippers[0].NumSegments()
		ceilings := make([]float64, numSegs)
		for segIdx := range ceilings {
			for _, sk := range skippers {
				ceilings[segIdx] += sk.MaxImpactForSegment(segIdx)
			}
		}
		minCeil := math.MaxFloat64
		for _, c := range ceilings {
			if c < minCeil {
				minCeil = c
			}
		}
		s.segSkippers = skippers
		s.segCeilings = ceilings
		s.minSegCeiling = minCeil
		s.cachedSegEnd = 0 // invalidate §15 segment cache; force refresh on first use
	}

	// §9: Populate lazySearchers if all sub-searchers are *TermSearcher.
	// Using the concrete type eliminates vtable dispatch for nextDocIDOnly /
	// advanceDocIDOnly (non-inlinable, cost 206/207) and allows scoreCurrentDoc
	// (cost 64, inlinable) to be folded into the caller.
	// The slice was pre-allocated in newDisjunctionSliceSearcher.
	for i, searcher := range s.searchers {
		ts, ok := searcher.(*TermSearcher)
		if !ok {
			s.lazySearchers = s.lazySearchers[:0] // signal: lazy path unavailable
			s.lazyMode = false
			return
		}
		s.lazySearchers[i] = ts
	}
	// All searchers are *TermSearcher; lazySearchers is fully populated.
	s.lazyMode = true
}

// computeMAXSCOREPivot sets pivotIdx to the smallest index in maxscoreOrder
// such that the suffix sum of MaxImpact values from pivotIdx onward exceeds
// threshold.  If even the sum of all terms doesn't exceed threshold, sets
// pivotIdx = len(maxscoreOrder) (signal to stop iterating).
func (s *DisjunctionSliceSearcher) computeMAXSCOREPivot(threshold float64) {
	s.lastThreshold = threshold
	n := len(s.maxscoreOrder)
	var sum float64
	for i := n - 1; i >= 0; i-- {
		sum += s.wandMaxImpacts[s.maxscoreOrder[i]]
		if sum > threshold {
			s.pivotIdx = i
			return
		}
	}
	s.pivotIdx = n
}

// wandAboveThreshold returns true if the current candidate should be scored.
// Returns false when ctx.ScoreThreshold > 0 AND the sum of per-term
// MaxImpact values for the matching terms is ≤ the threshold — the candidate
// cannot improve the top-k heap regardless of its actual score.
// Returns true whenever the bound cannot be computed (non-BM25, etc.).
func (s *DisjunctionSliceSearcher) wandAboveThreshold(ctx *search.SearchContext) bool {
	threshold := ctx.ScoreThreshold
	if threshold <= 0 {
		return true
	}
	if s.wandMaxImpacts == nil {
		s.initWANDMaxImpacts()
	}
	mi := s.wandMaxImpacts
	if len(mi) == 0 {
		return true // WAND unavailable for this query
	}
	var upperBound float64
	for _, i := range s.matchingIdxs {
		upperBound += mi[i]
	}
	return upperBound > threshold
}

func (s *DisjunctionSliceSearcher) Weight() float64 {
	var rv float64
	for _, searcher := range s.searchers {
		rv += searcher.Weight()
	}
	return rv
}

func (s *DisjunctionSliceSearcher) SetQueryNorm(qnorm float64) {
	// Invalidate all caches: MaxImpact, MAXSCORE sort order, and per-segment
	// ceilings all depend on queryNorm via scorer.QueryWeight().
	s.wandMaxImpacts = nil
	s.maxscoreOrder = nil
	s.lastThreshold = 0
	s.segSkippers = nil
	s.segCeilings = nil
	s.minSegCeiling = 0
	s.cachedSegEnd = 0
	for _, searcher := range s.searchers {
		searcher.SetQueryNorm(qnorm)
	}
}

func (s *DisjunctionSliceSearcher) Next(ctx *search.SearchContext) (
	*search.DocumentMatch, error,
) {
	// §7 parallel segment search: on the first call, fan out to goroutines and
	// cache all results. Subsequent calls drain the cache in score order.
	if s.parallelResults == nil {
		if ok, shardK := shouldRunParallel(s); ok {
			var err error
			s.parallelResults, err = runParallelSegmentSearch(s.ctx, s, shardK)
			if err != nil {
				return nil, err
			}
			// Ensure non-nil sentinel so the "not yet run" check above stays false.
			if s.parallelResults == nil {
				s.parallelResults = []*search.DocumentMatch{}
			}
		}
	}
	if s.parallelResults != nil {
		if s.parallelPos >= len(s.parallelResults) {
			return nil, nil
		}
		rv := s.parallelResults[s.parallelPos]
		s.parallelPos++
		return rv, nil
	}

	if !s.initialized {
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}

	// MAXSCORE: when we have a score threshold, WAND is available, and the
	// caller has opted into speed optimizations, check whether at least one
	// term is non-essential.  If so, use the MAXSCORE path which skips
	// Next() calls on non-essential iterators entirely.
	if ctx.WANDEnabled && ctx.ScoreThreshold > 0 {
		if s.wandMaxImpacts == nil {
			s.initWANDMaxImpacts()
		}
		if len(s.wandMaxImpacts) > 0 { // WAND available
			if ctx.ScoreThreshold != s.lastThreshold {
				s.computeMAXSCOREPivot(ctx.ScoreThreshold)
			}
			if s.pivotIdx == len(s.maxscoreOrder) {
				// All remaining candidates are WAND-pruned: no doc's MaxImpact
				// sum can exceed the threshold. Total is now a lower bound.
				ctx.WANDPruned = true
				return nil, nil // no doc can beat threshold
			}
			if s.pivotIdx > 0 {
				return s.nextMAXSCORE(ctx)
			}
		}
	}

	return s.nextBasic(ctx)
}

// nextBasic is the original Next() loop: advances all matching iterators on
// every candidate, with a per-candidate WAND upper-bound check.
func (s *DisjunctionSliceSearcher) nextBasic(ctx *search.SearchContext) (
	*search.DocumentMatch, error,
) {
	var err error
	var rv *search.DocumentMatch

	found := false
	for !found && len(s.matching) > 0 {
		if len(s.matching) >= s.min {
			// WAND pruning: skip scoring when upper bound ≤ threshold (opt-in only).
			if ctx.WANDEnabled && !s.wandAboveThreshold(ctx) {
				// discard; advance happens below
				ctx.WANDPruned = true
			} else {
				found = true
				if s.retrieveScoreBreakdown {
					rv = s.scorer.ScoreAndExplBreakdown(ctx, s.matching, s.matchingIdxs, s.originalPos, s.numSearchers)
				} else {
					rv = s.scorer.Score(ctx, s.matching, len(s.matching), s.numSearchers)
				}
			}
		}

		for _, i := range s.matchingIdxs {
			if s.currs[i] != rv {
				ctx.DocumentMatchPool.Put(s.currs[i])
			}
			s.currs[i], err = s.searchers[i].Next(ctx)
			if err != nil {
				return nil, err
			}
		}

		if err = s.updateMatches(); err != nil {
			return nil, err
		}
	}
	return rv, nil
}

// nextMAXSCORE implements the MAXSCORE essential/non-essential partition.
//
// Essential terms (maxscoreOrder[pivotIdx:]) drive candidate generation —
// only their iterators are advanced with Next().  Non-essential terms
// (maxscoreOrder[:pivotIdx]) are only seeked forward via Advance() to check
// whether they also match the current essential-term candidate, contributing
// a bonus to the score.  This eliminates all Next() calls on non-essential
// iterators between candidates, which is the bulk of the speedup for queries
// with stopwords or highly asymmetric term weights.
//
// When s.lazySearchers != nil (§9), BM25 scoring is deferred: pre-fetched
// DocumentMatches carry only the docID (Score=0) and BM25 is computed only
// for candidates that survive the WAND threshold check.
//
// Invariant on entry: pivotIdx > 0 (caller checked).
func (s *DisjunctionSliceSearcher) nextMAXSCORE(ctx *search.SearchContext) (
	*search.DocumentMatch, error,
) {
	var err error
	// minID is a slice into s.minIDBuf (a struct field, already on the heap).
	// Using a local [8]byte would escape to the heap every call because the
	// slice is passed to Advance(), an interface method — see s.minIDBuf doc.
	var minID index.IndexInternalID
	// s.lazyMode is pre-computed in initWANDMaxImpacts: true when all
	// sub-searchers support the §9 lazy BM25 path. Reading it from offset 81
	// (cache line 1, hot) avoids the cold load of len(s.lazySearchers) from
	// offset 360 (cache line 5) that the previous check required.
	lazy := s.lazyMode // hoisted: constant per query
	// wandImpacts and threshold are both constant within a single nextMAXSCORE
	// call (threshold only changes after we return a result to the collector).
	// Hoist them here to avoid re-loading ctx fields and to allow the upper-bound
	// accumulation to be folded into the matching-collection loop below (which
	// eliminates the wandAboveThreshold function call and its second pass over
	// matchingIdxs — wandAboveThreshold exceeds the Go inliner budget).
	wandImpacts := s.wandMaxImpacts // non-nil, len>0 guaranteed by caller
	threshold := ctx.ScoreThreshold

	for {
		// Find the minimum docID among essential iterators.
		// Scorch IDs are always 8-byte big-endian uint64; decode once and use
		// integer comparison throughout the loop to avoid bytes.Compare overhead.
		var minIDVal uint64 = math.MaxUint64
		for _, si := range s.maxscoreOrder[s.pivotIdx:] {
			curr := s.currs[si]
			if curr == nil {
				continue
			}
			if len(curr.IndexInternalID) != 8 {
				// ID was Reset by pool (pool aliasing); treat as exhausted this round.
				continue
			}
			v := binary.BigEndian.Uint64(curr.IndexInternalID)
			if v < minIDVal {
				minIDVal = v
			}
		}
		if minIDVal == math.MaxUint64 {
			return nil, nil // all essential iterators exhausted
		}
		// Encode minIDVal once. PutUint64 is a single instruction (STREV on arm64)
		// and avoids re-loading the source bytes from curr.IndexInternalID.
		binary.BigEndian.PutUint64(s.minIDBuf[:], minIDVal)
		minID = s.minIDBuf[:]

		// §15: Per-segment score ceiling check.
		// If no document in the current segment can beat the threshold, advance
		// all essential iterators to the first document of the next eligible segment.
		// Guard: only call SegmentIndexOf (a sort.Search) when the threshold is high
		// enough that at least one segment could be skipped (threshold ≥ minSegCeiling).
		// Segment cache: SegmentIndexOf is O(log numSegs) per call.  Cache the last
		// result's bounds so we only call it when minIDVal crosses a segment boundary
		// (~15 calls per query instead of once per candidate doc).
		if s.segCeilings != nil && threshold >= s.minSegCeiling {
			segIdx := s.cachedSegIdx
			if minIDVal < s.cachedSegStart || minIDVal >= s.cachedSegEnd {
				segIdx = s.segSkippers[0].SegmentIndexOf(minID)
				s.cachedSegIdx = segIdx
				segStart := s.segSkippers[0].FirstDocIDOfSegment(segIdx, s.segCacheBuf[:])
				if len(segStart) == 8 {
					s.cachedSegStart = binary.BigEndian.Uint64(segStart)
				}
				segEnd := s.segSkippers[0].FirstDocIDOfSegment(segIdx+1, s.segCacheBuf[:])
				if len(segEnd) == 8 {
					s.cachedSegEnd = binary.BigEndian.Uint64(segEnd)
				} else {
					s.cachedSegEnd = math.MaxUint64
				}
			}
			if s.segCeilings[segIdx] <= threshold {
				// Find the first segment whose ceiling exceeds the threshold.
				nextSeg := segIdx + 1
				for nextSeg < len(s.segCeilings) && s.segCeilings[nextSeg] <= threshold {
					nextSeg++
				}
				if nextSeg >= len(s.segCeilings) {
					return nil, nil // all remaining segments are below threshold
				}
				skipTo := s.segSkippers[0].FirstDocIDOfSegment(nextSeg, s.segSkipBuf[:])
				if skipTo == nil {
					return nil, nil
				}
				for _, si := range s.maxscoreOrder[s.pivotIdx:] {
					curr := s.currs[si]
					if curr == nil {
						continue
					}
					if s.segSkippers[si].SegmentIndexOf(curr.IndexInternalID) < nextSeg {
						if lazy {
							ctx.DocumentMatchPool.PutLazy(curr)
							s.currs[si], err = s.lazySearchers[si].advanceDocIDOnly(ctx, skipTo)
						} else {
							ctx.DocumentMatchPool.Put(curr)
							s.currs[si], err = s.searchers[si].Advance(ctx, skipTo)
						}
						if err != nil {
							return nil, err
						}
					}
				}
				continue // re-scan for new minID
			}
		}

		// Advance non-essential iterators to minID so they can contribute
		// bonus score if they happen to match this candidate.
		// In the lazy path, these docs have only IndexInternalID set (never scored),
		// so PutLazy (zeros IndexInternalID+Score) avoids the full Reset overhead.
		for _, si := range s.maxscoreOrder[:s.pivotIdx] {
			curr := s.currs[si]
			if curr != nil && len(curr.IndexInternalID) == 8 && binary.BigEndian.Uint64(curr.IndexInternalID) < minIDVal {
				if lazy {
					ctx.DocumentMatchPool.PutLazy(curr)
					s.currs[si], err = s.lazySearchers[si].advanceDocIDOnly(ctx, minID)
				} else {
					ctx.DocumentMatchPool.Put(curr)
					s.currs[si], err = s.searchers[si].Advance(ctx, minID)
				}
				if err != nil {
					return nil, err
				}
			}
		}

		// Collect all terms (essential and non-essential) that match minID.
		// Accumulate the WAND upper bound in the same pass to avoid a second
		// iteration over matchingIdxs inside wandAboveThreshold (which also
		// can't be inlined — cost 109 > budget 80).
		s.matching = s.matching[:0]
		s.matchingIdxs = s.matchingIdxs[:0]
		var upperBound float64
		for i, curr := range s.currs {
			if curr != nil && len(curr.IndexInternalID) == 8 && binary.BigEndian.Uint64(curr.IndexInternalID) == minIDVal {
				s.matching = append(s.matching, curr)
				s.matchingIdxs = append(s.matchingIdxs, i)
				upperBound += wandImpacts[i]
			}
		}

		// Score if we have enough matching terms and the upper bound clears the threshold.
		// threshold > 0 and len(wandImpacts) > 0 are guaranteed by the caller.
		var rv *search.DocumentMatch
		if len(s.matching) >= s.min {
			if upperBound > threshold {
				if lazy {
					// §9: BM25 deferred — score only candidates that survive WAND.
					for _, si := range s.matchingIdxs {
						s.lazySearchers[si].scoreCurrentDoc(s.currs[si])
					}
				}
				if s.retrieveScoreBreakdown {
					rv = s.scorer.ScoreAndExplBreakdown(ctx, s.matching, s.matchingIdxs, s.originalPos, s.numSearchers)
				} else if lazy {
					// ScoreImpact is inlinable (cost 37 < 80): skips MergeFieldTermLocations
					// and the explain branch (neither ever fires in the lazy BM25 path —
					// scoreCurrentDoc only sets Score, never FieldTermLocations or Expl).
					rv = s.scorer.ScoreImpact(s.matching, len(s.matching), s.numSearchers)
				} else {
					rv = s.scorer.Score(ctx, s.matching, len(s.matching), s.numSearchers)
				}
			} else {
				ctx.WANDPruned = true
			}
		}

		// Advance ALL iterators (essential and non-essential) that are at minID.
		//
		// Essential iterators at minID are always advanced so the next iteration
		// picks up fresh candidates beyond minID.
		//
		// Non-essential iterators at minID MUST also be advanced here — not lazily
		// in the next iteration.  The lazy path would call ctx.DocumentMatchPool.Put
		// on a DocumentMatch that is already in the collector's top-k heap (since rv
		// = constituents[0] = s.currs[si_ne] for the non-essential that matched).
		// Put calls dm.Reset() which sets IndexInternalID = IndexInternalID[:0],
		// zeroing the len field and corrupting the heap entry.
		//
		// In the lazy path, non-rv matched docs have at most IndexInternalID + Score
		// set (Score only when scoreCurrentDoc was called, i.e. rv != nil).
		// PutLazy (zeros both fields) is sufficient and avoids the full Reset.
		for i, curr := range s.currs {
			if curr != nil && len(curr.IndexInternalID) == 8 && binary.BigEndian.Uint64(curr.IndexInternalID) == minIDVal {
				if curr != rv {
					if lazy {
						ctx.DocumentMatchPool.PutLazy(curr)
					} else {
						ctx.DocumentMatchPool.Put(curr)
					}
				}
				if lazy {
					s.currs[i], err = s.lazySearchers[i].nextDocIDOnly(ctx)
				} else {
					s.currs[i], err = s.searchers[i].Next(ctx)
				}
				if err != nil {
					return nil, err
				}
			}
		}

		if rv != nil {
			return rv, nil
		}
	}
}

func (s *DisjunctionSliceSearcher) Advance(ctx *search.SearchContext,
	ID index.IndexInternalID,
) (*search.DocumentMatch, error) {
	if !s.initialized {
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}
	// get all searchers pointing at their first match
	var err error
	for i, searcher := range s.searchers {
		if s.currs[i] != nil {
			if s.currs[i].IndexInternalID.Compare(ID) >= 0 {
				continue
			}
			ctx.DocumentMatchPool.Put(s.currs[i])
		}
		s.currs[i], err = searcher.Advance(ctx, ID)
		if err != nil {
			return nil, err
		}
	}

	err = s.updateMatches()
	if err != nil {
		return nil, err
	}

	return s.Next(ctx)
}

func (s *DisjunctionSliceSearcher) Count() uint64 {
	// for now return a worst case
	var sum uint64
	for _, searcher := range s.searchers {
		sum += searcher.Count()
	}
	return sum
}

func (s *DisjunctionSliceSearcher) Close() (rv error) {
	for _, searcher := range s.searchers {
		err := searcher.Close()
		if err != nil && rv == nil {
			rv = err
		}
	}
	return rv
}

func (s *DisjunctionSliceSearcher) Min() int {
	return s.min
}

func (s *DisjunctionSliceSearcher) DocumentMatchPoolSize() int {
	rv := len(s.currs)
	for _, s := range s.searchers {
		rv += s.DocumentMatchPoolSize()
	}
	return rv
}

// a disjunction searcher implements the index.Optimizable interface
// but only activates on an edge case where the disjunction is a
// wrapper around a single Optimizable child searcher
func (s *DisjunctionSliceSearcher) Optimize(kind string, octx index.OptimizableContext) (
	index.OptimizableContext, error,
) {
	if len(s.searchers) == 1 {
		o, ok := s.searchers[0].(index.Optimizable)
		if ok {
			return o.Optimize(kind, octx)
		}
	}

	return nil, nil
}
