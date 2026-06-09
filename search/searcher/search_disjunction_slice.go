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
	currs                  []*search.DocumentMatch
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
	// (wandUnavailableImpacts) = WAND cannot be applied for this query
	// (non-BM25 scorer, or at least one term returned math.MaxFloat64).
	//
	// See zapx/inverted_text_cache.go for the full cache-hierarchy diagram.
	//
	// FUTURE optimisations considered but not yet implemented:
	//   1. Snapshot-level maxTFNorm cache: IndexSnapshotTermFieldReader.MaxTFNorm
	//      currently iterates N segments per term per query.  Caching the
	//      cross-segment max on IndexSnapshot would cut initWANDMaxImpacts from
	//      ~900 ns to ~30 ns for a 3-term/15-segment query.
	//   2. Block-max WAND (Lucene ImpactsDISI): store max-impact per 128-doc
	//      block in the posting list; skip entire blocks when block_max <
	//      threshold rather than checking every doc.  Requires format change.
	//   3. res.Total accuracy: pruned candidates are not counted in
	//      ctx.Collector's total, mirroring Lucene's approximate-total mode.
	//      A TotalRelation field on SearchResult should expose this
	//      (symmetric with the existing Total field name).
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
}

// wandUnavailableImpacts is a non-nil zero-length sentinel stored in
// wandMaxImpacts when WAND cannot be applied for the current query.
var wandUnavailableImpacts = make([]float64, 0)

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

		matching:     make([]*search.DocumentMatch, len(searchers)),
		matchingIdxs: make([]int, len(searchers)),
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

// initWANDMaxImpacts pre-computes each searcher's MaxImpact() into the
// wandMaxImpacts slice so the per-candidate hot path only does array reads
// and float additions with no interface dispatch or type assertions.
// Also builds maxscoreOrder (argsort of wandMaxImpacts ascending) for MAXSCORE.
// Sets wandMaxImpacts to wandUnavailableImpacts if WAND cannot be applied.
func (s *DisjunctionSliceSearcher) initWANDMaxImpacts() {
	mi := make([]float64, len(s.searchers))
	for i, searcher := range s.searchers {
		wi, ok := searcher.(wandImpacter)
		if !ok {
			s.wandMaxImpacts = wandUnavailableImpacts
			return
		}
		v := wi.MaxImpact()
		if v >= math.MaxFloat64 {
			s.wandMaxImpacts = wandUnavailableImpacts
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
	// Invalidate both caches: MaxImpact and the MAXSCORE sort order depend on queryNorm.
	s.wandMaxImpacts = nil
	s.maxscoreOrder = nil
	s.lastThreshold = 0
	for _, searcher := range s.searchers {
		searcher.SetQueryNorm(qnorm)
	}
}

func (s *DisjunctionSliceSearcher) Next(ctx *search.SearchContext) (
	*search.DocumentMatch, error,
) {
	if !s.initialized {
		err := s.initSearchers(ctx)
		if err != nil {
			return nil, err
		}
	}

	// MAXSCORE: when we have a score threshold and WAND is available, check
	// whether at least one term is non-essential.  If so, use the MAXSCORE
	// path which skips Next() calls on non-essential iterators entirely.
	if ctx.ScoreThreshold > 0 {
		if s.wandMaxImpacts == nil {
			s.initWANDMaxImpacts()
		}
		if len(s.wandMaxImpacts) > 0 { // WAND available
			if ctx.ScoreThreshold != s.lastThreshold {
				s.computeMAXSCOREPivot(ctx.ScoreThreshold)
			}
			if s.pivotIdx == len(s.maxscoreOrder) {
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
			// WAND pruning: skip scoring when upper bound ≤ threshold.
			if !s.wandAboveThreshold(ctx) {
				// discard; advance happens below
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
// Invariant on entry: pivotIdx > 0 (caller checked).
func (s *DisjunctionSliceSearcher) nextMAXSCORE(ctx *search.SearchContext) (
	*search.DocumentMatch, error,
) {
	var err error
	// minID is a slice into s.minIDBuf (a struct field, already on the heap).
	// Using a local [8]byte would escape to the heap every call because the
	// slice is passed to Advance(), an interface method — see s.minIDBuf doc.
	var minID index.IndexInternalID

	for {
		// Find the minimum docID among essential iterators.
		minID = minID[:0]
		for _, si := range s.maxscoreOrder[s.pivotIdx:] {
			curr := s.currs[si]
			if curr == nil {
				continue
			}
			if len(minID) == 0 || curr.IndexInternalID.Compare(minID) < 0 {
				n := copy(s.minIDBuf[:], curr.IndexInternalID)
				minID = s.minIDBuf[:n]
			}
		}
		if len(minID) == 0 {
			return nil, nil // all essential iterators exhausted
		}

		// Advance non-essential iterators to minID so they can contribute
		// bonus score if they happen to match this candidate.
		for _, si := range s.maxscoreOrder[:s.pivotIdx] {
			curr := s.currs[si]
			if curr != nil && curr.IndexInternalID.Compare(minID) < 0 {
				ctx.DocumentMatchPool.Put(curr)
				s.currs[si], err = s.searchers[si].Advance(ctx, minID)
				if err != nil {
					return nil, err
				}
			}
		}

		// Collect all terms (essential and non-essential) that match minID.
		s.matching = s.matching[:0]
		s.matchingIdxs = s.matchingIdxs[:0]
		for i, curr := range s.currs {
			if curr != nil && curr.IndexInternalID.Compare(minID) == 0 {
				s.matching = append(s.matching, curr)
				s.matchingIdxs = append(s.matchingIdxs, i)
			}
		}

		// Score if we have enough matching terms and the upper bound clears the threshold.
		var rv *search.DocumentMatch
		if len(s.matching) >= s.min && s.wandAboveThreshold(ctx) {
			if s.retrieveScoreBreakdown {
				rv = s.scorer.ScoreAndExplBreakdown(ctx, s.matching, s.matchingIdxs, s.originalPos, s.numSearchers)
			} else {
				rv = s.scorer.Score(ctx, s.matching, len(s.matching), s.numSearchers)
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
		for i, curr := range s.currs {
			if curr != nil && curr.IndexInternalID.Compare(minID) == 0 {
				if curr != rv {
					ctx.DocumentMatchPool.Put(curr)
				}
				s.currs[i], err = s.searchers[i].Next(ctx)
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
