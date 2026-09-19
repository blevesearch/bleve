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
	"math"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// A conjunction's MUST clauses are, today, only ever walked doc-at-a-time via
// leapfrog (see search_conjunction.go's Next). This is the block-max WAND
// alternative: a batch score-then-filter pass over the rarest clause's
// decoded postings block, pruned by the sum of every clause's block-max
// bound against the collector's top-K threshold, before paying for a real
// seek into any other clause. Ported from tantivy's block_wand_intersection
// (see BLOCK_BATCH_SCORING.md §8) onto zapx v18's BlockMax/ShallowAdvance/
// NextBlock primitives -- the same primitives search_term_blockmax.go
// already uses for a single term, driven here across several clauses at
// once.
//
// Unlike tantivy, zapx does not expose "decode a block but stop partway
// through it" -- NextBlock always decodes one whole physical 128-doc block.
// So a window here is a *sub-range* of the leader's current physical block:
// bound each iteration to the tightest lastDoc among the leader's own block
// and every secondary's current block (a plain BlockMax() peek, never a
// ShallowAdvance -- see the note on that below), process the prefix of the
// still-decoded leader entries that falls within it, and leave the rest
// pending for the next iteration. No re-decode needed: the physical block
// stays resident in leaderDocs/Freqs/Norms/Scores until fully consumed.
//
// Bound computation must never ShallowAdvance a secondary: doing so moves
// its cursor *forward past* candidates this same window still needs to seek
// to in phase 2, and a subsequent Advance() to an earlier candidate is a
// backward seek -- undefined for a forward-only postings iterator, and
// silently wrong in practice (it returns whatever the cursor already
// overshot to, so a real match looks like a miss). ShallowAdvance is only
// ever safe here *after* a window's candidates have all been decided one
// way or another, moving every secondary to windowEnd+1 -- strictly past
// everything just processed, strictly before everything still pending.
type blockConjunction struct {
	leader      conjLeg
	secondaries []conjLeg

	// threshold is the collector's current top-K cutoff: a document must
	// score strictly greater than this to be worth returning, matching
	// collectBulk's own cutoff test. Starts at -Inf (tantivy's Score::MIN)
	// so the engine runs, unpruned, even before a threshold ever arrives --
	// exactly the same shape as TermSearcher.hasMinCompetitiveScore, except
	// the -Inf default makes every comparison below a natural no-op rather
	// than needing a separate boolean guard.
	threshold float64
	// hasThreshold is true once setMinCompetitiveScore has ever been
	// called. From that point on, every threshold-driven shortcut below
	// (the whole-window skip, the pass-1 score pre-filter, and the pass-2
	// suffix-sum early exit) may have bypassed a candidate without ever
	// checking whether it was a real intersection match, so Total() can no
	// longer be trusted as exact -- see ConjunctionSearcher.
	// TotalIsApproximate. Tracked as "was a threshold ever supplied" rather
	// than "did a skip actually happen": simpler, and conservative in the
	// same direction every other skip safety guard in this codebase errs.
	hasThreshold bool

	// done is set the first time any secondary reports no document at or
	// after a candidate: because candidates are visited in ascending doc
	// order, that secondary can never match anything from here on, and
	// neither can the conjunction as a whole.
	done bool

	// The leader's currently decoded physical block (<=conjBlockCap docs),
	// resident across however many sub-windows it takes to consume it.
	// leaderPos is the next unconsumed index; leaderLen is how much is
	// decoded. leaderBound is this block's MaxScore bound (math.Inf(1) --
	// never a valid bound to prune with -- when the block was decoded
	// without one).
	leaderDocs      []uint64
	leaderFreqs     []uint64
	leaderNorms     []float64
	leaderScores    []float64
	leaderPos       int
	leaderLen       int
	leaderBound     float64
	leaderNoBound   bool
	leaderExhausted bool

	// Phase-1 survivors of the current sub-window's score pre-filter,
	// scratch reused every sub-window.
	candDocs   []uint64
	candScores []float64

	// Per-secondary block-max bound for the current sub-window, and its
	// suffix sums (secSuffix[i] = sum of secBound[i+1:]) for the same
	// incremental early-exit tantivy's pass 2 uses.
	secBound  []float64
	secSuffix []float64

	// secCursor[i] is the last document secondaries[i].ts.reader actually
	// returned from Advance, with secFreq[i]/secNorm[i] its (freq, norm) --
	// Advance may overshoot past a non-matching candidate to a later real
	// document ("the specified document or its immediate follower"), and
	// that overshoot is very often itself a later leader candidate for a
	// dense clause pair. Re-seeking to a document the reader already sits
	// at or past is a backward-or-equal seek relative to its true
	// position, which scorch's Advance handles by rebuilding the whole
	// reader from scratch (see IndexSnapshotTermFieldReader.Advance's
	// "seek backwards" fallback) -- ruinously expensive if it happens on
	// every miss for a dense conjunction. Tracking this lets
	// scoreCandidates, for any candidate the secondary has already
	// reached: skip it outright if the cursor is past it (guaranteed
	// non-match, same optimization tantivy's block_wand_intersection
	// documents), or reuse the cached (freq, norm) with no Advance call at
	// all if the cursor sits exactly on it (a real match discovered as a
	// previous candidate's overshoot).
	secCursor []uint64
	secValid  []bool
	secFreq   []uint64
	secNorm   []float64

	// Scratch for a single-document membership score, reused across every
	// secondary seek so no slice is allocated per candidate.
	freq1  [1]uint64
	norm1  [1]float64
	score1 [1]float64

	tfdScratch index.TermFieldDoc
	idBuf      []byte

	// The current sub-window's survivors, drained into the caller's
	// DocScoreBlock across possibly more than one ScoreBlock call.
	survDocs   []uint64
	survScores []float64
	survPos    int

	// noBenefitStreak/skipPrefilter: adaptive bailout for when phase 1's
	// score pre-filter isn't earning its keep. Two similarly-frequent,
	// similarly-scored clauses (e.g. two mid-frequency terms) give the
	// pre-filter almost nothing to reject -- profiling found ~98.7% of
	// candidates surviving it for such a pair -- yet every call still pays
	// for the filtering loop itself, the phase-2 suffix-sum bookkeeping it
	// exists to feed, and idBuf/cursor-switch overhead on candidates that
	// were never going to be rejected anyway: a CPU profile attributed
	// ~32% of scoreCandidates' own time to this bookkeeping for exactly
	// such a pair, on top of the ~56% that is the genuine, unavoidable
	// cost of a real secondary Advance() call.
	//
	// noBenefitStreak counts consecutive WINDOWS whose phase-1 pass
	// rejected zero candidates (see scoreCandidates). Once that streak
	// crosses conjNoBenefitBailoutStreak, skipPrefilter permanently skips
	// the phase-1 filtering comparison and the phase-2 suffix-sum early
	// exit for the rest of this query -- both cheap arithmetic that isn't
	// paying for itself -- while leaving every correctness-relevant piece
	// untouched: the real Advance()-based membership check, the
	// secCursor/secValid overshoot cache (a genuine efficiency win
	// unrelated to score-based pruning), and advanceWindow's whole-window
	// skip check (already cheap at ~1.76% of profiled time, and still
	// occasionally useful even when per-candidate filtering isn't).
	//
	// A streak over WINDOWS rather than individual candidates, unlike the
	// disjunction path's equivalent bailout: a window batches up to
	// conjBlockCap (128) candidate-level pass/fail outcomes, so "this
	// whole window rejected nothing" is already a much rarer coincidence
	// for a clause pair where the pre-filter is genuinely earning its
	// keep. At a real per-candidate rejection rate of even 5%, a window
	// rejecting nothing by pure chance has probability roughly
	// (0.95)^128 =~ 0.14%; at and-mid-mid's measured ~1.3% rejection rate
	// it's close to 19%, so a handful of consecutive clean windows
	// reliably separates "not helping at all" from "helping, just not on
	// this one window" without needing the much larger streak the
	// disjunction bailout needs to survive many more, much cheaper,
	// individual-document trials.
	//
	// One-way for the lifetime of this blockConjunction (one per query,
	// spanning every segment): a clause pair's relative frequency/idf is
	// fixed for the whole query, so there is nothing to react to by
	// re-enabling filtering later.
	noBenefitStreak int
	skipPrefilter   bool
}

// conjNoBenefitBailoutStreak is how many consecutive windows must each
// reject zero phase-1 candidates before scoreCandidates concludes the
// pre-filter isn't earning its keep for this clause pair and permanently
// skips it -- see blockConjunction.noBenefitStreak's doc comment for the
// probability reasoning behind this specific value.
const conjNoBenefitBailoutStreak = 4

// EnableConjunctionBlockMaxWAND gates the whole feature: when false,
// canBlockConjunct always reports ineligible, so NewConjunctionSearcher
// falls back to today's push-down-narrowing + scalar-leapfrog path
// unconditionally. A kill switch for the same reason index/scorch/
// optimize.go's OptimizeConjunction exists, and a lever tests use to get a
// known-good reference result to compare the new path against.
var EnableConjunctionBlockMaxWAND = true

// conjBlockCap is zapx v18's on-disk postings block size (bitpack.BlockLen).
// One block-max bound covers exactly this many documents (fewer for a
// postings list's final, unindexed tail).
const conjBlockCap = 128

// conjLeg is one clause of a block-WAND conjunction: a *TermSearcher plus
// the reader-side capabilities the engine drives directly, below the
// search.Searcher level, so a membership check doesn't pay for a pooled
// DocumentMatch or Explain machinery it will immediately discard.
type conjLeg struct {
	ts  *TermSearcher
	bmr blockMaxReader
	nb  blockTermFieldReader
}

// canBlockConjunct reports whether every clause is eligible for the
// block-WAND conjunction engine: a bare TermSearcher, able to bulk-score,
// actually running BM25 (see TermQueryScorer.UsesBM25 -- zapx's stored
// bound carries no guarantee under any other formula), and backed by a
// reader that exposes both block-max bounds and bulk decode.
//
// Callers must decide this once, before anything has a chance to narrow the
// clauses' readers (see NewConjunctionSearcher): a reader that has been
// through the "conjunction" push-down optimization's ReplaceActual still
// satisfies these interfaces structurally, it just silently reports no
// block-max bound at all from then on, which would make this decision
// wrong in a way nothing here could detect after the fact.
func canBlockConjunct(searchers []search.Searcher) bool {
	if !EnableConjunctionBlockMaxWAND {
		return false
	}
	if len(searchers) < 2 {
		return false
	}
	for _, s := range searchers {
		ts, ok := s.(*TermSearcher)
		if !ok || !ts.scorer.CanScoreBulk() || !ts.scorer.UsesBM25() {
			return false
		}
		if _, ok := ts.reader.(blockMaxReader); !ok {
			return false
		}
		if _, ok := ts.reader.(blockTermFieldReader); !ok {
			return false
		}
	}
	return true
}

func newBlockConjunction(searchers []search.Searcher) *blockConjunction {
	legs := make([]conjLeg, len(searchers))
	for i, s := range searchers {
		ts, ok := s.(*TermSearcher)
		if !ok {
			return nil
		}
		bmr, ok := ts.reader.(blockMaxReader)
		if !ok {
			return nil
		}
		nb, ok := ts.reader.(blockTermFieldReader)
		if !ok {
			return nil
		}
		legs[i] = conjLeg{ts: ts, bmr: bmr, nb: nb}
	}
	numSec := len(legs) - 1
	return &blockConjunction{
		leader:       legs[0],
		secondaries:  legs[1:],
		threshold:    math.Inf(-1),
		leaderDocs:   make([]uint64, conjBlockCap),
		leaderFreqs:  make([]uint64, conjBlockCap),
		leaderNorms:  make([]float64, conjBlockCap),
		leaderScores: make([]float64, conjBlockCap),
		candDocs:     make([]uint64, conjBlockCap),
		candScores:   make([]float64, conjBlockCap),
		secBound:     make([]float64, numSec),
		secSuffix:    make([]float64, numSec),
		secCursor:    make([]uint64, numSec),
		secValid:     make([]bool, numSec),
		secFreq:      make([]uint64, numSec),
		secNorm:      make([]float64, numSec),
		survDocs:     make([]uint64, 0, conjBlockCap),
		survScores:   make([]float64, 0, conjBlockCap),
	}
}

func (bc *blockConjunction) setMinCompetitiveScore(minScore float64) {
	bc.threshold = minScore
	bc.hasThreshold = true
}

// scoreBlock fills out with this conjunction's next batch of scored,
// ascending-doc-order matches, resuming a partially-drained sub-window
// before computing a new one.
func (bc *blockConjunction) scoreBlock(out *search.DocScoreBlock) (int, error) {
	capOut := len(out.IDs)
	if capOut == 0 {
		return 0, nil
	}
	for {
		if bc.survPos < len(bc.survDocs) {
			n := copy(out.IDs, bc.survDocs[bc.survPos:])
			copy(out.Scores, bc.survScores[bc.survPos:bc.survPos+n])
			bc.survPos += n
			return n, nil
		}

		more, err := bc.advanceWindow()
		if err != nil {
			return 0, err
		}
		if !more {
			return 0, nil
		}
		// Either survDocs now holds this sub-window's matches (drained on
		// the next loop iteration), or it produced none and the loop tries
		// the next one.
	}
}

// ensureLeaderBlock decodes the leader's next physical block once the
// current one (if any) is fully consumed. Returns false only once the
// leader is exhausted.
func (bc *blockConjunction) ensureLeaderBlock() (bool, error) {
	if bc.leaderPos < bc.leaderLen {
		return true, nil
	}
	if bc.leaderExhausted {
		return false, nil
	}

	maxTF, maxNorm, _, docCount, blockOK := bc.leader.bmr.BlockMax()
	var n int
	var err error
	if blockOK {
		n, err = bc.leader.nb.NextBlock(bc.leaderDocs[:docCount], bc.leaderFreqs[:docCount], bc.leaderNorms[:docCount])
		bc.leaderBound = bc.leader.ts.scorer.MaxScore(maxTF, maxNorm)
		bc.leaderNoBound = false
	} else {
		// No bound available for whatever's next (exhausted, mid-block,
		// tail, or an unsupported segment) -- decode and score it anyway,
		// just without anything to prune with.
		n, err = bc.leader.nb.NextBlock(bc.leaderDocs, bc.leaderFreqs, bc.leaderNorms)
		bc.leaderBound = math.Inf(1)
		bc.leaderNoBound = true
	}
	if err != nil {
		return false, err
	}
	if n == 0 {
		bc.leaderExhausted = true
		return false, nil
	}
	bc.leader.ts.scorer.ScoreBulk(bc.leaderFreqs[:n], bc.leaderNorms[:n], bc.leaderScores[:n])
	bc.leaderLen, bc.leaderPos = n, 0
	return true, nil
}

// advanceWindow processes one sub-window: the prefix of the leader's
// currently-decoded (and possibly partially consumed) physical block that
// falls at or before the tightest lastDoc among the leader and every
// secondary's current block. Appends any surviving matches to
// bc.survDocs/survScores. Returns more=false only once the conjunction can
// never produce another match; the caller should otherwise keep calling
// this (each time getting an empty or non-empty sub-window) until it does.
func (bc *blockConjunction) advanceWindow() (bool, error) {
	bc.survDocs = bc.survDocs[:0]
	bc.survScores = bc.survScores[:0]
	bc.survPos = 0

	if bc.done {
		return false, nil
	}
	ok, err := bc.ensureLeaderBlock()
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	// windowEnd starts at the leader's own remaining range and is clipped
	// down by each secondary's *current* block -- a non-destructive
	// BlockMax() peek only. degraded means "some bound in this sum isn't
	// available," in which case the whole remaining physical leader block
	// becomes one unpruned sub-window instead of being clipped further.
	windowEnd := bc.leaderDocs[bc.leaderLen-1]
	degraded := bc.leaderNoBound
	secSum := 0.0
	if !degraded {
		for i := range bc.secondaries {
			maxTF, maxNorm, lastDoc, _, ok := bc.secondaries[i].bmr.BlockMax()
			if !ok {
				degraded = true
				break
			}
			if lastDoc < windowEnd {
				windowEnd = lastDoc
			}
			b := bc.secondaries[i].ts.scorer.MaxScore(maxTF, maxNorm)
			bc.secBound[i] = b
			secSum += b
		}
	}

	// The prefix of the still-pending leader entries that falls within
	// this sub-window; the rest stays resident for the next call.
	k := bc.leaderPos
	for k < bc.leaderLen && bc.leaderDocs[k] <= windowEnd {
		k++
	}

	skip := !degraded && bc.leaderBound+secSum <= bc.threshold

	if !skip && k > bc.leaderPos {
		if err := bc.scoreCandidates(bc.leaderPos, k, degraded, secSum); err != nil {
			return false, err
		}
	}
	bc.leaderPos = k

	// Every secondary that contributed to windowEnd is now done with this
	// sub-window's candidates (skipped outright, or fully seeked to by
	// scoreCandidates below): move each to windowEnd+1. Every not-yet-
	// processed leader entry is strictly > windowEnd by construction of k
	// above, so this can never advance past a future candidate.
	if !degraded {
		for i := range bc.secondaries {
			if err := bc.secondaries[i].bmr.ShallowAdvance(windowEnd + 1); err != nil {
				return false, err
			}
		}
	}

	return true, nil
}

// scoreCandidates runs phase 2 over the leader's decoded entries in
// [from, to): a batch score pre-filter against the best any secondary
// could possibly contribute, then a real membership check (score-first) for
// survivors only, appending matches to bc.survDocs/survScores.
func (bc *blockConjunction) scoreCandidates(from, to int, degraded bool, secSum float64) error {
	m := 0
	if bc.skipPrefilter {
		// Bailed out: every leader entry in range is a candidate, copied
		// over unconditionally rather than compared against scoreThreshold
		// -- see noBenefitStreak's doc comment for why this comparison
		// stopped earning its keep for this clause pair.
		m = to - from
		copy(bc.candDocs[:m], bc.leaderDocs[from:to])
		copy(bc.candScores[:m], bc.leaderScores[from:to])
	} else {
		scoreThreshold := math.Inf(-1)
		if !degraded {
			scoreThreshold = bc.threshold - secSum
		}
		for i := from; i < to; i++ {
			if bc.leaderScores[i] > scoreThreshold {
				bc.candDocs[m] = bc.leaderDocs[i]
				bc.candScores[m] = bc.leaderScores[i]
				m++
			}
		}
		// Track whether this window's pass rejected anything at all --
		// see noBenefitStreak's doc comment for the probability reasoning
		// behind treating a run of totally-unproductive windows as a
		// signal to stop paying for this check.
		if m == to-from {
			bc.noBenefitStreak++
			if bc.noBenefitStreak >= conjNoBenefitBailoutStreak {
				bc.skipPrefilter = true
			}
		} else {
			bc.noBenefitStreak = 0
		}
	}
	if m == 0 {
		return nil
	}

	if !degraded && !bc.skipPrefilter {
		running := 0.0
		for i := len(bc.secondaries) - 1; i >= 0; i-- {
			bc.secSuffix[i] = running
			running += bc.secBound[i]
		}
	}

	for ci := 0; ci < m; ci++ {
		doc := bc.candDocs[ci]
		total := bc.candScores[ci]
		bc.idBuf = index.NewIndexInternalID(bc.idBuf, doc)

		matched := true
		for si := range bc.secondaries {
			sec := &bc.secondaries[si]
			var freq uint64
			var norm float64

			switch {
			case bc.secValid[si] && bc.secCursor[si] > doc:
				// This secondary's reader already overshot past doc while
				// resolving an earlier, smaller candidate -- it cannot
				// contain doc, and re-seeking to it would be a backward
				// seek (see the secCursor doc comment). No Advance call.
				matched = false
			case bc.secValid[si] && bc.secCursor[si] == doc:
				// The reader is already sitting exactly on doc -- discovered
				// as a previous candidate's overshoot, since that overshoot
				// is very often itself a later leader candidate for a dense
				// clause pair. Re-seeking to the same position is the same
				// ruinously expensive backward-or-equal case as above; reuse
				// what that overshoot already read instead.
				freq, norm = bc.secFreq[si], bc.secNorm[si]
			default:
				// secCursor[si] < doc, or the reader has never been
				// positioned: a genuine forward seek.
				tfd, err := sec.ts.reader.Advance(bc.idBuf, &bc.tfdScratch)
				if err != nil {
					return err
				}
				if tfd == nil {
					bc.done = true
					matched = false
				} else {
					bc.secCursor[si] = tfd.ID.Value()
					bc.secValid[si] = true
					bc.secFreq[si], bc.secNorm[si] = tfd.Freq, tfd.Norm
					if tfd.ID.Value() != doc {
						matched = false
					} else {
						freq, norm = tfd.Freq, tfd.Norm
					}
				}
			}
			// A break here would only exit the switch, not this loop over
			// secondaries (a Go gotcha worth flagging) -- this check is
			// deliberately outside the switch so it exits the right thing.
			if !matched {
				break
			}

			bc.freq1[0], bc.norm1[0] = freq, norm
			sec.ts.scorer.ScoreBulk(bc.freq1[:], bc.norm1[:], bc.score1[:])
			total += bc.score1[0]

			if !degraded && !bc.skipPrefilter && total+bc.secSuffix[si] <= bc.threshold {
				matched = false
				break
			}
		}

		if matched && total > bc.threshold {
			bc.survDocs = append(bc.survDocs, doc)
			bc.survScores = append(bc.survScores, total)
		}
		if bc.done {
			break
		}
	}
	return nil
}

// ---------------------------------------------------------------- wiring

// CanScoreBlock reports whether this conjunction's clauses qualify for the
// block-max WAND path -- decided once at construction (see wandEligible's
// doc comment on ConjunctionSearcher), since by the time this is called the
// push-down optimization may already have run for the non-eligible case.
func (s *ConjunctionSearcher) CanScoreBlock() bool {
	return s.wandEligible
}

// ScoreBlock drives the block-max WAND engine over this conjunction's
// clauses. See search_conjunction_block.go's blockConjunction for the
// algorithm.
func (s *ConjunctionSearcher) ScoreBlock(out *search.DocScoreBlock) (int, error) {
	if s.blockConj == nil {
		s.blockConj = newBlockConjunction(s.searchers)
		if s.blockConj == nil {
			return 0, nil
		}
	}
	return s.blockConj.scoreBlock(out)
}

// SetMinCompetitiveScore implements search.CompetitiveScorer: pushes the
// collector's tightening top-K threshold into the block-WAND engine so it
// can start pruning whole windows -- across every clause at once, unlike
// the single-term case -- that cannot possibly enter the result set.
//
// A conjunction pruned this way can skip documents no clause ever
// individually decoded, so unlike TermSearcher this deliberately does not
// implement search.SkippedForCompetitiveScore: a skipped candidate is not
// known to be a real match (most of a skipped window likely fails the
// other clauses), so there is no exact count to fold back into Total().
// It implements search.ApproximateTotal instead -- see TotalIsApproximate.
//
// The collector offers every root searcher a threshold generically,
// regardless of whether that searcher actually implements BulkSearcher, or
// whether the collector ends up driving it through the bulk path at all
// (canBulkCollect and CanScoreBlock are checked separately) -- see
// TopNCollector.propagateMinCompetitiveScore. A non-wandEligible
// conjunction always runs its scalar Next()/Advance() leapfrog, which never
// looks at blockConj, so a threshold offered to it here would otherwise sit
// unused and, worse, make TotalIsApproximate lie: nothing was ever pruned.
// Gating on wandEligible keeps this a true no-op in that case, matching
// search.CompetitiveScorer's documented "free to do nothing" contract.
func (s *ConjunctionSearcher) SetMinCompetitiveScore(minScore float64) {
	if !s.wandEligible {
		return
	}
	if s.blockConj == nil {
		s.blockConj = newBlockConjunction(s.searchers)
		if s.blockConj == nil {
			return
		}
	}
	s.blockConj.setMinCompetitiveScore(minScore)
}

// TotalIsApproximate implements search.ApproximateTotal: once a threshold
// has actually reached the block-WAND engine, this conjunction's Total()
// may under-count, the same documented trade-off
// TopNCollector.EarlyStopped() already describes for its own early-stop
// feature. Gated on wandEligible for the same reason SetMinCompetitiveScore
// is -- a threshold can be offered to this searcher without the block path
// ever running.
func (s *ConjunctionSearcher) TotalIsApproximate() bool {
	return s.wandEligible && s.blockConj != nil && s.blockConj.hasThreshold
}

var (
	_ search.BulkSearcher      = (*ConjunctionSearcher)(nil)
	_ search.CompetitiveScorer = (*ConjunctionSearcher)(nil)
	_ search.ApproximateTotal  = (*ConjunctionSearcher)(nil)
)
