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
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/collector"
	index "github.com/blevesearch/bleve_index_api"
)

// genericOnly hides the optimized path of the searcher it wraps, so that the
// collector has to drain it block by block
type genericOnly struct {
	search.PerSegmentSearcher
}

// ---------------------------------------------------------------- the oracle

type oracleHit struct {
	doc   uint64 // global doc number
	score float32
}

// compositeOracle is every match of a conjunction or disjunction of terms with
// its score, worked out in plain code from the postings of each term alone. It
// shares nothing with the cursors, the unions and the pruning, but the block
// kernel that scores.
//
// The terms' scorers have had the query norm set, so it takes the searchers a
// composite was built from.
func compositeOracle(t *testing.T, terms []*PerSegmentTermSearcher, conjunction bool, min int) []oracleHit {
	t.Helper()
	n := len(terms)
	type acc struct {
		sum     float32
		matches int
	}
	byDoc := map[uint64]*acc{}
	for seg := 0; seg < n; seg++ {
	}
	numSegs := 0
	for _, term := range terms {
		numSegs = max(numSegs, len(term.Readers()))
	}
	for seg := 0; seg < numSegs; seg++ {
		for _, term := range terms { // in the order of the query
			r := term.Readers()
			if seg >= len(r) || r[seg] == nil {
				continue
			}
			offset := r[seg].Offset()
			for _, p := range postingsOf(t, term, seg) {
				a := byDoc[offset+uint64(p.doc)]
				if a == nil {
					a = &acc{}
					byDoc[offset+uint64(p.doc)] = a
				}
				a.sum += p.score
				a.matches++
			}
		}
	}
	var rv []oracleHit
	for doc, a := range byDoc {
		if conjunction {
			if a.matches != n {
				continue
			}
			rv = append(rv, oracleHit{doc, a.sum})
		} else {
			if a.matches < max(min, 1) {
				continue
			}
			rv = append(rv, oracleHit{doc, a.sum * (float32(a.matches) / float32(n))})
		}
	}
	return rv
}

// best orders hits as the collector does: by score, ties to the lower doc
func best(hits []oracleHit, from, size int) []oracleHit {
	sorted := append([]oracleHit(nil), hits...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].score != sorted[j].score {
			return sorted[i].score > sorted[j].score
		}
		return sorted[i].doc < sorted[j].doc
	})
	if from >= len(sorted) {
		return nil
	}
	sorted = sorted[from:]
	if len(sorted) > size {
		sorted = sorted[:size]
	}
	return sorted
}

func inDocOrder(hits []oracleHit, from, size int) []oracleHit {
	sorted := append([]oracleHit(nil), hits...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].doc < sorted[j].doc })
	if from >= len(sorted) {
		return nil
	}
	sorted = sorted[from:]
	if len(sorted) > size {
		sorted = sorted[:size]
	}
	return sorted
}

// ---------------------------------------------------------------- the checks

type compositeBuilder func(fx *perSegFixture, terms []string, scored bool, model string) (
	composite search.PerSegmentSearcher, termSearchers []*PerSegmentTermSearcher)

func disjunctionBuilder(min int) compositeBuilder {
	return func(fx *perSegFixture, terms []string, scored bool, model string) (search.PerSegmentSearcher, []*PerSegmentTermSearcher) {
		var ts []*PerSegmentTermSearcher
		var qs []search.Searcher
		for _, term := range terms {
			s := fx.termSearcher(term, scored, model)
			ts = append(ts, s)
			qs = append(qs, s)
		}
		opts := search.SearcherOptions{}
		if !scored {
			opts.Score = "none"
		}
		return NewPerSegmentDisjunctionSearcher(qs, float64(min), opts), ts
	}
}

func conjunctionBuilder() compositeBuilder {
	return func(fx *perSegFixture, terms []string, scored bool, model string) (search.PerSegmentSearcher, []*PerSegmentTermSearcher) {
		var ts []*PerSegmentTermSearcher
		var qs []search.Searcher
		for _, term := range terms {
			s := fx.termSearcher(term, scored, model)
			ts = append(ts, s)
			qs = append(qs, s)
		}
		opts := search.SearcherOptions{}
		if !scored {
			opts.Score = "none"
		}
		return NewPerSegmentConjunctionSearcher(qs, opts), ts
	}
}

func collect(t *testing.T, s search.PerSegmentSearcher, fx *perSegFixture, size, from int) *collector.PerSegmentTopNCollector {
	t.Helper()
	c := collector.NewPerSegmentTopNCollector(size, from)
	if err := c.Collect(context.Background(), s, fx.reader); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
	return c
}

func checkHits(t *testing.T, what string, c *collector.PerSegmentTopNCollector, want []oracleHit, checkScores bool) {
	t.Helper()
	got := c.Results()
	if len(got) != len(want) {
		t.Fatalf("%s: %d hits, want %d", what, len(got), len(want))
	}
	for i := range want {
		if got[i].IndexInternalID.Value() != want[i].doc {
			t.Fatalf("%s: hit %d is doc %d, want %d (score %v, want %v)", what, i,
				got[i].IndexInternalID.Value(), want[i].doc, got[i].Score, want[i].score)
		}
		if checkScores && got[i].Score != float64(want[i].score) {
			t.Fatalf("%s: hit %d (doc %d) scores %v, want %v", what, i, want[i].doc, got[i].Score, want[i].score)
		}
	}
}

var compositeTermSets = [][]string{
	{"alpha", "bravo"},
	{"bravo", "charlie"},
	{"alpha", "foxtrot"},
	{"charlie", "delta", "echo"},
	{"alpha", "bravo", "charlie", "delta"},
	{"echo", "foxtrot"},
	{"alpha", "zulu"}, // zulu is nowhere
	{"bravo", "delta", "echo", "foxtrot", "zulu"},
	{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"},
	{"delta"},
}

var compositeSizes = []struct{ size, from int }{{1, 0}, {5, 0}, {10, 3}, {100, 0}, {2000, 0}, {0, 0}}

// runCompositeChecks checks a composite, in every mode, against the oracle.
func runCompositeChecks(t *testing.T, builder compositeBuilder, conjunction bool, min int) {
	prunedSomewhere := false
	for _, regime := range []struct{ deleteEvery, outlierOneIn int }{
		{0, 40},  // outliers everywhere: blocks all look alike
		{9, 40},  // and deletions
		{0, 900}, // rare outliers: the blocks are unlike
		{5, 900},
	} {
		deleteEvery := regime.deleteEvery
		fx := newPerSegFixture(t, perSegFixtureOpts{segments: 4, docsPerSeg: 1500, deleteEvery: deleteEvery,
			seed: 13, outlierOneIn: regime.outlierOneIn})
		for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
			for _, terms := range compositeTermSets {
				if conjunction && len(terms) < 2 {
					continue
				}
				// the oracle: from the terms of a composite of its own, as the query
				// norm is set by building one
				oc, ots := builder(fx, terms, true, model)
				_ = oc
				oracle := compositeOracle(t, ots, conjunction, min)
				for _, ot := range ots {
					_ = ot.Close()
				}

				for _, sz := range compositeSizes {
					what := fmt.Sprintf("deleted=%d %s %v min=%d size=%d from=%d", deleteEvery, model, terms, min, sz.size, sz.from)

					// with scores
					opt, _ := builder(fx, terms, true, model)
					co := collect(t, opt, fx, sz.size, sz.from)
					gen, _ := builder(fx, terms, true, model)
					cg := collect(t, genericOnly{gen}, fx, sz.size, sz.from)

					want := best(oracle, sz.from, sz.size)
					checkHits(t, what+" (generic)", cg, want, true)
					checkHits(t, what+" (optimized)", co, want, true)
					if cg.Total() != uint64(len(oracle)) || cg.EarlyStopped() {
						t.Fatalf("%s (generic): total %d (pruned: %v), want %d", what, cg.Total(), cg.EarlyStopped(), len(oracle))
					}
					if co.EarlyStopped() {
						prunedSomewhere = true
						if co.Total() > uint64(len(oracle)) {
							t.Fatalf("%s: pruned total %d above the real %d", what, co.Total(), len(oracle))
						}
					} else if co.Total() != uint64(len(oracle)) {
						t.Fatalf("%s: total %d, want %d", what, co.Total(), len(oracle))
					}
					if sz.size == 0 && co.EarlyStopped() {
						t.Fatalf("%s: a count query can't be pruned", what)
					}
					var oracleMax float32
					for _, h := range oracle {
						oracleMax = max(oracleMax, h.score)
					}
					if sz.size > 0 && len(want) > 0 && (co.MaxScore() != float64(oracleMax) || cg.MaxScore() != float64(oracleMax)) {
						t.Fatalf("%s: max score %v / %v, want %v", what, co.MaxScore(), cg.MaxScore(), oracleMax)
					}

					// without scores: the first ones in doc order, and the exact total
					uopt, _ := builder(fx, terms, false, model)
					uo := collect(t, uopt, fx, sz.size, sz.from)
					ugen, _ := builder(fx, terms, false, model)
					ug := collect(t, genericOnly{ugen}, fx, sz.size, sz.from)
					wantUnscored := inDocOrder(oracle, sz.from, sz.size)
					checkHits(t, what+" (unscored optimized)", uo, wantUnscored, false)
					checkHits(t, what+" (unscored generic)", ug, wantUnscored, false)
					// the generic path counts every match
					if ug.Total() != uint64(len(oracle)) || ug.EarlyStopped() {
						t.Fatalf("%s (unscored generic): total %d, want the exact %d", what, ug.Total(), len(oracle))
					}
					// the optimized one stops once it has the hits wanted, unless it
					// is a count; the total says if it is a lower bound
					switch {
					case sz.size == 0:
						if uo.Total() != uint64(len(oracle)) || uo.EarlyStopped() {
							t.Fatalf("%s (unscored): a count has to be exact, got %d (lower bound: %v), want %d",
								what, uo.Total(), uo.EarlyStopped(), len(oracle))
						}
					case uo.EarlyStopped():
						if uo.Total() > uint64(len(oracle)) || uo.Total() < uint64(minOf(len(oracle), sz.size+sz.from)) {
							t.Fatalf("%s (unscored): lower bound %d out of [%d, %d]", what, uo.Total(),
								minOf(len(oracle), sz.size+sz.from), len(oracle))
						}
					default:
						if uo.Total() != uint64(len(oracle)) {
							t.Fatalf("%s (unscored): total %d claimed exact, want %d", what, uo.Total(), len(oracle))
						}
					}
				}
			}
		}
	}
	if !prunedSomewhere {
		t.Fatal("the optimized path never pruned anything: the test doesn't exercise it")
	}
}

func TestPerSegmentDisjunctionMatchesOracle(t *testing.T) {
	runCompositeChecks(t, disjunctionBuilder(1), false, 1)
}

func TestPerSegmentConjunctionMatchesOracle(t *testing.T) {
	runCompositeChecks(t, conjunctionBuilder(), true, 0)
}

// a minimum number of matching clauses isn't pruned, but has to be right
func TestPerSegmentDisjunctionMinMatchesOracle(t *testing.T) {
	for _, min := range []int{2, 3} {
		fx := newPerSegFixture(t, perSegFixtureOpts{segments: 3, docsPerSeg: 1200, deleteEvery: 11, seed: 3})
		for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
			for _, terms := range compositeTermSets {
				if len(terms) < min {
					continue
				}
				_, ots := disjunctionBuilder(min)(fx, terms, true, model)
				oracle := compositeOracle(t, ots, false, min)
				for _, sz := range []struct{ size, from int }{{10, 0}, {500, 0}, {0, 0}} {
					s, _ := disjunctionBuilder(min)(fx, terms, true, model)
					if s.(search.OptimizedPerSegmentSearcher).CanCollectOptimized() {
						t.Fatal("a minimum above 1 must not be optimized")
					}
					c := collect(t, s, fx, sz.size, sz.from)
					what := fmt.Sprintf("min=%d %s %v %+v", min, model, terms, sz)
					checkHits(t, what, c, best(oracle, sz.from, sz.size), true)
					if c.Total() != uint64(len(oracle)) {
						t.Fatalf("%s: total %d, want %d", what, c.Total(), len(oracle))
					}
				}
			}
		}
	}
}

// the same checks, nested: a composite of composites, which only has the
// generic path
func TestPerSegmentNestedComposites(t *testing.T) {
	fx := newPerSegFixture(t, perSegFixtureOpts{segments: 3, docsPerSeg: 1000, deleteEvery: 0, seed: 21})
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		build := func(scored bool) (search.PerSegmentSearcher, []*PerSegmentTermSearcher) {
			opts := search.SearcherOptions{}
			if !scored {
				opts.Score = "none"
			}
			mk := func(term string) *PerSegmentTermSearcher { return fx.termSearcher(term, scored, model) }
			a, b, c, d := mk("alpha"), mk("bravo"), mk("charlie"), mk("delta")
			// alpha AND (bravo OR (charlie AND delta))
			inner := NewPerSegmentConjunctionSearcher([]search.Searcher{c, d}, opts)
			mid := NewPerSegmentDisjunctionSearcher([]search.Searcher{b, inner}, 1, opts)
			top := NewPerSegmentConjunctionSearcher([]search.Searcher{a, mid}, opts)
			return top, []*PerSegmentTermSearcher{a, b, c, d}
		}

		// the oracle, from the semantics: score(alpha AND (bravo OR (charlie AND delta)))
		// with the query norms the nesting gives
		top, ts := build(true)
		_ = top
		a, b, c, d := ts[0], ts[1], ts[2], ts[3]
		type post = map[uint64]float32
		toMap := func(s *PerSegmentTermSearcher) post {
			rv := post{}
			for seg, r := range s.Readers() {
				if r == nil {
					continue
				}
				for _, p := range postingsOf(t, s, seg) {
					rv[r.Offset()+uint64(p.doc)] = p.score
				}
			}
			return rv
		}
		pa, pb, pc, pd := toMap(a), toMap(b), toMap(c), toMap(d)
		var oracle []oracleHit
		for doc, sa := range pa {
			sb, hasB := pb[doc]
			sc, hasC := pc[doc]
			sd, hasD := pd[doc]
			hasInner := hasC && hasD
			if !hasB && !hasInner {
				continue
			}
			// mid: a disjunction of two clauses, bravo and the inner conjunction
			var sum float32
			m := 0
			if hasB {
				sum += sb
				m++
			}
			if hasInner {
				sum += sc + sd
				m++
			}
			mid := sum * (float32(m) / 2)
			oracle = append(oracle, oracleHit{doc, sa + mid})
		}

		for _, sz := range []struct{ size, from int }{{10, 0}, {300, 0}, {0, 0}} {
			s, _ := build(true)
			cs := collect(t, s, fx, sz.size, sz.from)
			what := fmt.Sprintf("nested %s %+v", model, sz)
			checkHits(t, what, cs, best(oracle, sz.from, sz.size), true)
			if cs.Total() != uint64(len(oracle)) || cs.EarlyStopped() {
				t.Fatalf("%s: total %d, want %d", what, cs.Total(), len(oracle))
			}
		}
		_ = math.Pi
	}
}

// A stress test of the pruning: many random corpora, with outliers of all
// densities, and many random queries over them, each checked against the
// oracle. A wrongly skipped doc only matters when it belongs to the top hits,
// which takes a particular layout of the postings; random ones find them.
func TestPerSegmentPruningStress(t *testing.T) {
	allTerms := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "zulu"}
	queries := 120
	if testing.Short() {
		queries = 30
	}
	pruned := map[bool]int{}
	for seed := int64(1); seed <= 4; seed++ {
		rnd := rand.New(rand.NewSource(seed * 7919))
		outlier := []int{25, 150, 700, 3000}[seed-1]
		// two of the four have segments of more than a window of docs (4096, and the
		// 8192 of the outer window of MAXSCORE), where the edges of windows are crossed
		fx := newPerSegFixture(t, perSegFixtureOpts{segments: 2 + int(seed%2),
			docsPerSeg:  []int{2500, 9000, 2500, 9000}[seed-1],
			deleteEvery: []int{0, 13, 0, 6}[seed-1], seed: seed, outlierOneIn: outlier})

		for q := 0; q < queries; q++ {
			// a random subset of the terms, in a random order
			perm := rnd.Perm(len(allTerms))
			n := 2 + rnd.Intn(5)
			var terms []string
			for _, i := range perm[:n] {
				terms = append(terms, allTerms[i])
			}
			model := []string{index.DefaultScoringModel, index.BM25Scoring}[rnd.Intn(2)]
			sz := []int{1, 2, 3, 5, 10}[rnd.Intn(5)]
			from := rnd.Intn(2)

			for _, conjunction := range []bool{false, true} {
				builder := disjunctionBuilder(1)
				if conjunction {
					builder = conjunctionBuilder()
				}
				_, ots := builder(fx, terms, true, model)
				oracle := compositeOracle(t, ots, conjunction, 1)
				for _, ot := range ots {
					_ = ot.Close()
				}

				s, _ := builder(fx, terms, true, model)
				c := collect(t, s, fx, sz, from)
				what := fmt.Sprintf("seed=%d outlier=%d conj=%v %s %v size=%d from=%d", seed, outlier, conjunction, model, terms, sz, from)
				checkHits(t, what, c, best(oracle, from, sz), true)
				if c.EarlyStopped() {
					pruned[conjunction]++
				} else if c.Total() != uint64(len(oracle)) {
					t.Fatalf("%s: total %d, want %d", what, c.Total(), len(oracle))
				}
			}
		}
	}
	t.Logf("searches that pruned: disjunctions %d, conjunctions %d", pruned[false], pruned[true])
	if pruned[false] == 0 || pruned[true] == 0 {
		t.Fatal("pruning never kicked in")
	}
}

// The conjunction's windows end where the first block of any of its terms
// does. The way to get that wrong only shows up in a layout where a term's next
// block is much hotter than the one before it, so this one asks small top-k of
// few terms, over corpora of rare outliers, as often as it can: the oracle of a
// term set is worked out once, and kept.
func TestPerSegmentConjunctionWindowStress(t *testing.T) {
	sets := [][]string{
		{"alpha", "bravo"}, {"alpha", "charlie"}, {"bravo", "charlie"}, {"alpha", "delta"},
		{"bravo", "delta"}, {"charlie", "delta"}, {"alpha", "bravo", "charlie"},
		{"alpha", "bravo", "delta"}, {"alpha", "charlie", "delta"}, {"bravo", "charlie", "delta"},
		{"alpha", "bravo", "charlie", "delta"},
		// a rare leader, whose one block spans many blocks of the other terms
		{"alpha", "echo"}, {"bravo", "echo"}, {"alpha", "foxtrot"}, {"charlie", "echo"},
		{"alpha", "bravo", "echo"}, {"alpha", "echo", "foxtrot"}, {"bravo", "delta", "echo"},
	}
	pruned := 0
	for _, regime := range []struct {
		outlierOneIn, deleteEvery int
		seed                      int64
	}{{150, 0, 31}, {400, 7, 32}, {1500, 0, 33}, {6000, 5, 34}} {
		// the last two have segments of more than a window of docs
		docsPerSeg := 3000
		if regime.seed >= 33 {
			docsPerSeg = 9000
		}
		fx := newPerSegFixture(t, perSegFixtureOpts{segments: 3, docsPerSeg: docsPerSeg,
			deleteEvery: regime.deleteEvery, seed: regime.seed, outlierOneIn: regime.outlierOneIn})
		for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
			for _, terms := range sets {
				_, ots := conjunctionBuilder()(fx, terms, true, model)
				oracle := compositeOracle(t, ots, true, 1)
				for _, ot := range ots {
					_ = ot.Close()
				}
				for _, sz := range []struct{ size, from int }{{1, 0}, {2, 0}, {3, 1}, {5, 0}, {1, 2}} {
					s, _ := conjunctionBuilder()(fx, terms, true, model)
					c := collect(t, s, fx, sz.size, sz.from)
					what := fmt.Sprintf("outlier=%d deleted=%d %s %v %+v", regime.outlierOneIn, regime.deleteEvery, model, terms, sz)
					checkHits(t, what, c, best(oracle, sz.from, sz.size), true)
					if c.EarlyStopped() {
						pruned++
					}
				}
			}
		}
	}
	t.Logf("pruned searches: %d", pruned)
	if pruned == 0 {
		t.Fatal("pruning never kicked in")
	}
}

func repeatToken(tok string, n int) []string {
	rv := make([]string, n)
	for i := range rv {
		rv[i] = tok
	}
	return rv
}

// A planted layout, which a conjunction's windows have to get right: the best
// doc is the one place where a common term is hot, deep inside a segment, while
// the rare term that leads has a single block that spans the whole segment, and
// an earlier segment has already set a threshold that the blocks at the start
// of the common term can't beat. A window that took its bounds from the first
// block of a common term alone would skip the best doc.
func TestPerSegmentConjunctionPlantedOutlier(t *testing.T) {
	const plantedAt = 1500
	tokens := func(seg, i int) []string {
		toks := []string{"cc", "yy"} // in every doc
		switch {
		case seg == 0 && i == 0:
			// a decent hit, to set the threshold
			toks = append(append(toks, repeatToken("zz", 3)...), repeatToken("cc", 5)...)
			toks = append(toks, repeatToken("yy", 5)...)
		case seg == 0 && i%30 == 0:
			toks = append(toks, "zz")
		case seg == 1 && i == plantedAt:
			toks = append(toks, "zz")
			toks = append(toks, repeatToken("cc", 250)...)
		case seg == 1 && i%100 == 7:
			toks = append(toks, "zz")
		}
		return toks
	}

	for attempt := int64(0); attempt < 12; attempt++ {
		fx := newPerSegFixture(t, perSegFixtureOpts{segments: 2, docsPerSeg: 3000, seed: attempt, docTokens: tokens})
		for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
			terms := []string{"zz", "cc", "yy"}
			_, ots := conjunctionBuilder()(fx, terms, true, model)
			oracle := compositeOracle(t, ots, true, 1)
			for _, ot := range ots {
				_ = ot.Close()
			}
			for _, sz := range []struct{ size, from int }{{1, 0}, {2, 0}, {3, 0}} {
				s, _ := conjunctionBuilder()(fx, terms, true, model)
				c := collect(t, s, fx, sz.size, sz.from)
				checkHits(t, fmt.Sprintf("planted attempt=%d %s %+v", attempt, model, sz), c, best(oracle, sz.from, sz.size), true)
			}
		}
	}
}

// The soundness of a conjunction's windows, checked head on: whatever doc a
// window starts at, no posting of any of the terms inside it may score more than
// the block max that the window's bounds give the term. That's what lets a
// whole window be skipped; a window that went past the end of one of the
// terms' blocks would break it.
func TestPerSegmentConjunctionWindowBoundsHold(t *testing.T) {
	for _, regime := range []struct {
		outlierOneIn int
		seed         int64
	}{{40, 41}, {300, 42}, {3000, 43}} {
		fx := newPerSegFixture(t, perSegFixtureOpts{segments: 2, docsPerSeg: 4000, seed: regime.seed, outlierOneIn: regime.outlierOneIn})
		rnd := rand.New(rand.NewSource(regime.seed))
		for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
			for _, terms := range [][]string{{"alpha", "bravo"}, {"alpha", "echo"}, {"bravo", "charlie", "delta"}, {"alpha", "bravo", "charlie", "echo"}} {
				_, ots := conjunctionBuilder()(fx, terms, true, model)
				// the postings of every term, by segment
				for seg := 0; seg < len(ots[0].Readers()); seg++ {
					var curs []*termCursor
					var postings [][]cursorPosting
					complete := true
					for idx, ot := range ots {
						if ot.Readers()[seg] == nil {
							complete = false
							break
						}
						postings = append(postings, postingsOf(t, ot, seg))
						_ = idx
					}
					if !complete {
						continue
					}
					// fresh cursors, as the oracle drained the readers
					_, cts := conjunctionBuilder()(fx, terms, true, model)
					for idx, ct := range cts {
						curs = append(curs, newTermCursor(idx, ct.Readers()[seg], ct.Scorer(), true))
					}
					order := append([]*termCursor(nil), curs...)
					sort.Slice(order, func(i, j int) bool { return order[i].cost < order[j].cost })
					leader, secs := order[0], order[1:]
					postingsOfCursor := func(c *termCursor) []cursorPosting { return postings[c.idx] }

					bms := make([]float32, len(secs))
					for trial := 0; trial < 120; trial++ {
						lp := postingsOfCursor(leader)
						doc := lp[rnd.Intn(len(lp))].doc
						if rnd.Intn(3) == 0 {
							doc = uint32(rnd.Intn(4000))
						}
						windowEnd, _, ok := conjunctionWindow(leader, secs, doc, bms)
						if !ok {
							continue
						}
						if windowEnd < doc {
							t.Fatalf("window [%d,%d] is empty", doc, windowEnd)
						}
						for _, c := range append([]*termCursor{leader}, secs...) {
							bm := c.BlockMaxScore()
							for _, p := range postingsOfCursor(c) {
								if p.doc >= doc && p.doc <= windowEnd && p.score > bm {
									t.Fatalf("%s %v seg %d: window [%d,%d]: doc %d scores %v above the block max %v",
										model, terms, seg, doc, windowEnd, p.doc, p.score, bm)
								}
							}
						}
					}
				}
				for _, ot := range ots {
					_ = ot.Close()
				}
			}
		}
	}
}

func minOf(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// totalsSink is a sink that only keeps the totals of the segments
type totalsSink struct {
	heap   *search.PerSegmentHeap
	totals map[int]uint64
}

func newTotalsSink() *totalsSink {
	return &totalsSink{heap: search.NewPerSegmentHeap(0), totals: map[int]uint64{}}
}

func (s *totalsSink) Limit() int                      { return 0 }
func (s *totalsSink) Heap(int) *search.PerSegmentHeap { return s.heap }
func (s *totalsSink) Threshold() (float32, bool)      { return 0, true }
func (s *totalsSink) AddTotal(seg int, n uint64)      { s.totals[seg] += n }
func (s *totalsSink) ObserveMaxScore(float32)         {}
func (s *totalsSink) MarkPruned()                     {}
func (s *totalsSink) total(seg int) uint64            { return s.totals[seg] }
func (s *totalsSink) reset()                          { s.totals = map[int]uint64{} }

// The two ways a conjunction is counted, tantivy's dense windows of bitmasks and
// its sparse walk, each forced on its own, against the intersection of the
// terms' postings done in plain code: they have to agree, whatever the density
// of the terms and however the windows fall.
func TestPerSegmentConjunctionCountStrategies(t *testing.T) {
	sets := [][]string{
		{"alpha", "bravo"}, {"alpha", "bravo", "charlie"}, {"bravo", "charlie"}, {"charlie", "delta"},
		{"alpha", "delta"}, {"bravo", "delta", "echo"}, {"alpha", "echo"}, {"alpha", "bravo", "charlie", "delta"},
		{"delta", "echo", "foxtrot"}, {"alpha", "foxtrot"}, {"charlie", "delta", "echo", "foxtrot"},
	}
	for _, deleteEvery := range []int{0, 5} {
		fx := newPerSegFixture(t, perSegFixtureOpts{segments: 3, docsPerSeg: 4000, deleteEvery: deleteEvery, seed: 77})
		for _, terms := range sets {
			// what each segment should have, from the postings
			_, ots := conjunctionBuilder()(fx, terms, false, index.DefaultScoringModel)
			want := map[int]uint64{}
			for seg := 0; seg < len(ots[0].Readers()); seg++ {
				var common map[uint32]bool
				ok := true
				for _, ot := range ots {
					if ot.Readers()[seg] == nil {
						ok = false
						break
					}
					docs := map[uint32]bool{}
					for _, p := range postingsOf(t, ot, seg) {
						docs[p.doc] = true
					}
					if common == nil {
						common = docs
						continue
					}
					for d := range common {
						if !docs[d] {
							delete(common, d)
						}
					}
				}
				if ok {
					want[seg] = uint64(len(common))
				}
			}

			for name, count := range map[string]func(*PerSegmentConjunctionSearcher, context.Context, search.PerSegmentSink, int, []*termCursor) error{
				"sparse": (*PerSegmentConjunctionSearcher).countSparse,
				"dense": func(s *PerSegmentConjunctionSearcher, ctx context.Context, sink search.PerSegmentSink, seg int, curs []*termCursor) error {
					order := append([]*termCursor(nil), curs...)
					sort.Slice(order, func(i, j int) bool { return order[i].cost < order[j].cost })
					return s.countDense(ctx, sink, seg, order)
				},
				"chosen": (*PerSegmentConjunctionSearcher).countSegment,
			} {
				cs, cts := conjunctionBuilder()(fx, terms, false, index.DefaultScoringModel)
				conj := cs.(*PerSegmentConjunctionSearcher)
				sink := newTotalsSink()
				for seg := 0; seg < len(cts[0].Readers()); seg++ {
					var curs []*termCursor
					for idx, ct := range cts {
						if ct.Readers()[seg] == nil {
							curs = nil
							break
						}
						curs = append(curs, newTermCursor(idx, ct.Readers()[seg], ct.Scorer(), false))
					}
					if curs == nil {
						continue
					}
					if err := count(conj, context.Background(), sink, seg, curs); err != nil {
						t.Fatal(err)
					}
					if sink.total(seg) != want[seg] {
						t.Fatalf("deleted=%d %v seg %d: %s counts %d, want %d", deleteEvery, terms, seg, name, sink.total(seg), want[seg])
					}
				}
				sink.reset()
			}
		}
	}
}

// A clause that is a disjunction or conjunction of one is its clause: a match of
// one token is one, and an OR of those should be an OR of terms, to the bit, and
// get the algorithms that work on terms.
func TestPerSegmentUnwrapsSingleClauses(t *testing.T) {
	fx := newPerSegFixture(t, perSegFixtureOpts{segments: 3, docsPerSeg: 2000, deleteEvery: 7, seed: 51})
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		for _, scored := range []bool{true, false} {
			opts := search.SearcherOptions{}
			if !scored {
				opts.Score = "none"
			}
			mk := func(wrapped bool, conjunction bool, terms ...string) search.PerSegmentSearcher {
				var qs []search.Searcher
				for _, term := range terms {
					var c search.Searcher = fx.termSearcher(term, scored, model)
					if wrapped {
						if (len(qs)+1)%2 == 0 {
							c = NewPerSegmentConjunctionSearcher([]search.Searcher{c}, opts)
						} else {
							c = NewPerSegmentDisjunctionSearcher([]search.Searcher{c}, 1, opts)
						}
					}
					qs = append(qs, c)
				}
				if conjunction {
					return NewPerSegmentConjunctionSearcher(qs, opts)
				}
				return NewPerSegmentDisjunctionSearcher(qs, 1, opts)
			}

			for _, conjunction := range []bool{false, true} {
				terms := []string{"alpha", "bravo", "charlie", "delta"}
				wrapped := mk(true, conjunction, terms...)

				// they are made of terms: the algorithms apply
				switch w := wrapped.(type) {
				case *PerSegmentDisjunctionSearcher:
					if w.terms == nil || !w.pruningApplies() {
						t.Fatal("an OR of one clause queries should be an OR of terms")
					}
				case *PerSegmentConjunctionSearcher:
					if w.terms == nil || !w.pruningApplies() {
						t.Fatal("an AND of one clause queries should be an AND of terms")
					}
				}

				_ = wrapped.Close()

				for _, sz := range []struct{ size, from int }{{5, 0}, {50, 3}, {0, 0}} {
					what := fmt.Sprintf("%s scored=%v conj=%v %+v", model, scored, conjunction, sz)
					a := collect(t, mk(true, conjunction, terms...), fx, sz.size, sz.from)
					b := collect(t, mk(false, conjunction, terms...), fx, sz.size, sz.from)
					ra, rb := a.Results(), b.Results()
					if len(ra) != len(rb) || a.Total() != b.Total() || a.EarlyStopped() != b.EarlyStopped() {
						t.Fatalf("%s: %d hits / total %d vs %d hits / total %d", what, len(ra), a.Total(), len(rb), b.Total())
					}
					for i := range ra {
						if ra[i].IndexInternalID.Value() != rb[i].IndexInternalID.Value() || ra[i].Score != rb[i].Score {
							t.Fatalf("%s: hit %d: %v/%v vs %v/%v", what, i, ra[i].IndexInternalID.Value(), ra[i].Score,
								rb[i].IndexInternalID.Value(), rb[i].Score)
						}
					}
				}
			}
		}
	}

	// a disjunction that wants two of its one clause matches nothing, whatever it
	// is wrapped in: it is not its clause
	s := fx.termSearcher("alpha", true, index.DefaultScoringModel)
	never := NewPerSegmentDisjunctionSearcher([]search.Searcher{s}, 2, search.SearcherOptions{})
	outer := NewPerSegmentDisjunctionSearcher([]search.Searcher{never, fx.termSearcher("bravo", true, index.DefaultScoringModel)}, 1, search.SearcherOptions{})
	if outer.terms != nil {
		t.Fatal("a disjunction with a minimum above its one clause must not be unwrapped")
	}
	c := collect(t, outer, fx, 5, 0)
	if c.Total() == 0 {
		t.Fatal("the bravo clause still matches")
	}
}
