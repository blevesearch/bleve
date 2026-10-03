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

package bleve

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/collector"
	"github.com/blevesearch/bleve/v2/search/query"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

// skipTestIndex builds an index whose terms have many blocks of postings, with
// frequencies and field lengths that vary a lot from block to block, so that a
// lone term search has blocks to skip. The segments have 9000 docs: more than the
// 4096 of a batch of the algorithms that work on windows of docs, and than the 8192
// of their outer window, so that they cross the edges of those. With deletions, one
// doc in 20 is deleted after the fact.
func skipTestIndex(t testing.TB, model string, deletions bool) (Index, func()) {
	t.Helper()
	return skipTestIndexN(t, model, deletions, 27000, 300)
}

// skipTestIndexN is skipTestIndex for a number of docs, of which starsPer100k
// in every 100000 are "stars": short, with a high frequency of "hot".
func skipTestIndexN(t testing.TB, model string, deletions bool, docs, starsPer100k int) (Index, func()) {
	t.Helper()
	return skipTestIndexB(t, model, deletions, docs, starsPer100k, 3)
}

// skipTestIndexB is skipTestIndexN for a number of segments (batches).
func skipTestIndexB(t testing.TB, model string, deletions bool, docs, starsPer100k, batches int) (Index, func()) {
	t.Helper()
	dir := createTmpIndexPath(t)
	im := NewIndexMapping()
	im.ScoringModel = model
	idx, err := NewUsing(dir, im, scorch.Name, scorch.Name, map[string]interface{}{
		// no merging: the segments, and the deletions in them, stay as made
		"scorchMergePlanOptions": map[string]interface{}{"MaxSegmentSize": 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	cleanup := func() {
		_ = idx.Close()
		cleanupTmpIndexPath(t, dir)
	}

	rnd := rand.New(rand.NewSource(7))
	zipf := rand.NewZipf(rnd, 1.3, 3, 999)
	for bt := 0; bt < batches; bt++ {
		b := idx.NewBatch()
		for i := bt * docs / batches; i < (bt+1)*docs/batches; i++ {
			var sb strings.Builder
			// "hot" is in every doc; a few docs, spread all over, have it often
			// and are short, the rest once and long: the best hits are rare, and
			// most blocks have none of them. (Doc numbers within a segment aren't
			// in the order of the docs, so no order could be relied on.)
			words := 30 + rnd.Intn(40)
			if rnd.Intn(100000) < starsPer100k {
				words = 3 + rnd.Intn(5)
				sb.WriteString("hot hot hot hot hot hot ")
			} else {
				sb.WriteString("hot ")
			}
			for k := words; k > 0; k-- {
				sb.WriteString("w" + strconv.FormatUint(zipf.Uint64(), 10) + " ")
			}
			// "common" is everywhere, with a frequency that is mostly 1
			for r := 1 + rnd.Intn(rnd.Intn(8)+1)/4; r > 0; r-- {
				sb.WriteString("common ")
			}
			if err := b.Index(strconv.Itoa(i), map[string]interface{}{"body": sb.String()}); err != nil {
				t.Fatal(err)
			}
		}
		persisted := make(chan error, 1)
		b.SetPersistedCallback(func(err error) { persisted <- err })
		if err := idx.Batch(b); err != nil {
			t.Fatal(err)
		}
		if err := <-persisted; err != nil {
			t.Fatal(err)
		}
	}
	if deletions {
		b := idx.NewBatch()
		for i := 0; i < docs; i += 20 {
			b.Delete(strconv.Itoa(i))
		}
		if err := idx.Batch(b); err != nil {
			t.Fatal(err)
		}
	}
	return idx, cleanup
}

// A lone scored term skips the blocks that can't make the top hits. What it
// returns is the regular path's: the same hits and max score, an exact total
// when no segment has deletions, and, with deletions, a total that is a lower
// bound and says so.
func TestPerSegmentTermBlockSkipping(t *testing.T) {
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		for _, deletions := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deletions=%v", model, deletions), func(t *testing.T) {
				idx, cleanup := skipTestIndex(t, model, deletions)
				defer cleanup()

				segments, anyDeleted := segmentCountAndDeletions(t, idx)
				if segments < 3 || anyDeleted != deletions {
					t.Fatalf("test index has %d segments (deletions: %v)", segments, anyDeleted)
				}

				var skippedSome bool
				for _, term := range []string{"hot", "common", "w0", "w1", "w7", "w50", "w400"} {
					for _, sz := range []struct{ size, from int }{{1, 0}, {10, 0}, {10, 25}, {100, 0}, {1000, 0}, {0, 0}} {
						what := fmt.Sprintf("%s %s deletions=%v %+v", model, term, deletions, sz)
						mk := func() *SearchRequest {
							return NewSearchRequestOptions(termQueryOn("body", term), sz.size, sz.from, false)
						}
						old, got := runBothPaths(t, idx, mk, what)
						compareSearchResults(t, what, old, got)

						if got.TotalRelation == TotalRelationGte {
							skippedSome = true
							if !deletions {
								t.Fatalf("%s: a total that's a lower bound without any deletion", what)
							}
						}
						if sz.size == 0 && got.TotalRelation != TotalRelationEq {
							t.Fatalf("%s: a search without hits has to count exactly", what)
						}
						// the answer is what it is, with or without the skipping
						if got.MaxScore != 0 && !sameScore(old.MaxScore, got.MaxScore) {
							t.Fatalf("%s: max score %v, want %v", what, got.MaxScore, old.MaxScore)
						}
					}
				}
				if deletions && !skippedSome {
					t.Fatalf("no search skipped a block: the test has nothing to test")
				}
			})
		}
	}
}

// The skipping has to be skipping: a search for few hits reads fewer bytes than
// one that needs every block, and without deletions it still counts exactly.
func TestPerSegmentTermBlockSkippingReadsLess(t *testing.T) {
	idx, cleanup := skipTestIndex(t, index.BM25Scoring, false)
	defer cleanup()

	bytesOf := func(size int) (uint64, *SearchResult) {
		res, err := idx.Search(NewSearchRequestOptions(termQueryOn("body", "hot"), size, 0, false))
		if err != nil {
			t.Fatal(err)
		}
		return res.Cost, res
	}
	// warm: the first query on a segment pays for its metadata
	bytesOf(10)
	few, resFew := bytesOf(10)
	all, resAll := bytesOf(27000)
	if resFew.TotalRelation != TotalRelationEq || resFew.Total != resAll.Total {
		t.Fatalf("total %d (%v) for 10 hits, %d (%v) for all", resFew.Total, resFew.TotalRelation,
			resAll.Total, resAll.TotalRelation)
	}
	// the skip data of the term is read either way, and the blocks are small
	// (a few dozen bytes: the docs are consecutive), so it's the difference that
	// tells
	if few*100 > all*92 {
		t.Fatalf("10 hits read %d bytes, all of them %d: nothing was skipped", few, all)
	}
	t.Logf("10 hits: %d bytes, all hits: %d bytes", few, all)
}

// BenchmarkTermBlockSkipping is a lone scored term search where most blocks
// can be skipped ("hot": the best hits are in the first blocks of each
// segment), and one where none can ("common").
func BenchmarkTermBlockSkipping(b *testing.B) {
	idx, cleanup := skipTestIndexN(b, index.BM25Scoring, false, 200000, 20)
	defer cleanup()
	for _, term := range []string{"hot", "common"} {
		q := termQueryOn("body", term)
		for i := 0; i < 50; i++ {
			if _, err := idx.Search(NewSearchRequestOptions(q, 10, 0, false)); err != nil {
				b.Fatal(err)
			}
		}
		b.Run(term, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := idx.Search(NewSearchRequestOptions(q, 10, 0, false)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// A plain OR of terms returns the regular path's hits whichever algorithm finds
// them, on an index with many docs per segment so that the pruning has windows
// of docs to work through.
func TestPerSegmentDisjunctionAlgorithms(t *testing.T) {
	queries := [][]string{
		{"w0", "w1", "w2"},
		{"w0", "w1", "w10", "w100", "hot"},
		{"w0", "w400", "w900"},
		{"common", "w3", "w50", "w600", "w700", "w800"},
		{"w1", "w2", "w3", "w4", "w5", "w6", "w7", "w8", "w9", "w10"},
		{"hot", "w997", "w998"},
		{"w0", "nonexistent", "w1", "w5"},
	}
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		for _, deletions := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deletions=%v", model, deletions), func(t *testing.T) {
				idx, cleanup := skipTestIndex(t, model, deletions)
				defer cleanup()
				for _, algo := range []string{"maxscore", "wand"} {
					restore := searcher.SetPerSegmentDisjunctionAlgo(algo)
					for qi, terms := range queries {
						for _, sz := range []struct{ size, from int }{{1, 0}, {10, 0}, {10, 30}, {200, 0}} {
							what := fmt.Sprintf("%s %s deletions=%v query %d %v %+v", algo, model, deletions, qi, terms, sz)
							mk := func() *SearchRequest {
								qs := make([]query.Query, len(terms))
								for i, term := range terms {
									qs[i] = termQueryOn("body", term)
								}
								return NewSearchRequestOptions(query.NewDisjunctionQuery(qs), sz.size, sz.from, false)
							}
							old, got := runBothPaths(t, idx, mk, what)
							compareCompositeResults(t, what, old, got)
						}
					}
					restore()
				}
			})
		}
	}
}

// MAXSCORE has to return the best docs. For random ORs of terms, from very
// common to very rare, the truth is the regular path's ranking of every match;
// the top k of the per segment path must be the first k of it: the same score
// at every rank (so the same docs, but for those that tie at the last rank),
// and each doc the score it has in the truth.
func TestPerSegmentMaxScoreRetrievesTheBestDocs(t *testing.T) {
	vocab := []string{"hot", "common"}
	for _, w := range []int{0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 997} {
		vocab = append(vocab, "w"+strconv.Itoa(w))
	}
	pruned := 0
	checked := 0
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		for _, deletions := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deletions=%v", model, deletions), func(t *testing.T) {
				idx, cleanup := skipTestIndex(t, model, deletions)
				defer cleanup()
				restore := searcher.SetPerSegmentDisjunctionAlgo("maxscore")
				defer restore()

				rnd := rand.New(rand.NewSource(99))
				for qi := 0; qi < 40; qi++ {
					terms := map[string]bool{}
					for want := 2 + rnd.Intn(9); len(terms) < want; {
						terms[vocab[rnd.Intn(len(vocab))]] = true
					}
					names := make([]string, 0, len(terms))
					for name := range terms {
						names = append(names, name)
					}
					mk := func(size int) *SearchRequest {
						qs := make([]query.Query, len(names))
						for i, name := range names {
							qs[i] = termQueryOn("body", name)
						}
						return NewSearchRequestOptions(query.NewDisjunctionQuery(qs), size, 0, false)
					}

					perSegmentSearchEnabled.Store(false)
					truth, err := idx.Search(mk(12000))
					perSegmentSearchEnabled.Store(true)
					if err != nil {
						t.Fatal(err)
					}
					truthScore := make(map[string]float64, len(truth.Hits))
					for _, h := range truth.Hits {
						truthScore[h.ID] = h.Score
					}

					for _, k := range []int{1, 5, 10, 50} {
						what := fmt.Sprintf("%s deletions=%v %v k=%d", model, deletions, names, k)
						got, err := idx.Search(mk(k))
						if err != nil {
							t.Fatalf("%s: %v", what, err)
						}
						want := min(k, len(truth.Hits))
						if len(got.Hits) != want {
							t.Fatalf("%s: %d hits, want %d", what, len(got.Hits), want)
						}
						seen := map[string]bool{}
						for i, h := range got.Hits {
							if seen[h.ID] {
								t.Fatalf("%s: doc %s twice", what, h.ID)
							}
							seen[h.ID] = true
							if !sameScore(h.Score, truth.Hits[i].Score) {
								t.Fatalf("%s: rank %d scores %v, the best doc at that rank scores %v",
									what, i, h.Score, truth.Hits[i].Score)
							}
							if ts, ok := truthScore[h.ID]; !ok || !sameScore(h.Score, ts) {
								t.Fatalf("%s: doc %s scores %v, it is %v (a match: %v) in the truth",
									what, h.ID, h.Score, ts, ok)
							}
						}
						if got.TotalRelation == TotalRelationGte {
							pruned++
							if got.Total > truth.Total {
								t.Fatalf("%s: total %d above the real %d", what, got.Total, truth.Total)
							}
						} else if got.Total != truth.Total {
							t.Fatalf("%s: exact total %d, want %d", what, got.Total, truth.Total)
						}
						checked++
					}
				}
			})
		}
	}
	t.Logf("%d searches checked, %d of them pruned", checked, pruned)
	if pruned == 0 {
		t.Fatalf("nothing was pruned: the test doesn't test the pruning")
	}
}

// An AND of terms returns the best docs whichever way its windows are done
// (bitmaps always, as soon as they can be, or never): the first k of the regular
// path's ranking of every match, up to ties at the last rank.
func TestPerSegmentConjunctionRetrievesTheBestDocs(t *testing.T) {
	vocab := []string{"hot", "common"}
	for _, w := range []int{0, 1, 2, 3, 4, 5, 6, 8, 13, 21, 34, 55, 89, 144} {
		vocab = append(vocab, "w"+strconv.Itoa(w))
	}
	for _, minCands := range []int{1, 16, 1 << 30} {
		for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
			for _, deletions := range []bool{false, true} {
				t.Run(fmt.Sprintf("mincands=%d/%s/deletions=%v", minCands, model, deletions), func(t *testing.T) {
					idx, cleanup := skipTestIndex(t, model, deletions)
					defer cleanup()
					restore := searcher.SetPerSegmentConjunctionBitmapMinCandidates(minCands)
					defer restore()

					rnd := rand.New(rand.NewSource(5))
					for qi := 0; qi < 30; qi++ {
						terms := map[string]bool{}
						for want := 2 + rnd.Intn(3); len(terms) < want; {
							terms[vocab[rnd.Intn(len(vocab))]] = true
						}
						names := make([]string, 0, len(terms))
						for name := range terms {
							names = append(names, name)
						}
						mk := func(size int) *SearchRequest {
							qs := make([]query.Query, len(names))
							for i, name := range names {
								qs[i] = termQueryOn("body", name)
							}
							return NewSearchRequestOptions(query.NewConjunctionQuery(qs), size, 0, false)
						}
						perSegmentSearchEnabled.Store(false)
						truth, err := idx.Search(mk(12000))
						perSegmentSearchEnabled.Store(true)
						if err != nil {
							t.Fatal(err)
						}
						truthScore := make(map[string]float64, len(truth.Hits))
						for _, h := range truth.Hits {
							truthScore[h.ID] = h.Score
						}
						for _, k := range []int{1, 5, 10, 50} {
							what := fmt.Sprintf("mincands=%d %s deletions=%v %v k=%d", minCands, model, deletions, names, k)
							got, err := idx.Search(mk(k))
							if err != nil {
								t.Fatalf("%s: %v", what, err)
							}
							if want := min(k, len(truth.Hits)); len(got.Hits) != want {
								t.Fatalf("%s: %d hits, want %d", what, len(got.Hits), want)
							}
							seen := map[string]bool{}
							for i, h := range got.Hits {
								if seen[h.ID] {
									t.Fatalf("%s: doc %s twice", what, h.ID)
								}
								seen[h.ID] = true
								if !sameScore(h.Score, truth.Hits[i].Score) {
									t.Fatalf("%s: rank %d scores %v, the best doc at that rank scores %v",
										what, i, h.Score, truth.Hits[i].Score)
								}
								if ts, ok := truthScore[h.ID]; !ok || !sameScore(h.Score, ts) {
									t.Fatalf("%s: doc %s scores %v, it is %v (a match: %v) in the truth", what, h.ID, h.Score, ts, ok)
								}
							}
							if got.TotalRelation == TotalRelationGte {
								if got.Total > truth.Total {
									t.Fatalf("%s: total %d above the real %d", what, got.Total, truth.Total)
								}
							} else if got.Total != truth.Total {
								t.Fatalf("%s: exact total %d, want %d", what, got.Total, truth.Total)
							}
						}
					}
				})
			}
		}
	}
}

// A plain OR of terms that is read match by match (the generic loop, which is what
// the collection of a sort other than by score is made of) finds every match, with
// the score the regular path gives it, and counts them all: it is the best k of the
// regular path's ranking of every match, and the total is exact.
func TestPerSegmentOrGenericLoopFindsEveryMatch(t *testing.T) {
	vocab := []string{"hot", "common"}
	for _, w := range []int{0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 997} {
		vocab = append(vocab, "w"+strconv.Itoa(w))
	}
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		for _, deletions := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deletions=%v", model, deletions), func(t *testing.T) {
				idx, cleanup := skipTestIndex(t, model, deletions)
				defer cleanup()
				adv, err := idx.Advanced()
				if err != nil {
					t.Fatal(err)
				}
				reader, err := adv.Reader()
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = reader.Close() }()
				// the model the index scores with, which the searchers ask for
				ctx := context.WithValue(context.Background(), search.GetScoringModelCallbackKey,
					search.GetScoringModelCallbackFn(func() string { return model }))

				rnd := rand.New(rand.NewSource(21))
				for qi := 0; qi < 25; qi++ {
					terms := map[string]bool{}
					for want := 2 + rnd.Intn(9); len(terms) < want; {
						terms[vocab[rnd.Intn(len(vocab))]] = true
					}
					names := make([]string, 0, len(terms))
					qs := make([]query.Query, 0, len(terms))
					for name := range terms {
						names = append(names, name)
						qs = append(qs, termQueryOn("body", name))
					}
					mk := func() query.Query { return query.NewDisjunctionQuery(qs) }

					perSegmentSearchEnabled.Store(false)
					truth, err := idx.Search(NewSearchRequestOptions(mk(), 27000, 0, false))
					perSegmentSearchEnabled.Store(true)
					if err != nil {
						t.Fatal(err)
					}

					for _, scored := range []bool{true, false} {
						opts := search.SearcherOptions{}
						if !scored {
							opts.Score = ScoreNone
						}
						for _, k := range []int{1, 10, 100, 27000} {
							what := fmt.Sprintf("%s deletions=%v %v scored=%v k=%d", model, deletions, names, scored, k)
							ps, err := mk().(query.PerSegmentQuery).PerSegmentSearcher(ctx, reader, NewIndexMapping(), opts)
							if err != nil {
								t.Fatalf("%s: %v", what, err)
							}
							c := collector.NewPerSegmentTopNCollector(k, 0)
							// the generic loop: NextMatch until there are no more
							if err := c.Collect(ctx, genericOnly{ps}, reader); err != nil {
								t.Fatalf("%s: %v", what, err)
							}
							_ = ps.Close()

							if c.Total() != truth.Total || c.EarlyStopped() {
								t.Fatalf("%s: total %d (early stopped: %v), want exactly %d", what,
									c.Total(), c.EarlyStopped(), truth.Total)
							}
							got := c.Results()
							if want := min(k, len(truth.Hits)); len(got) != want {
								t.Fatalf("%s: %d hits, want %d", what, len(got), want)
							}
							if !scored {
								continue // the hits are the first matches; their scores are 0
							}
							if !sameScore(c.MaxScore(), truth.MaxScore) {
								t.Fatalf("%s: max score %v, want %v", what, c.MaxScore(), truth.MaxScore)
							}
							truthScore := map[string]float64{}
							for _, h := range truth.Hits {
								truthScore[h.ID] = h.Score
							}
							for i, h := range got {
								if !sameScore(h.Score, truth.Hits[i].Score) {
									t.Fatalf("%s: rank %d scores %v, want %v", what, i, h.Score, truth.Hits[i].Score)
								}
								if ts, ok := truthScore[h.ID]; !ok || !sameScore(h.Score, ts) {
									t.Fatalf("%s: doc %s scores %v, it is %v (%v) in the truth", what, h.ID, h.Score, ts, ok)
								}
							}
						}
					}
				}
			})
		}
	}
}

// An AND of terms that is read match by match (the generic loop, which is what
// the collection of a sort other than by score is made of) finds every match, with
// the score the regular path gives it, and counts them all: it is the best k of the
// regular path's ranking of every match, and the total is exact.
func TestPerSegmentAndGenericLoopFindsEveryMatch(t *testing.T) {
	vocab := []string{"hot", "common"}
	for _, w := range []int{0, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 997} {
		vocab = append(vocab, "w"+strconv.Itoa(w))
	}
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		for _, deletions := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deletions=%v", model, deletions), func(t *testing.T) {
				idx, cleanup := skipTestIndex(t, model, deletions)
				defer cleanup()
				adv, err := idx.Advanced()
				if err != nil {
					t.Fatal(err)
				}
				reader, err := adv.Reader()
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = reader.Close() }()
				// the model the index scores with, which the searchers ask for
				ctx := context.WithValue(context.Background(), search.GetScoringModelCallbackKey,
					search.GetScoringModelCallbackFn(func() string { return model }))

				rnd := rand.New(rand.NewSource(21))
				for qi := 0; qi < 30; qi++ {
					terms := map[string]bool{}
					for want := 2 + rnd.Intn(3); len(terms) < want; {
						terms[vocab[rnd.Intn(len(vocab))]] = true
					}
					names := make([]string, 0, len(terms))
					qs := make([]query.Query, 0, len(terms))
					for name := range terms {
						names = append(names, name)
						qs = append(qs, termQueryOn("body", name))
					}
					mk := func() query.Query { return query.NewConjunctionQuery(qs) }

					perSegmentSearchEnabled.Store(false)
					truth, err := idx.Search(NewSearchRequestOptions(mk(), 27000, 0, false))
					perSegmentSearchEnabled.Store(true)
					if err != nil {
						t.Fatal(err)
					}

					for _, scored := range []bool{true, false} {
						opts := search.SearcherOptions{}
						if !scored {
							opts.Score = ScoreNone
						}
						for _, k := range []int{1, 10, 100, 27000} {
							what := fmt.Sprintf("%s deletions=%v %v scored=%v k=%d", model, deletions, names, scored, k)
							ps, err := mk().(query.PerSegmentQuery).PerSegmentSearcher(ctx, reader, NewIndexMapping(), opts)
							if err != nil {
								t.Fatalf("%s: %v", what, err)
							}
							c := collector.NewPerSegmentTopNCollector(k, 0)
							// the generic loop: NextMatch until there are no more
							if err := c.Collect(ctx, genericOnly{ps}, reader); err != nil {
								t.Fatalf("%s: %v", what, err)
							}
							_ = ps.Close()

							if c.Total() != truth.Total || c.EarlyStopped() {
								t.Fatalf("%s: total %d (early stopped: %v), want exactly %d", what,
									c.Total(), c.EarlyStopped(), truth.Total)
							}
							got := c.Results()
							if want := min(k, len(truth.Hits)); len(got) != want {
								t.Fatalf("%s: %d hits, want %d", what, len(got), want)
							}
							if !scored {
								continue // the hits are the first matches; their scores are 0
							}
							if !sameScore(c.MaxScore(), truth.MaxScore) {
								t.Fatalf("%s: max score %v, want %v", what, c.MaxScore(), truth.MaxScore)
							}
							truthScore := map[string]float64{}
							for _, h := range truth.Hits {
								truthScore[h.ID] = h.Score
							}
							for i, h := range got {
								if !sameScore(h.Score, truth.Hits[i].Score) {
									t.Fatalf("%s: rank %d scores %v, want %v", what, i, h.Score, truth.Hits[i].Score)
								}
								if ts, ok := truthScore[h.ID]; !ok || !sameScore(h.Score, ts) {
									t.Fatalf("%s: doc %s scores %v, it is %v (%v) in the truth", what, h.ID, h.Score, ts, ok)
								}
							}
						}
					}
				}
			})
		}
	}
}

// Booleans and ORs and ANDs inside other composites have the cursors of their
// clauses picked by how they will be used (read through, or sought: see
// segCursorSeeked), and read a window of docs at a time when that pays. Whichever it
// is, the hits are the regular path's, on segments of more than a window of docs, with
// clauses that are dense and sparse, alike and not.
func TestPerSegmentNestedAndBooleanOnBigSegments(t *testing.T) {
	terms := func(names ...string) []query.Query {
		rv := make([]query.Query, len(names))
		for i, n := range names {
			rv[i] = termQueryOn("body", n)
		}
		return rv
	}
	or := func(names ...string) query.Query { return query.NewDisjunctionQuery(terms(names...)) }
	and := func(names ...string) query.Query { return query.NewConjunctionQuery(terms(names...)) }
	queries := []struct {
		name string
		q    func() query.Query
	}{
		{"and of an or and a term", func() query.Query {
			return query.NewConjunctionQuery([]query.Query{or("w0", "w1", "w5"), termQueryOn("body", "w2")})
		}},
		{"a rare term and an or", func() query.Query {
			return query.NewConjunctionQuery([]query.Query{termQueryOn("body", "hot"), or("w0", "w1", "w5")})
		}},
		{"a very rare term and an or", func() query.Query {
			return query.NewConjunctionQuery([]query.Query{termQueryOn("body", "w997"), or("w0", "w1", "common")})
		}},
		{"or of an and and a term", func() query.Query {
			return query.NewDisjunctionQuery([]query.Query{and("w0", "w1"), termQueryOn("body", "w2")})
		}},
		{"or of two ands", func() query.Query {
			return query.NewDisjunctionQuery([]query.Query{and("w0", "w1"), and("w2", "w3")})
		}},
		{"and of two ors", func() query.Query {
			return query.NewConjunctionQuery([]query.Query{or("w0", "w3"), or("w1", "w2", "w4")})
		}},
		{"boolean: must, should and must not", func() query.Query {
			return query.NewBooleanQuery(terms("w0"), terms("w1", "w5"), terms("w2"))
		}},
		{"boolean: must with ors", func() query.Query {
			return query.NewBooleanQuery([]query.Query{or("w0", "w1")}, []query.Query{or("w2", "w3")}, []query.Query{or("w4", "w6")})
		}},
		{"boolean: should only", func() query.Query { return query.NewBooleanQuery(nil, terms("w0", "w1", "w5"), nil) }},
		{"boolean: should, must not", func() query.Query {
			return query.NewBooleanQuery(nil, terms("w0", "w1"), terms("w3", "w4"))
		}},
		{"boolean: a rare must and a dense should", func() query.Query {
			return query.NewBooleanQuery(terms("hot"), terms("w0", "w1"), terms("w2"))
		}},
		{"boolean: a dense must and a rare should", func() query.Query {
			return query.NewBooleanQuery(terms("w0"), terms("w400", "w600"), nil)
		}},
		{"boolean: must not rare", func() query.Query {
			return query.NewBooleanQuery(terms("w0", "w1"), nil, terms("hot"))
		}},
	}
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		for _, deletions := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/deletions=%v", model, deletions), func(t *testing.T) {
				idx, cleanup := skipTestIndex(t, model, deletions)
				defer cleanup()
				for _, qc := range queries {
					for _, sz := range []struct{ size, from int }{{1, 0}, {10, 0}, {10, 25}, {200, 0}, {0, 0}} {
						what := fmt.Sprintf("%s %s deletions=%v %+v", qc.name, model, deletions, sz)
						old, got := runBothPaths(t, idx, func() *SearchRequest {
							return NewSearchRequestOptions(qc.q(), sz.size, sz.from, false)
						}, what)
						compareCompositeResults(t, what, old, got)
					}
				}
			})
		}
	}
}
