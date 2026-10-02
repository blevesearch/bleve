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
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/collector"
	index "github.com/blevesearch/bleve_index_api"
)

func unscoredRequest(field, term string, size, from int) *SearchRequest {
	r := NewSearchRequestOptions(termQueryOn(field, term), size, from, false)
	r.Score = ScoreNone
	return r
}

// A search that wants no scores: the first size hits in doc order, the exact
// number of matches.
func TestPerSegmentUnscoredSearch(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()

	terms := []string{"common", "even", "five", "mid", "dup", "rare", "alpha", "missing"}
	sizes := []struct{ size, from int }{
		{10, 0}, {10, 7}, {1, 0}, {0, 0}, {3, 1}, {50, 100}, {5000, 0}, {10, 5000}, {128, 0}, {129, 128}, {200, 0},
	}
	for _, term := range terms {
		// what the exact number of matches is
		perSegmentSearchEnabled.Store(false)
		truth, err := idx.Search(NewSearchRequestOptions(termQueryOn("body", term), 1, 0, false))
		perSegmentSearchEnabled.Store(true)
		if err != nil {
			t.Fatal(err)
		}

		for _, sz := range sizes {
			what := fmt.Sprintf("term=%s size=%d from=%d", term, sz.size, sz.from)

			// the regular path's early stop gives the first matches in doc order
			perSegmentSearchEnabled.Store(false)
			old, err := idx.Search(unscoredRequest("body", term, sz.size, sz.from))
			perSegmentSearchEnabled.Store(true)
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}

			before := perSegmentSearches.Load()
			got, err := idx.Search(unscoredRequest("body", term, sz.size, sz.from))
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
			if perSegmentSearches.Load() != before+1 {
				t.Fatalf("%s: the per segment path was not taken", what)
			}

			if got.Total != truth.Total {
				t.Fatalf("%s: total %d, want the exact %d", what, got.Total, truth.Total)
			}
			if got.TotalRelation != TotalRelationEq {
				t.Fatalf("%s: total relation %v, want exact", what, got.TotalRelation)
			}
			if len(got.Hits) != len(old.Hits) {
				t.Fatalf("%s: %d hits, want %d", what, len(got.Hits), len(old.Hits))
			}
			for i := range old.Hits {
				if got.Hits[i].ID != old.Hits[i].ID {
					t.Fatalf("%s: hit %d is %s, want %s", what, i, got.Hits[i].ID, old.Hits[i].ID)
				}
				if got.Hits[i].Score != 0 {
					t.Fatalf("%s: hit %d scored %v", what, i, got.Hits[i].Score)
				}
			}
		}
	}
}

// The count query of a term: a search for no hits, whose total is the number of
// docs having the term.
func TestPerSegmentCountQuery(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()

	for _, term := range []string{"common", "even", "rare", "missing"} {
		perSegmentSearchEnabled.Store(false)
		truth, err := idx.Search(NewSearchRequestOptions(termQueryOn("body", term), 1, 0, false))
		perSegmentSearchEnabled.Store(true)
		if err != nil {
			t.Fatal(err)
		}

		for _, scored := range []bool{false, true} {
			req := NewSearchRequestOptions(termQueryOn("body", term), 0, 0, false)
			if !scored {
				req.Score = ScoreNone
			}
			got, err := idx.Search(req)
			if err != nil {
				t.Fatal(err)
			}
			if got.Total != truth.Total || len(got.Hits) != 0 || got.TotalRelation != TotalRelationEq {
				t.Fatalf("%s scored=%v: total %d (%v) with %d hits, want %d with none",
					term, scored, got.Total, got.TotalRelation, len(got.Hits), truth.Total)
			}
		}
	}
}

// genericOnly hides the optimized path of the searcher it wraps, so that the
// collector has to drain it block by block
type genericOnly struct {
	search.PerSegmentSearcher
}

// For the same searcher the optimized path and the generic loop have to give the
// very same answer: the scores come from the same kernel, so even those are
// bit for bit identical.
func TestPerSegmentOptimizedPathMatchesGenericLoop(t *testing.T) {
	for _, scored := range []bool{true, false} {
		for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
			idx, cleanup := perSegmentTestIndex(t, model, nil)

			adv, err := idx.Advanced()
			if err != nil {
				t.Fatal(err)
			}
			reader, err := adv.Reader()
			if err != nil {
				t.Fatal(err)
			}

			ctx := context.WithValue(context.Background(), search.PerSegmentSearchKey, true)
			opts := search.SearcherOptions{}
			if !scored {
				opts.Score = ScoreNone
			}
			newSearcher := func(term string) search.PerSegmentSearcher {
				s, err := termQueryOn("body", term).Searcher(ctx, reader, NewIndexMapping(), opts)
				if err != nil {
					t.Fatal(err)
				}
				ps, ok := s.(search.PerSegmentSearcher)
				if !ok {
					t.Fatalf("not a per segment searcher: %T", s)
				}
				return ps
			}

			for _, term := range []string{"common", "even", "five", "dup", "rare", "missing"} {
				for _, sz := range []struct{ size, from int }{{10, 0}, {25, 30}, {0, 0}, {5000, 0}, {1, 1500}} {
					what := fmt.Sprintf("scored=%v %s term=%s %+v", scored, model, term, sz)

					fast := newSearcher(term)
					if opt, ok := fast.(search.OptimizedPerSegmentSearcher); !ok || !opt.CanCollectOptimized() {
						t.Fatalf("%s: the term searcher has no optimized path", what)
					}
					cf := collector.NewPerSegmentTopNCollector(sz.size, sz.from)
					if err := cf.Collect(ctx, fast, reader); err != nil {
						t.Fatalf("%s: %v", what, err)
					}
					_ = fast.Close()

					slow := genericOnly{newSearcher(term)}
					if _, ok := search.PerSegmentSearcher(slow).(search.OptimizedPerSegmentSearcher); ok {
						t.Fatalf("%s: wrapper exposes the optimized path", what)
					}
					cg := collector.NewPerSegmentTopNCollector(sz.size, sz.from)
					if err := cg.Collect(ctx, slow, reader); err != nil {
						t.Fatalf("%s: %v", what, err)
					}
					_ = slow.Close()

					totalOK := cf.Total() == cg.Total()
					if cf.EarlyStopped() {
						// blocks were skipped: the total is a lower bound
						totalOK = cf.Total() <= cg.Total() && cf.Total() >= uint64(len(cf.Results()))
					}
					if !totalOK || cf.MaxScore() != cg.MaxScore() {
						t.Fatalf("%s: total/max %d/%v vs generic %d/%v", what,
							cf.Total(), cf.MaxScore(), cg.Total(), cg.MaxScore())
					}
					rf, rg := cf.Results(), cg.Results()
					if len(rf) != len(rg) {
						t.Fatalf("%s: %d hits vs generic %d", what, len(rf), len(rg))
					}
					for i := range rf {
						if rf[i].ID != rg[i].ID || rf[i].Score != rg[i].Score || (!cf.EarlyStopped() && rf[i].HitNumber != rg[i].HitNumber) {
							t.Fatalf("%s: hit %d is %s/%v/%d vs generic %s/%v/%d", what, i,
								rf[i].ID, rf[i].Score, rf[i].HitNumber, rg[i].ID, rg[i].Score, rg[i].HitNumber)
						}
					}
				}
			}
			_ = reader.Close()
			cleanup()
		}
	}
}
