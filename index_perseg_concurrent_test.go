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
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

// The per segment readers are pooled, which is only safe if no two searches
// ever hold the same one. Many searches of all shapes at once, on one index,
// each of which has to give exactly what it gives when it's alone. Run it under
// the race detector.
func TestPerSegmentSearchConcurrent(t *testing.T) {
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		idx, cleanup := perSegmentTestIndex(t, model, nil)

		type job struct {
			name string
			mk   func() *SearchRequest
		}
		var jobs []job
		for _, qc := range compositeQueryCases {
			if qc.name == "match of nothing" {
				continue
			}
			qc := qc
			for _, sz := range []struct{ size, from int }{{10, 0}, {100, 5}, {0, 0}} {
				sz := sz
				jobs = append(jobs, job{fmt.Sprintf("%s %+v", qc.name, sz), func() *SearchRequest {
					return NewSearchRequestOptions(qc.q(), sz.size, sz.from, false)
				}})
				jobs = append(jobs, job{fmt.Sprintf("%s %+v unscored", qc.name, sz), func() *SearchRequest {
					r := NewSearchRequestOptions(qc.q(), sz.size, sz.from, false)
					r.Score = ScoreNone
					return r
				}})
			}
		}
		for _, term := range []string{"common", "even", "rare", "nowhere"} {
			term := term
			jobs = append(jobs, job{"term " + term, func() *SearchRequest {
				return NewSearchRequestOptions(query.Query(termQueryOn("body", term)), 10, 0, false)
			}})
		}

		type outcome struct {
			total uint64
			rel   string
			ids   []string
			score []float64
		}
		run := func(j job) (outcome, error) {
			res, err := idx.Search(j.mk())
			if err != nil {
				return outcome{}, err
			}
			o := outcome{total: res.Total, rel: res.TotalRelation}
			for _, h := range res.Hits {
				o.ids = append(o.ids, h.ID)
				o.score = append(o.score, h.Score)
			}
			return o, nil
		}

		// alone
		want := make([]outcome, len(jobs))
		for i, j := range jobs {
			o, err := run(j)
			if err != nil {
				t.Fatalf("%s: %v", j.name, err)
			}
			want[i] = o
		}

		// all at once
		var wg sync.WaitGroup
		errs := make(chan error, 64)
		for g := 0; g < 12; g++ {
			wg.Add(1)
			go func(seed int64) {
				defer wg.Done()
				rnd := rand.New(rand.NewSource(seed))
				for it := 0; it < 150; it++ {
					i := rnd.Intn(len(jobs))
					got, err := run(jobs[i])
					if err != nil {
						errs <- fmt.Errorf("%s: %v", jobs[i].name, err)
						return
					}
					w := want[i]
					if got.total != w.total || got.rel != w.rel || len(got.ids) != len(w.ids) {
						errs <- fmt.Errorf("%s %s: total %d (%s) with %d hits, alone %d (%s) with %d",
							model, jobs[i].name, got.total, got.rel, len(got.ids), w.total, w.rel, len(w.ids))
						return
					}
					for k := range w.ids {
						if got.ids[k] != w.ids[k] || got.score[k] != w.score[k] {
							errs <- fmt.Errorf("%s %s: hit %d is %s/%v, alone %s/%v", model, jobs[i].name, k,
								got.ids[k], got.score[k], w.ids[k], w.score[k])
							return
						}
					}
				}
			}(int64(g) + 1)
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		cleanup()
	}
}
