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
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// The explanation of a doc is worked out again from the postings, by the same
// arithmetic the search scored with. For every match of every shape, the root of
// its explanation is the score it was found with, to the bit; and asking only
// whether a doc is a match (what a clause that excludes docs is asked) says the
// same as asking for the whole explanation, for every doc, matching or not.
func TestPerSegmentExplanationsAgreeWithTheSearch(t *testing.T) {
	fx := newPerSegFixture(t, perSegFixtureOpts{segments: 3, docsPerSeg: 3000, seed: 11})
	for _, model := range []string{"", index.BM25Scoring} {
		opts := search.SearcherOptions{}
		term := func(s string) *PerSegmentTermSearcher { return fx.termSearcher(s, true, model) }
		mustBuild := func(s *PerSegmentBooleanSearcher, err error) *PerSegmentBooleanSearcher {
			t.Helper()
			if err != nil {
				t.Fatal(err)
			}
			return s
		}
		shapes := []struct {
			name string
			mk   func() perSegChild
		}{
			{"term", func() perSegChild { return term("charlie") }},
			{"and", func() perSegChild {
				return fx.conjunction([]search.PerSegmentSearcher{term("bravo"), term("charlie")}, opts)
			}},
			{"or", func() perSegChild {
				return fx.disjunction([]search.PerSegmentSearcher{term("delta"), term("echo"), term("charlie")}, 1, opts)
			}},
			{"or min 2", func() perSegChild {
				return fx.disjunction([]search.PerSegmentSearcher{term("bravo"), term("charlie"), term("delta")}, 2, opts)
			}},
			{"nested", func() perSegChild {
				inner := fx.conjunction([]search.PerSegmentSearcher{term("charlie"), term("delta")}, opts)
				mid := fx.disjunction([]search.PerSegmentSearcher{term("echo"), inner}, 1, opts)
				return fx.conjunction([]search.PerSegmentSearcher{term("bravo"), mid}, opts)
			}},
			{"must not", func() perSegChild {
				return mustBuild(NewPerSegmentBooleanSearcher(term("bravo"), nil, term("charlie"), opts))
			}},
			{"must should not", func() perSegChild {
				return mustBuild(NewPerSegmentBooleanSearcher(term("bravo"), term("delta"), term("charlie"), opts))
			}},
			{"should not", func() perSegChild {
				should := fx.disjunction([]search.PerSegmentSearcher{term("delta"), term("echo")}, 1, opts)
				return mustBuild(NewPerSegmentBooleanSearcher(nil, should, term("charlie"), opts))
			}},
		}
		for _, sh := range shapes {
			t.Run(sh.name+"/"+model, func(t *testing.T) {
				s := sh.mk()
				defer s.Close()
				matched := map[uint64]float32{}
				for {
					m, ok, err := s.NextMatch()
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						break
					}
					matched[m.Doc] = m.Score
				}
				if len(matched) == 0 {
					t.Fatal("the shape matches nothing: the test would prove nothing")
				}
				for seg := 0; seg < s.numSegments(); seg++ {
					for doc := uint64(seg) * 3000; doc < uint64(seg+1)*3000; doc++ {
						full, isMatch, err := s.explain(seg, doc, true)
						if err != nil {
							t.Fatal(err)
						}
						only, isMatchOnly, err := s.explain(seg, doc, false)
						if err != nil {
							t.Fatal(err)
						}
						if only != nil {
							t.Fatalf("doc %d: asking only for a match built an explanation", doc)
						}
						score, found := matched[doc]
						if isMatch != found || isMatchOnly != found {
							t.Fatalf("doc %d: found by the search %v, explained as a match %v, as a match alone %v",
								doc, found, isMatch, isMatchOnly)
						}
						if found && full.Value != float64(score) {
							t.Fatalf("doc %d: explanation says %v, found with %v", doc, full.Value, score)
						}
					}
				}
			})
		}
	}
}
