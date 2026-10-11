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
	"testing"

	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

func terms(names ...string) []query.Query {
	rv := make([]query.Query, len(names))
	for i, n := range names {
		rv[i] = termQueryOn("body", n)
	}
	return rv
}

func disjunctionOf(min float64, names ...string) query.Query {
	dq := query.NewDisjunctionQuery(terms(names...))
	dq.SetMin(min)
	return dq
}

func matchBody(text string, op query.MatchQueryOperator) query.Query {
	mq := query.NewMatchQuery(text)
	mq.SetField("body")
	mq.SetOperator(op)
	return mq
}

type compositeQueryCase struct {
	name string
	q    func() query.Query
}

var compositeQueryCases = []compositeQueryCase{
	{"or of 3 terms", func() query.Query { return disjunctionOf(1, "common", "even", "five") }},
	{"or of rare terms", func() query.Query { return disjunctionOf(1, "rare", "mid", "alpha") }},
	{"or with a term that is nowhere", func() query.Query { return disjunctionOf(1, "common", "nowhere", "five") }},
	{"or of 2 terms, minimum 2", func() query.Query { return disjunctionOf(2, "even", "five", "mid") }},
	{"and of 2 terms", func() query.Query { return query.NewConjunctionQuery(terms("common", "even")) }},
	{"and of 3 terms", func() query.Query { return query.NewConjunctionQuery(terms("even", "five", "mid")) }},
	{"and with a rare term", func() query.Query { return query.NewConjunctionQuery(terms("common", "rare")) }},
	{"and with a term that is nowhere", func() query.Query { return query.NewConjunctionQuery(terms("common", "nowhere")) }},
	{"nested: and of (or) and a term", func() query.Query {
		return query.NewConjunctionQuery([]query.Query{disjunctionOf(1, "even", "five"), termQueryOn("body", "common")})
	}},
	{"nested: or of (and) and a term", func() query.Query {
		return query.NewDisjunctionQuery([]query.Query{query.NewConjunctionQuery(terms("even", "five")), termQueryOn("body", "mid")})
	}},
	{"match, or", func() query.Query { return matchBody("common even five", query.MatchQueryOperatorOr) }},
	{"match, and", func() query.Query { return matchBody("common even five", query.MatchQueryOperatorAnd) }},
	{"match of one term", func() query.Query { return matchBody("common", query.MatchQueryOperatorOr) }},
	{"match of nothing", func() query.Query { return matchBody("", query.MatchQueryOperatorOr) }},
}

// compareCompositeResults is compareSearchResults for results of a search that
// may have been pruned: then the total is a lower bound (and says so), where the
// regular path counts every match.
func compareCompositeResults(t *testing.T, what string, old, got *SearchResult) {
	t.Helper()
	switch got.TotalRelation {
	case TotalRelationEq:
		if got.Total != old.Total {
			t.Fatalf("%s: total %d (exact), want %d", what, got.Total, old.Total)
		}
	case TotalRelationGte:
		if got.Total > old.Total {
			t.Fatalf("%s: total %d (a lower bound) above the real %d", what, got.Total, old.Total)
		}
		if got.Total < uint64(len(got.Hits)) {
			t.Fatalf("%s: total %d (a lower bound) below the %d hits", what, got.Total, len(got.Hits))
		}
	default:
		t.Fatalf("%s: total relation %q", what, got.TotalRelation)
	}
	if len(old.Hits) != len(got.Hits) {
		t.Fatalf("%s: %d hits, want %d", what, len(got.Hits), len(old.Hits))
	}
	if old.Total > 0 && len(got.Hits) > 0 && !sameScore(old.MaxScore, got.MaxScore) {
		t.Fatalf("%s: max score %v, want %v", what, got.MaxScore, old.MaxScore)
	}
	for i := range old.Hits {
		o, g := old.Hits[i], got.Hits[i]
		if !sameScore(o.Score, g.Score) {
			t.Fatalf("%s: hit %d scores %v, want %v", what, i, g.Score, o.Score)
		}
		nearNeighbor := (i > 0 && sameScore(o.Score, old.Hits[i-1].Score)) ||
			(i+1 < len(old.Hits) && sameScore(o.Score, old.Hits[i+1].Score)) ||
			i+1 == len(old.Hits)
		if o.ID != g.ID && !nearNeighbor {
			t.Fatalf("%s: hit %d is %s, want %s", what, i, g.ID, o.ID)
		}
	}
}

func TestPerSegmentCompositesMatchRegularSearch(t *testing.T) {
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		idx, cleanup := perSegmentTestIndex(t, model, nil)

		for _, qc := range compositeQueryCases {
			for _, sz := range []struct{ size, from int }{{10, 0}, {1, 0}, {5, 3}, {100, 0}, {5000, 0}, {0, 0}} {
				what := fmt.Sprintf("%s: %s size=%d from=%d", model, qc.name, sz.size, sz.from)
				mk := func() *SearchRequest { return NewSearchRequestOptions(qc.q(), sz.size, sz.from, false) }

				perSegmentSearchEnabled.Store(false)
				old, err := idx.Search(mk())
				perSegmentSearchEnabled.Store(true)
				if err != nil {
					t.Fatalf("%s: regular: %v", what, err)
				}

				ps, tn := perSegmentSearches.Load(), topNCollectorsBuilt.Load()
				got, err := idx.Search(mk())
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				// the empty match is a match none searcher: not a per segment one
				if qc.name != "match of nothing" && (perSegmentSearches.Load() != ps+1 || topNCollectorsBuilt.Load() != tn) {
					t.Fatalf("%s: the per segment path was not taken", what)
				}
				compareCompositeResults(t, what, old, got)
				if sz.size == 0 && got.TotalRelation != TotalRelationEq {
					t.Fatalf("%s: a count query has to be exact", what)
				}
			}
		}
		cleanup()
	}
}

// without scores: the first matches in doc order; the total is exact for a
// count, and otherwise exact or a lower bound that says it is one
func TestPerSegmentCompositesWithoutScores(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()

	for _, qc := range compositeQueryCases {
		if qc.name == "match of nothing" {
			continue
		}
		for _, sz := range []struct{ size, from int }{{10, 0}, {1, 0}, {7, 5}, {5000, 0}, {0, 0}} {
			what := fmt.Sprintf("%s size=%d from=%d", qc.name, sz.size, sz.from)

			// the exact count, from the regular path
			perSegmentSearchEnabled.Store(false)
			truth, err := idx.Search(NewSearchRequestOptions(qc.q(), 1, 0, false))
			// and the regular path's early stop, which has the first matches in doc order
			req := NewSearchRequestOptions(qc.q(), sz.size, sz.from, false)
			req.Score = ScoreNone
			old, err2 := idx.Search(req)
			perSegmentSearchEnabled.Store(true)
			if err != nil || err2 != nil {
				t.Fatalf("%s: %v %v", what, err, err2)
			}

			req = NewSearchRequestOptions(qc.q(), sz.size, sz.from, false)
			req.Score = ScoreNone
			ps := perSegmentSearches.Load()
			got, err := idx.Search(req)
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
			if perSegmentSearches.Load() != ps+1 {
				t.Fatalf("%s: the per segment path was not taken", what)
			}
			// stops once it has the hits, unless it's a count; and says so
			switch {
			case got.TotalRelation == TotalRelationEq:
				if got.Total != truth.Total {
					t.Fatalf("%s: total %d claimed exact, want %d", what, got.Total, truth.Total)
				}
			case sz.size == 0:
				t.Fatalf("%s: a count has to be exact, got %d (%v)", what, got.Total, got.TotalRelation)
			default:
				if got.Total > truth.Total || got.Total < uint64(len(got.Hits)) {
					t.Fatalf("%s: lower bound %d out of [%d, %d]", what, got.Total, len(got.Hits), truth.Total)
				}
			}
			if len(got.Hits) != len(old.Hits) {
				t.Fatalf("%s: %d hits, want %d", what, len(got.Hits), len(old.Hits))
			}
			for i := range old.Hits {
				if got.Hits[i].ID != old.Hits[i].ID {
					t.Fatalf("%s: hit %d is %s, want %s", what, i, got.Hits[i].ID, old.Hits[i].ID)
				}
			}
		}
	}
}

// shapes the composite path doesn't cover must not take it, and a mix of
// clauses that can and cannot be per segment ones must still answer right
func TestPerSegmentCompositesFallBack(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()

	mixed := func() query.Query {
		// a prefix clause is not a per segment searcher
		return query.NewConjunctionQuery([]query.Query{termQueryOn("body", "common"), query.NewPrefixQuery("eve")})
	}
	fuzzyMatch := func() query.Query {
		mq := query.NewMatchQuery("common even")
		mq.SetField("body")
		mq.SetFuzziness(1)
		return mq
	}
	for name, q := range map[string]func() query.Query{"mixed": mixed, "fuzzy match": fuzzyMatch,
		"or with a phrase": func() query.Query {
			return query.NewDisjunctionQuery([]query.Query{termQueryOn("body", "common"), query.NewMatchPhraseQuery("even five")})
		}} {
		ps := perSegmentSearches.Load()
		res, err := idx.Search(NewSearchRequest(q()))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if res.Total == 0 {
			t.Fatalf("%s: no hits", name)
		}
		if perSegmentSearches.Load() != ps {
			t.Fatalf("%s: went through the per segment path", name)
		}
	}
}
