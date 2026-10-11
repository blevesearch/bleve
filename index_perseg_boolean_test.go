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

func flagQuery(b bool) query.Query {
	q := query.NewBoolFieldQuery(b)
	q.SetField("flag")
	return q
}

func booleanOf(must, should, mustNot []query.Query, minShould float64) *query.BooleanQuery {
	bq := query.NewBooleanQuery(must, should, mustNot)
	if minShould > 0 {
		bq.SetMinShould(minShould)
	}
	return bq
}

func queryString(s string) query.Query {
	return query.NewQueryStringQuery(s)
}

type shapeCase struct {
	name string
	q    func() query.Query
	// whether the per segment path is expected to serve it
	perSegment bool
	// the regular path leaves the score of an optional clause out of the docs
	// it matches, here and there, when a boolean with such a clause is nested in
	// a conjunction (it shows in its own explanation: a doc that has the
	// clause's term has no component for it). So its scores are not a reference;
	// the docs are, and no score may be lower than the regular path's.
	oldDropsOptionalScore bool
}

var booleanShapeCases = []shapeCase{
	// boolean fields are terms
	{"bool field", func() query.Query { return flagQuery(true) }, true, false},
	{"bool field false", func() query.Query { return flagQuery(false) }, true, false},
	{"and of a bool field", func() query.Query {
		return query.NewConjunctionQuery([]query.Query{flagQuery(true), termQueryOn("body", "common")})
	}, true, false},
	{"or of a bool field", func() query.Query {
		return query.NewDisjunctionQuery([]query.Query{flagQuery(true), termQueryOn("body", "rare"), termQueryOn("body", "five")})
	}, true, false},

	// pure boolean shapes are conjunctions and disjunctions
	{"bool: must only", func() query.Query { return booleanOf(terms("common", "even"), nil, nil, 0) }, true, false},
	{"bool: should only", func() query.Query { return booleanOf(nil, terms("even", "five", "mid"), nil, 0) }, true, false},
	{"bool: should only, minimum 2", func() query.Query { return booleanOf(nil, terms("even", "five", "mid"), nil, 2) }, true, false},

	// and the mixed ones
	{"bool: must and must not", func() query.Query { return booleanOf(terms("common", "even"), nil, terms("five"), 0) }, true, false},
	{"bool: should and must not", func() query.Query { return booleanOf(nil, terms("even", "mid"), terms("five", "rare"), 0) }, true, false},
	{"bool: must and should", func() query.Query { return booleanOf(terms("even"), terms("five", "mid"), nil, 0) }, true, false},
	{"bool: must and should, should required", func() query.Query { return booleanOf(terms("even"), terms("five", "mid"), nil, 1) }, true, false},
	{"bool: must and should, 2 required", func() query.Query { return booleanOf(terms("common"), terms("five", "mid", "even"), nil, 2) }, true, false},
	{"bool: all three", func() query.Query { return booleanOf(terms("common"), terms("five", "mid"), terms("rare"), 0) }, true, false},
	{"bool: a term that is nowhere", func() query.Query { return booleanOf(terms("common"), terms("nowhere"), terms("five"), 0) }, true, false},
	{"bool: must is nowhere", func() query.Query { return booleanOf(terms("nowhere"), terms("five"), nil, 0) }, true, false},
	{"bool: nested", func() query.Query {
		return booleanOf([]query.Query{booleanOf(terms("even"), terms("five"), nil, 0), flagQuery(true)}, terms("mid"), terms("rare"), 0)
	}, true, true},
	{"bool: in a conjunction", func() query.Query {
		return query.NewConjunctionQuery([]query.Query{booleanOf(terms("even"), nil, terms("five"), 0), termQueryOn("body", "common")})
	}, true, false},

	// query strings are parsed into these
	{"query string: a term", func() query.Query { return queryString("body:common") }, true, false},
	{"query string: should terms", func() query.Query { return queryString("body:even body:five body:mid") }, true, false},
	{"query string: required terms", func() query.Query { return queryString("+body:even +body:five") }, true, false},
	{"query string: required and optional", func() query.Query { return queryString("+body:even body:five body:mid") }, true, false},
	{"query string: with an exclusion", func() query.Query { return queryString("+body:common -body:five") }, true, false},
	{"query string: should and an exclusion", func() query.Query { return queryString("body:even body:mid -body:five") }, true, false},
	{"query string: the default field", func() query.Query { return queryString("common") }, true, false},
	{"query string: boost", func() query.Query { return queryString("+body:even^2 body:five") }, true, false},

	// not these
	{"bool: with a filter", func() query.Query {
		bq := booleanOf(terms("common"), nil, nil, 0)
		bq.Filter = termQueryOn("body", "even")
		return bq
	}, false, false},
	{"bool: only a must not", func() query.Query { return booleanOf(nil, nil, terms("five"), 0) }, false, false},
	{"bool: with a prefix", func() query.Query {
		return booleanOf([]query.Query{termQueryOn("body", "common"), query.NewPrefixQuery("eve")}, nil, nil, 0)
	}, false, false},
	{"query string: a phrase", func() query.Query { return queryString(`body:"common even"`) }, false, false},
	{"query string: a prefix", func() query.Query { return queryString("body:comm*") }, false, false},
	{"query string: fuzzy", func() query.Query { return queryString("body:common~1") }, false, false},
	{"query string: a regexp", func() query.Query { return queryString("body:/com.*/") }, false, false},
}

func TestPerSegmentBooleanShapesMatchRegularSearch(t *testing.T) {
	for _, model := range []string{index.DefaultScoringModel, index.BM25Scoring} {
		idx, cleanup := perSegmentTestIndex(t, model, nil)

		for _, sc := range booleanShapeCases {
			for _, sz := range []struct{ size, from int }{{10, 0}, {1, 0}, {7, 3}, {5000, 0}, {0, 0}} {
				what := fmt.Sprintf("%s: %s size=%d from=%d", model, sc.name, sz.size, sz.from)
				mk := func() *SearchRequest { return NewSearchRequestOptions(sc.q(), sz.size, sz.from, false) }

				perSegmentSearchEnabled.Store(false)
				old, err := idx.Search(mk())
				perSegmentSearchEnabled.Store(true)
				if err != nil {
					t.Fatalf("%s: regular: %v", what, err)
				}

				ps := perSegmentSearches.Load()
				got, err := idx.Search(mk())
				if err != nil {
					t.Fatalf("%s: %v", what, err)
				}
				took := perSegmentSearches.Load() == ps+1
				if took != sc.perSegment {
					t.Fatalf("%s: per segment path taken: %v, want %v", what, took, sc.perSegment)
				}
				if sc.oldDropsOptionalScore {
					compareIgnoringOptionalScores(t, what, old, got)
					continue
				}
				compareCompositeResults(t, what, old, got)
			}
		}
		cleanup()
	}
}

// and without scores
func TestPerSegmentBooleanShapesWithoutScores(t *testing.T) {
	idx, cleanup := perSegmentTestIndex(t, index.DefaultScoringModel, nil)
	defer cleanup()

	for _, sc := range booleanShapeCases {
		if !sc.perSegment {
			continue
		}
		for _, sz := range []struct{ size, from int }{{10, 0}, {7, 5}, {5000, 0}, {0, 0}} {
			what := fmt.Sprintf("%s size=%d from=%d", sc.name, sz.size, sz.from)

			perSegmentSearchEnabled.Store(false)
			truth, err := idx.Search(NewSearchRequestOptions(sc.q(), 1, 0, false))
			req := NewSearchRequestOptions(sc.q(), sz.size, sz.from, false)
			req.Score = ScoreNone
			old, err2 := idx.Search(req)
			perSegmentSearchEnabled.Store(true)
			if err != nil || err2 != nil {
				t.Fatalf("%s: %v %v", what, err, err2)
			}

			req = NewSearchRequestOptions(sc.q(), sz.size, sz.from, false)
			req.Score = ScoreNone
			got, err := idx.Search(req)
			if err != nil {
				t.Fatalf("%s: %v", what, err)
			}
			if got.TotalRelation == TotalRelationEq && got.Total != truth.Total {
				t.Fatalf("%s: total %d claimed exact, want %d", what, got.Total, truth.Total)
			}
			if got.TotalRelation == TotalRelationGte && (got.Total > truth.Total || got.Total < uint64(len(got.Hits))) {
				t.Fatalf("%s: lower bound %d out of range, real total %d", what, got.Total, truth.Total)
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

// compareIgnoringOptionalScores compares the results of a search for which the
// regular path's scores are not a reference (see shapeCase): the same total,
// the same hits where they can be told apart, and no score lower than the
// regular path's.
func compareIgnoringOptionalScores(t *testing.T, what string, old, got *SearchResult) {
	t.Helper()
	if got.TotalRelation == TotalRelationEq && got.Total != old.Total {
		t.Fatalf("%s: total %d, want %d", what, got.Total, old.Total)
	}
	if len(got.Hits) > len(old.Hits) {
		t.Fatalf("%s: %d hits, regular has %d", what, len(got.Hits), len(old.Hits))
	}
	// when everything is asked for, the sets of docs have to be the same, and no
	// doc scores less
	if uint64(len(old.Hits)) == old.Total {
		if len(got.Hits) != len(old.Hits) {
			t.Fatalf("%s: %d hits, want %d", what, len(got.Hits), len(old.Hits))
		}
		oldScores := map[string]float64{}
		for _, h := range old.Hits {
			oldScores[h.ID] = h.Score
		}
		for _, h := range got.Hits {
			s, ok := oldScores[h.ID]
			if !ok {
				t.Fatalf("%s: doc %s is not a hit of the regular path", what, h.ID)
			}
			if h.Score < s && !sameScore(h.Score, s) {
				t.Fatalf("%s: doc %s scores %v, below the regular path's %v", what, h.ID, h.Score, s)
			}
		}
	}
}
