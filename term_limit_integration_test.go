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
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
)

// disjunctionOfTerms builds a disjunction of n distinct term queries over the
// "name" field. Each term query becomes one leaf term searcher at construction
// time, regardless of whether it matches any document.
func disjunctionOfTerms(prefix string, n int) query.Query {
	dq := NewDisjunctionQuery()
	for i := 0; i < n; i++ {
		tq := NewTermQuery(fmt.Sprintf("%s%d", prefix, i))
		tq.SetField("name")
		dq.AddQuery(tq)
	}
	return dq
}

// conjunctionOfDisjunctions builds a conjunction of `groups` disjunctions, each
// containing `per` distinct term queries -> groups*per leaf term searchers, but
// no single fan-out node has more than `per` children.
func conjunctionOfDisjunctions(groups, per int) query.Query {
	cq := NewConjunctionQuery()
	for g := 0; g < groups; g++ {
		cq.AddQuery(disjunctionOfTerms(fmt.Sprintf("g%d_", g), per))
	}
	return cq
}

// TestBleveMaxTermSearchers exercises the full bleveMaxTerms wiring end-to-end
// through SearchInContext: the per-search counter is installed from the ctx
// limit, every leaf term searcher is counted, and construction fails once the
// running total exceeds the limit. This complements TestRecordTermSearcher
// (which only covers the counter primitive) by validating that the counter is
// actually installed and consulted along the real search path.
func TestBleveMaxTermSearchers(t *testing.T) {
	idx, err := NewMemOnly(NewIndexMapping())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cerr := idx.Close(); cerr != nil {
			t.Fatal(cerr)
		}
	}()

	// A couple of real documents so the term searchers open real readers.
	if err := idx.Index("doc0", map[string]interface{}{"name": "alpha beta gamma"}); err != nil {
		t.Fatal(err)
	}
	if err := idx.Index("doc1", map[string]interface{}{"name": "delta epsilon zeta"}); err != nil {
		t.Fatal(err)
	}

	runSearch := func(q query.Query, limit int) error {
		ctx := context.WithValue(context.Background(), search.MaxTermSearchersKey, limit)
		_, serr := idx.SearchInContext(ctx, NewSearchRequest(q))
		return serr
	}

	t.Run("disabled when limit unset (0)", func(t *testing.T) {
		if err := runSearch(disjunctionOfTerms("t", 6), 0); err != nil {
			t.Fatalf("limit 0 should disable the check, got: %v", err)
		}
	})

	t.Run("disabled when limit is negative", func(t *testing.T) {
		// A non-positive limit (as produced upstream when a request explicitly
		// disables bleveMaxTerms) must be treated as "no limit", not enforced.
		if err := runSearch(disjunctionOfTerms("t", 6), -1); err != nil {
			t.Fatalf("negative limit should disable the check, got: %v", err)
		}
	})

	t.Run("under the limit succeeds", func(t *testing.T) {
		// 6 leaf term searchers, limit 100.
		if err := runSearch(disjunctionOfTerms("t", 6), 100); err != nil {
			t.Fatalf("6 term searchers under limit 100 should succeed, got: %v", err)
		}
	})

	t.Run("exactly at the limit succeeds", func(t *testing.T) {
		// 6 leaf term searchers, limit 6: 6 is not > 6, so it is allowed.
		if err := runSearch(disjunctionOfTerms("t", 6), 6); err != nil {
			t.Fatalf("6 term searchers at limit 6 should succeed, got: %v", err)
		}
	})

	t.Run("over the limit fails with TooManyTermSearchers", func(t *testing.T) {
		// 6 leaf term searchers, limit 5 -> trips at the 6th.
		err := runSearch(disjunctionOfTerms("t", 6), 5)
		if err == nil {
			t.Fatal("6 term searchers over limit 5 should fail, got nil")
		}
		if !strings.Contains(err.Error(), "TooManyTermSearchers") {
			t.Fatalf("expected a TooManyTermSearchers error, got: %v", err)
		}
		if !strings.Contains(err.Error(), "set to 5") {
			t.Fatalf("error should report the effective limit (5), got: %v", err)
		}
	})

	t.Run("counts globally across the whole query tree", func(t *testing.T) {
		// 2 disjunctions * 4 terms = 8 leaf term searchers, but no single node
		// has more than 4 children. With DisjunctionMaxClauseCount unset this
		// can only be tripped by the global bleveMaxTerms count, proving the two
		// checks are distinct: bleveMaxClauseCount is per-node, bleveMaxTerms is
		// the sum across the tree.
		err := runSearch(conjunctionOfDisjunctions(2, 4), 5)
		if err == nil {
			t.Fatal("8 term searchers over limit 5 should fail, got nil")
		}
		if !strings.Contains(err.Error(), "TooManyTermSearchers") {
			t.Fatalf("expected a TooManyTermSearchers error, got: %v", err)
		}
		if strings.Contains(err.Error(), "TooManyClauses") {
			t.Fatalf("should be the global term-count check, not the per-node clause check: %v", err)
		}

		// Same query, limit 8 -> allowed (8 is not > 8).
		if err := runSearch(conjunctionOfDisjunctions(2, 4), 8); err != nil {
			t.Fatalf("8 term searchers at limit 8 should succeed, got: %v", err)
		}
	})
}
