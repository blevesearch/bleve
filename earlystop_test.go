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
	"os"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
)

// buildEarlyStopIndex indexes n documents that all match "common", so a bounded
// scan has plenty of matches left undrained.
func buildEarlyStopIndex(t *testing.T, n int) Index {
	t.Helper()

	dir, err := os.MkdirTemp("", "earlystop")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	im := mapping.NewIndexMapping()
	im.DefaultAnalyzer = "standard"
	idx, err := New(dir+"/i.bleve", im)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	batch := idx.NewBatch()
	for i := 0; i < n; i++ {
		if err := batch.Index(fmt.Sprintf("d%05d", i), map[string]interface{}{
			"body": fmt.Sprintf("common tag%d", i%10),
			"num":  float64(i),
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := idx.Batch(batch); err != nil {
		t.Fatal(err)
	}
	return idx
}

func earlyStopQuery() query.Query {
	q := query.NewTermQuery("common")
	q.SetField("body")
	return q
}

// TestEarlyStopBoundedScan is the contract for the bounded scan: with
// Score="none" and a bounded Size, the request means "any Size+From matching
// docs", so collection may stop once that many hits are in hand instead of
// draining every match.
//
// Two things must hold. The caller must still get Size hits — stopping early must
// not lose results it asked for. And Total must be reported as a lower bound
// (TotalRelation "gte"), because the scan genuinely did not count the rest; a
// caller that reads Total as exact would otherwise be silently misled.
func TestEarlyStopBoundedScan(t *testing.T) {
	const n = 5000
	idx := buildEarlyStopIndex(t, n)

	for _, size := range []int{1, 10, 100} {
		req := NewSearchRequest(earlyStopQuery())
		req.Size = size
		req.Score = ScoreNone
		res, err := idx.Search(req)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.Hits) != size {
			t.Errorf("size=%d: got %d hits, want %d — early stop dropped results the "+
				"caller asked for", size, len(res.Hits), size)
		}
		if res.TotalRelation != TotalRelationGte {
			t.Errorf("size=%d: TotalRelation=%q, want %q — Total is a lower bound once "+
				"the scan stops early, and saying otherwise misleads the caller",
				size, res.TotalRelation, TotalRelationGte)
		}
		if res.Total > uint64(n) {
			t.Errorf("size=%d: Total=%d exceeds the corpus size %d", size, res.Total, n)
		}
	}
}

// TestEarlyStopDoesNotEngageWhenUnsafe pins the preconditions. Each of these
// requests depends on documents the bounded scan would never look at, so it must
// keep draining and report an exact Total. Getting any of these wrong is a silent
// wrong-answer bug, not a slowdown:
//
//	facets       every match must be counted into the facet buckets
//	field sort   the top-k by field value can lie anywhere in the match set
//	SearchAfter  the cursor position depends on the full ordering
//	scoring      Score != "none" means order depends on scores, not arrival
func TestEarlyStopDoesNotEngageWhenUnsafe(t *testing.T) {
	const n = 2000
	idx := buildEarlyStopIndex(t, n)

	cases := []struct {
		name  string
		tweak func(*SearchRequest)
	}{
		{"facets", func(r *SearchRequest) {
			r.Score = ScoreNone
			r.AddFacet("tags", NewFacetRequest("body", 5))
		}},
		{"field-sort", func(r *SearchRequest) {
			r.Score = ScoreNone
			r.SortBy([]string{"num"})
		}},
		{"search-after", func(r *SearchRequest) {
			r.Score = ScoreNone
			r.SortBy([]string{"_id"})
			r.SearchAfter = []string{"d00010"}
		}},
		{"scoring-enabled", func(r *SearchRequest) {
			// Score defaults to full scoring; ordering depends on scores.
		}},
	}

	for _, tc := range cases {
		req := NewSearchRequest(earlyStopQuery())
		req.Size = 10
		tc.tweak(req)
		res, err := idx.Search(req)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if res.TotalRelation != TotalRelationEq {
			t.Errorf("%s: TotalRelation=%q, want %q — the bounded scan engaged on a "+
				"request whose result depends on documents it would not visit",
				tc.name, res.TotalRelation, TotalRelationEq)
		}
		if res.Total != uint64(n) {
			t.Errorf("%s: Total=%d, want the exact %d", tc.name, res.Total, n)
		}
	}
}

// TestEarlyStopHitsAreRealMatches guards the cheapest way for a bounded scan to be
// wrong: returning documents that do not match. Order is not part of the contract
// under Score="none", but membership is.
func TestEarlyStopHitsAreRealMatches(t *testing.T) {
	idx := buildEarlyStopIndex(t, 1000)

	q := query.NewTermQuery("tag3")
	q.SetField("body")
	req := NewSearchRequest(q)
	req.Size = 20
	req.Score = ScoreNone
	res, err := idx.Search(req)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Hits) != 20 {
		t.Fatalf("got %d hits, want 20", len(res.Hits))
	}
	// tag3 was indexed on every doc where i%10 == 3.
	for _, h := range res.Hits {
		var i int
		if _, err := fmt.Sscanf(h.ID, "d%05d", &i); err != nil {
			t.Fatalf("unexpected id %q: %v", h.ID, err)
		}
		if i%10 != 3 {
			t.Errorf("id %s does not match tag3 — the bounded scan returned a non-match", h.ID)
		}
	}
}
