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
	"math"
	"os"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

// TestBM25FastPathMatchesFormulaEndToEnd asserts that the §25 field-norm table
// fast path (Explain=false) produces the same BM25 score as the plain formula
// path (Explain=true, which disables the table) for a corpus spanning a wide
// range of field lengths.
//
// This is the accuracy contract for §20/§25: the norm column stores the exact
// analyzed field length, so the table is a pure speedup and never an
// approximation.  An earlier revision keyed the table on a quantized SmallFloat
// norm byte, which truncated a 3-bit mantissa and cost up to ~12% error in the
// field length (and hence in tfNorm) — this test fails loudly if any such
// quantization is reintroduced anywhere along the
// normColumn → NormColumn() → TermFieldDoc.FieldLen → scorer path.
func TestBM25FastPathMatchesFormulaEndToEnd(t *testing.T) {
	dir, err := os.MkdirTemp("", "bm25_exact_norm")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	im := mapping.NewIndexMapping()
	im.DefaultAnalyzer = "standard"
	im.ScoringModel = index.BM25Scoring
	idx, err := New(dir+"/i.bleve", im)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = idx.Close() }()

	// Field lengths 1..80 straddle several SmallFloat exponent buckets, so
	// mantissa truncation would show up here if it were still happening.
	const numDocs = 80
	b := idx.NewBatch()
	for n := 1; n <= numDocs; n++ {
		body := "target"
		for i := 1; i < n; i++ {
			body += fmt.Sprintf(" filler%d", i)
		}
		if err := b.Index(fmt.Sprintf("d%02d", n),
			map[string]interface{}{"body": body}); err != nil {
			t.Fatal(err)
		}
	}
	if err := idx.Batch(b); err != nil {
		t.Fatal(err)
	}

	run := func(explain bool) map[string]float64 {
		req := NewSearchRequest(query.NewMatchQuery("target"))
		req.Size = numDocs + 20
		req.Explain = explain
		res, err := idx.Search(req)
		if err != nil {
			t.Fatal(err)
		}
		out := make(map[string]float64, len(res.Hits))
		for _, h := range res.Hits {
			out[h.ID] = h.Score
		}
		return out
	}

	fast := run(false)
	formula := run(true)
	if len(fast) != numDocs || len(formula) != numDocs {
		t.Fatalf("expected %d hits from each path, got %d and %d",
			numDocs, len(fast), len(formula))
	}

	// Only float64 rounding may differ (the two paths group the multiplies
	// differently); anything above ~1e-9 means norm information was lost.
	const tol = 1e-9
	worst, worstID := 0.0, ""
	for id, f := range fast {
		g, ok := formula[id]
		if !ok {
			t.Fatalf("doc %s missing from the Explain=true result set", id)
		}
		rel := math.Abs(f-g) / g
		if rel > worst {
			worst, worstID = rel, id
		}
	}
	if worst > tol {
		t.Errorf("fast path diverges from the BM25 formula by %.3e at doc %s "+
			"(tolerance %.0e) — norm information lost?", worst, worstID, tol)
	}
}
