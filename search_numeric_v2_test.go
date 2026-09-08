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
	"math"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/index/scorch/mergeplan"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/query"
)

// The corpus is indexed twice, into a "v1" field of type number and a "v2"
// field of type number_v2, so every assertion below can be a comparison rather
// than a hand-written expectation.
const (
	nv2FieldV1 = "priceV1"
	nv2FieldV2 = "priceV2"
)

func nv2TestMapping(t *testing.T) mapping.IndexMapping {
	t.Helper()
	im := NewIndexMapping()

	v1 := NewNumericFieldMapping()
	v1.Name = nv2FieldV1
	v2 := NewNumberV2FieldMapping()
	v2.Name = nv2FieldV2

	dm := NewDocumentMapping()
	dm.AddFieldMappingsAt(nv2FieldV1, v1)
	dm.AddFieldMappingsAt(nv2FieldV2, v2)
	im.DefaultMapping = dm

	if err := im.Validate(); err != nil {
		t.Fatalf("mapping did not validate: %v", err)
	}
	return im
}

// nv2Corpus spans negatives, zero, fractions and duplicates.
var nv2Corpus = []float64{-1000, -42.5, -1, 0, 0.5, 1, 1, 7, 42.5, 99, 1000, 123456.75}

func nv2OpenIndex(t *testing.T, name string) (Index, func()) {
	t.Helper()
	path := fmt.Sprintf("%s/%s", t.TempDir(), name)
	idx, err := New(path, nv2TestMapping(t))
	if err != nil {
		t.Fatal(err)
	}
	return idx, func() {
		if cerr := idx.Close(); cerr != nil {
			t.Fatalf("error closing index: %v", cerr)
		}
		_ = os.RemoveAll(path)
	}
}

// nv2Index builds an index over the corpus. When batches > 1 the documents are
// spread over several batches so that the search spans multiple segments.
func nv2Index(t *testing.T, name string, batches int) (Index, func()) {
	t.Helper()
	idx, cleanup := nv2OpenIndex(t, name)

	perBatch := (len(nv2Corpus) + batches - 1) / batches
	for start := 0; start < len(nv2Corpus); start += perBatch {
		batch := idx.NewBatch()
		end := start + perBatch
		if end > len(nv2Corpus) {
			end = len(nv2Corpus)
		}
		for i := start; i < end; i++ {
			doc := map[string]interface{}{
				nv2FieldV1: nv2Corpus[i],
				nv2FieldV2: nv2Corpus[i],
			}
			if err := batch.Index(fmt.Sprintf("d%02d", i), doc); err != nil {
				cleanup()
				t.Fatal(err)
			}
		}
		if err := idx.Batch(batch); err != nil {
			cleanup()
			t.Fatal(err)
		}
	}
	return idx, cleanup
}

func nv2HitIDs(t *testing.T, idx Index, q query.Query, sorts []string) []string {
	t.Helper()
	req := NewSearchRequestOptions(q, len(nv2Corpus)+10, 0, false)
	if len(sorts) > 0 {
		req.SortBy(sorts)
	}
	res, err := idx.Search(req)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, len(res.Hits))
	for _, hit := range res.Hits {
		ids = append(ids, hit.ID)
	}
	return ids
}

func f64p(v float64) *float64 { return &v }
func boolp(v bool) *bool      { return &v }

// nv2Ranges is the shared range matrix: open ends, closed ends, every
// inclusivity combination, empty ranges and exact hits on corpus values.
type nv2Range struct {
	min, max *float64
	incMin   *bool
	incMax   *bool
}

func nv2Ranges() []nv2Range {
	var rv []nv2Range
	ends := []*float64{nil, f64p(-1000), f64p(-42.5), f64p(-1), f64p(0), f64p(0.5),
		f64p(1), f64p(42.5), f64p(99), f64p(1000), f64p(123456.75), f64p(1e9)}
	incs := []*bool{nil, boolp(true), boolp(false)}

	for _, min := range ends {
		for _, max := range ends {
			if min == nil && max == nil {
				continue // rejected by Validate on both query types
			}
			for _, incMin := range incs {
				for _, incMax := range incs {
					rv = append(rv, nv2Range{min, max, incMin, incMax})
				}
			}
		}
	}
	return rv
}

func (r nv2Range) label() string {
	fmtEnd := func(f *float64) string {
		if f == nil {
			return "nil"
		}
		return fmt.Sprintf("%g", *f)
	}
	fmtInc := func(b *bool) string {
		if b == nil {
			return "nil"
		}
		return fmt.Sprintf("%t", *b)
	}
	return fmt.Sprintf("[%s,%s] inc(%s,%s)", fmtEnd(r.min), fmtEnd(r.max),
		fmtInc(r.incMin), fmtInc(r.incMax))
}

func (r nv2Range) v1(field string) query.Query {
	q := query.NewNumericRangeInclusiveQuery(r.min, r.max, r.incMin, r.incMax)
	q.SetField(field)
	return q
}

func (r nv2Range) v2(field string) query.Query {
	q := query.NewNumericRangeV2InclusiveQuery(r.min, r.max, r.incMin, r.incMax)
	q.SetField(field)
	return q
}

// TestNumericV2MatchesNumericV1 is the load-bearing test for the whole feature:
// the same corpus, the same ranges, through both the inverted-index numeric path
// and the number_v2 section, must produce identical hit sets. An encoding or
// inclusivity mismatch shows up here and essentially nowhere else.
func TestNumericV2MatchesNumericV1(t *testing.T) {
	for _, batches := range []int{1, 4} {
		t.Run(fmt.Sprintf("batches=%d", batches), func(t *testing.T) {
			idx, cleanup := nv2Index(t, fmt.Sprintf("nv2-diff-%d", batches), batches)
			defer cleanup()

			for _, r := range nv2Ranges() {
				got := nv2HitIDs(t, idx, r.v2(nv2FieldV2), []string{"_id"})
				want := nv2HitIDs(t, idx, r.v1(nv2FieldV1), []string{"_id"})
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("range %s: number_v2 gave %v, number gave %v",
						r.label(), got, want)
				}
			}
		})
	}
}

// TestNumericV2WithDeletionsAndUpdates exercises the deleted-document path and
// then forces a merge, re-checking parity after each step.
func TestNumericV2WithDeletionsAndUpdates(t *testing.T) {
	idx, cleanup := nv2Index(t, "nv2-deletes", 3)
	defer cleanup()

	// delete a few documents and update another to a new value
	batch := idx.NewBatch()
	batch.Delete("d00") // -1000
	batch.Delete("d06") // one of the two 1s
	if err := batch.Index("d07", map[string]interface{}{
		nv2FieldV1: 5000.0,
		nv2FieldV2: 5000.0,
	}); err != nil {
		t.Fatal(err)
	}
	if err := idx.Batch(batch); err != nil {
		t.Fatal(err)
	}

	check := func(stage string) {
		for _, r := range nv2Ranges() {
			got := nv2HitIDs(t, idx, r.v2(nv2FieldV2), []string{"_id"})
			want := nv2HitIDs(t, idx, r.v1(nv2FieldV1), []string{"_id"})
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("%s, range %s: number_v2 gave %v, number gave %v",
					stage, r.label(), got, want)
			}
		}
	}

	check("after deletes")

	// the deleted documents must be gone from the v2 results too
	all := nv2HitIDs(t, idx, query.NewNumericRangeV2Query(f64p(math.Inf(-1)), f64p(math.Inf(1))), []string{"_id"})
	for _, id := range all {
		if id == "d00" || id == "d06" {
			t.Fatalf("deleted document %s still matched", id)
		}
	}

	// force everything into one segment and re-check: the merge path has its
	// own doc number remapping and doc value stream
	sc, ok := idx.(*indexImpl).i.(*scorch.Scorch)
	if !ok {
		t.Fatalf("expected a scorch index, got %T", idx.(*indexImpl).i)
	}
	if err := sc.ForceMerge(context.Background(), &mergeplan.SingleSegmentMergePlanOptions); err != nil {
		t.Fatalf("force merge: %v", err)
	}

	check("after merge")
}

// TestNumericV2SortParity checks that sorting on a number_v2 field agrees with
// sorting on a number field across sort types, modes and missing-value
// placements. This is what the doc value block exists for.
func TestNumericV2SortParity(t *testing.T) {
	idx, cleanup := nv2Index(t, "nv2-sort", 3)
	defer cleanup()

	all := query.NewMatchAllQuery()

	cases := []struct {
		name   string
		mkSort func(field string) search.SearchSort
	}{
		{"auto-asc", func(f string) search.SearchSort {
			return &search.SortField{Field: f}
		}},
		{"auto-desc", func(f string) search.SearchSort {
			return &search.SortField{Field: f, Desc: true}
		}},
		{"as-number-asc", func(f string) search.SearchSort {
			return &search.SortField{Field: f, Type: search.SortFieldAsNumber}
		}},
		{"as-number-desc", func(f string) search.SearchSort {
			return &search.SortField{Field: f, Type: search.SortFieldAsNumber, Desc: true}
		}},
		{"as-number-min", func(f string) search.SearchSort {
			return &search.SortField{Field: f, Type: search.SortFieldAsNumber, Mode: search.SortFieldMin}
		}},
		{"as-number-max", func(f string) search.SearchSort {
			return &search.SortField{Field: f, Type: search.SortFieldAsNumber, Mode: search.SortFieldMax}
		}},
		{"missing-first", func(f string) search.SearchSort {
			return &search.SortField{Field: f, Type: search.SortFieldAsNumber, Missing: search.SortFieldMissingFirst}
		}},
		{"missing-last", func(f string) search.SearchSort {
			return &search.SortField{Field: f, Type: search.SortFieldAsNumber, Missing: search.SortFieldMissingLast}
		}},
	}

	run := func(field string, s search.SearchSort) []string {
		req := NewSearchRequestOptions(all, len(nv2Corpus)+10, 0, false)
		// tie-break on _id so the comparison is deterministic
		req.Sort = search.SortOrder{s, &search.SortField{Field: "_id"}}
		res, err := idx.Search(req)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, 0, len(res.Hits))
		for _, hit := range res.Hits {
			ids = append(ids, hit.ID)
		}
		return ids
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := run(nv2FieldV2, tc.mkSort(nv2FieldV2))
			want := run(nv2FieldV1, tc.mkSort(nv2FieldV1))
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("sort %s: number_v2 gave %v, number gave %v", tc.name, got, want)
			}
			if len(got) != len(nv2Corpus) {
				t.Fatalf("sort %s: expected %d hits, got %d", tc.name, len(nv2Corpus), len(got))
			}
		})
	}
}

// TestNumericV2FacetParity checks that numeric range facets over a number_v2
// field agree with the same facets over a number field, including the missing
// and other counts.
func TestNumericV2FacetParity(t *testing.T) {
	idx, cleanup := nv2Index(t, "nv2-facet", 3)
	defer cleanup()

	buckets := []struct {
		name     string
		min, max *float64
	}{
		{"negative", nil, f64p(0)},
		{"small", f64p(0), f64p(10)},
		{"medium", f64p(10), f64p(1000)},
		{"large", f64p(1000), nil},
	}

	facetFor := func(field string) *search.FacetResult {
		req := NewSearchRequestOptions(query.NewMatchAllQuery(), 0, 0, false)
		fr := NewFacetRequest(field, 10)
		for _, b := range buckets {
			fr.AddNumericRange(b.name, b.min, b.max)
		}
		req.AddFacet("prices", fr)
		res, err := idx.Search(req)
		if err != nil {
			t.Fatal(err)
		}
		return res.Facets["prices"]
	}

	gotF, wantF := facetFor(nv2FieldV2), facetFor(nv2FieldV1)

	if gotF == nil || wantF == nil {
		t.Fatalf("missing facet result: v2=%v v1=%v", gotF, wantF)
	}
	if gotF.Total != wantF.Total || gotF.Missing != wantF.Missing || gotF.Other != wantF.Other {
		t.Fatalf("facet totals differ: v2 total=%d missing=%d other=%d, "+
			"v1 total=%d missing=%d other=%d",
			gotF.Total, gotF.Missing, gotF.Other,
			wantF.Total, wantF.Missing, wantF.Other)
	}

	counts := func(fr *search.FacetResult) map[string]int {
		rv := map[string]int{}
		for _, nr := range fr.NumericRanges {
			rv[nr.Name] = nr.Count
		}
		return rv
	}
	if got, want := counts(gotF), counts(wantF); !reflect.DeepEqual(got, want) {
		t.Fatalf("facet bucket counts differ: v2=%v v1=%v", got, want)
	}

	// sanity: the buckets must actually have counted the corpus
	var total int
	for _, c := range counts(gotF) {
		total += c
	}
	if total != len(nv2Corpus) {
		t.Fatalf("expected the buckets to cover all %d docs, counted %d",
			len(nv2Corpus), total)
	}
}

// TestNumericV2Conjunction exercises Advance, which a bare range query never
// reaches, by combining the range with another clause.
func TestNumericV2Conjunction(t *testing.T) {
	idx, cleanup := nv2Index(t, "nv2-conj", 3)
	defer cleanup()

	r := nv2Range{min: f64p(0), max: f64p(1000), incMin: boolp(true), incMax: boolp(true)}

	for _, other := range []query.Query{
		query.NewMatchAllQuery(),
		query.NewDocIDQuery([]string{"d03", "d05", "d06", "d07", "d10"}),
	} {
		v2 := query.NewConjunctionQuery([]query.Query{r.v2(nv2FieldV2), other})
		v1 := query.NewConjunctionQuery([]query.Query{r.v1(nv2FieldV1), other})

		got := nv2HitIDs(t, idx, v2, []string{"_id"})
		want := nv2HitIDs(t, idx, v1, []string{"_id"})
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("conjunction: number_v2 gave %v, number gave %v", got, want)
		}
		if len(got) == 0 {
			t.Fatal("expected the conjunction to match something")
		}

		dis2 := query.NewDisjunctionQuery([]query.Query{r.v2(nv2FieldV2), other})
		dis1 := query.NewDisjunctionQuery([]query.Query{r.v1(nv2FieldV1), other})
		got = nv2HitIDs(t, idx, dis2, []string{"_id"})
		want = nv2HitIDs(t, idx, dis1, []string{"_id"})
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("disjunction: number_v2 gave %v, number gave %v", got, want)
		}
	}
}

// TestNumericV2ConstantScore verifies that every hit scores identically, and
// that boost feeds through the scorer the way a constant-score clause should.
//
// Note that boost cannot change the score of a lone clause: the collector
// normalises by the query norm, which for a single searcher is 1/boost, so the
// two cancel and the score is exactly the constant. Boost only has a visible
// effect relative to sibling clauses, which is what the disjunction below
// checks.
func TestNumericV2ConstantScore(t *testing.T) {
	idx, cleanup := nv2Index(t, "nv2-score", 2)
	defer cleanup()

	wideRange := func(boost float64) *query.NumericRangeV2Query {
		q := query.NewNumericRangeV2Query(f64p(-1e9), f64p(1e9))
		q.SetField(nv2FieldV2)
		q.SetBoost(boost)
		return q
	}

	scores := func(q query.Query) map[string]float64 {
		req := NewSearchRequestOptions(q, len(nv2Corpus)+10, 0, false)
		res, err := idx.Search(req)
		if err != nil {
			t.Fatal(err)
		}
		rv := map[string]float64{}
		for _, hit := range res.Hits {
			rv[hit.ID] = hit.Score
		}
		return rv
	}

	// every hit of a lone constant-score clause scores the same
	base := scores(wideRange(1))
	if len(base) != len(nv2Corpus) {
		t.Fatalf("expected %d hits, got %d", len(nv2Corpus), len(base))
	}
	var first float64
	for _, s := range base {
		if first == 0 {
			first = s
		}
		if s != first {
			t.Fatalf("expected a constant score, got %v", base)
		}
	}

	// and boost cancels against the query norm for that lone clause
	boostedAlone := scores(wideRange(7))
	for id, s := range boostedAlone {
		if s != base[id] {
			t.Fatalf("boost changed a lone clause's score for %s: %v vs %v",
				id, s, base[id])
		}
	}

	// the scorer's weight, which drives that normalisation, does scale
	if got, want := wideRange(3).Boost(), 3.0; got != want {
		t.Fatalf("boost not retained on the query: got %v, want %v", got, want)
	}

	// relative to a sibling clause, boost does move the score: two disjoint
	// ranges in a disjunction, the negative half boosted much harder
	negatives := query.NewNumericRangeV2InclusiveQuery(f64p(-1e9), f64p(0), boolp(true), boolp(false))
	negatives.SetField(nv2FieldV2)
	negatives.SetBoost(100)
	positives := query.NewNumericRangeV2InclusiveQuery(f64p(0), f64p(1e9), boolp(true), boolp(true))
	positives.SetField(nv2FieldV2)
	positives.SetBoost(1)

	got := scores(query.NewDisjunctionQuery([]query.Query{negatives, positives}))

	// d00 = -1000 and d01 = -42.5 are in the boosted half; d07 = 7 is not
	for _, negID := range []string{"d00", "d01"} {
		if got[negID] <= got["d07"] {
			t.Fatalf("expected the boosted clause to score higher: %s=%v vs d07=%v",
				negID, got[negID], got["d07"])
		}
	}
}

// TestNumericV2QueryParsing checks the range_v2 discriminator, and in
// particular that a plain min/max query still parses as a v1 NumericRangeQuery.
func TestNumericV2QueryParsing(t *testing.T) {
	v2JSON := []byte(`{"field":"price","range_v2":{"min":10,"max":100,` +
		`"inclusive_min":true,"inclusive_max":false}}`)
	q, err := query.ParseQuery(v2JSON)
	if err != nil {
		t.Fatal(err)
	}
	nq, ok := q.(*query.NumericRangeV2Query)
	if !ok {
		t.Fatalf("expected a NumericRangeV2Query, got %T", q)
	}
	if nq.RangeV2.Min == nil || *nq.RangeV2.Min != 10 ||
		nq.RangeV2.Max == nil || *nq.RangeV2.Max != 100 {
		t.Fatalf("range not parsed: %+v", nq.RangeV2)
	}
	if nq.RangeV2.InclusiveMin == nil || !*nq.RangeV2.InclusiveMin ||
		nq.RangeV2.InclusiveMax == nil || *nq.RangeV2.InclusiveMax {
		t.Fatalf("inclusivity not parsed: %+v", nq.RangeV2)
	}
	if nq.Field() != "price" {
		t.Fatalf("field not parsed: %q", nq.Field())
	}

	// the v1 form must be unaffected
	v1JSON := []byte(`{"field":"price","min":10,"max":100}`)
	q, err = query.ParseQuery(v1JSON)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q.(*query.NumericRangeQuery); !ok {
		t.Fatalf("expected a NumericRangeQuery for the v1 form, got %T", q)
	}
}

// TestNumericV2Validation covers the query and mapping rejections.
func TestNumericV2Validation(t *testing.T) {
	if err := query.NewNumericRangeV2Query(nil, nil).Validate(); err == nil {
		t.Fatal("expected an unbounded range to be rejected")
	}
	nan := math.NaN()
	if err := query.NewNumericRangeV2Query(&nan, nil).Validate(); err == nil {
		t.Fatal("expected a NaN minimum to be rejected")
	}
	if err := query.NewNumericRangeV2Query(nil, &nan).Validate(); err == nil {
		t.Fatal("expected a NaN maximum to be rejected")
	}

	// IncludeInAll must be rejected at index-definition time
	im := NewIndexMapping()
	fm := NewNumberV2FieldMapping()
	fm.Name = nv2FieldV2
	fm.IncludeInAll = true
	dm := NewDocumentMapping()
	dm.AddFieldMappingsAt(nv2FieldV2, fm)
	im.DefaultMapping = dm
	if err := im.Validate(); err == nil {
		t.Fatal("expected IncludeInAll on a number_v2 field to be rejected")
	}
}

// TestNumericV2MultiValued indexes an array of numbers and checks that a
// document is returned once, not once per matching value.
func TestNumericV2MultiValued(t *testing.T) {
	idx, cleanup := nv2OpenIndex(t, "nv2-multi")
	defer cleanup()

	docs := map[string][]float64{
		"a": {1, 5, 9},
		"b": {2},
		"c": {100, 200},
	}
	batch := idx.NewBatch()
	for id, vals := range docs {
		if err := batch.Index(id, map[string]interface{}{
			nv2FieldV1: vals,
			nv2FieldV2: vals,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := idx.Batch(batch); err != nil {
		t.Fatal(err)
	}

	// a range covering several of a's values must return a exactly once
	r := nv2Range{min: f64p(0), max: f64p(10), incMin: boolp(true), incMax: boolp(true)}
	got := nv2HitIDs(t, idx, r.v2(nv2FieldV2), []string{"_id"})
	want := nv2HitIDs(t, idx, r.v1(nv2FieldV1), []string{"_id"})
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("multi-valued: number_v2 gave %v, number gave %v", got, want)
	}
	if len(got) != 2 {
		t.Fatalf("expected documents a and b, got %v", got)
	}
}
