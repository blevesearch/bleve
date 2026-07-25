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

// Legacy segment-format compatibility.
//
// scorch pins one segment plugin per index, read from the bolt meta bucket at
// open time ("all operations for this scorch will use this type/version", see
// persister.go), so an index built by an older bleve keeps being read *and
// written* through its original zapx plugin after a binary upgrade. Its segments
// are never silently rewritten in the new format. A deployment that upgrades to
// this branch therefore runs the new search code against old segment files
// indefinitely, and that combination has to produce the same answers.
//
// This branch adds two sections that no earlier version has — §20 (the norm
// column, which supplies index.TermFieldDoc.FieldLen) and §14 (the MaxTFNorm
// sidecar, which supplies the WAND per-term bound) — and it adds fast paths that
// consume them. Every such fast path needs a fallback that is not merely
// non-crashing but numerically and semantically identical, and it is very easy
// for a fallback to look plausible while being wrong: two bugs found by exactly
// these tests were "absent bound read as a bound of zero, pruning everything"
// and "the §20-fed scoring fast path multiplied by an uninitialised weight,
// returning score 0 for every top-level term query". Neither crashed, and
// neither showed up on a current-format index.
//
// So the strategy here is differential, not example-based: build one corpus at
// every supported zapx version, run identical queries, and require the older
// versions to agree with v18. Anything a fast path is supposed to accelerate
// must give the same answer when it is unavailable.
package bleve

import (
	"context"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/index/scorch/mergeplan"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search/query"
	index "github.com/blevesearch/bleve_index_api"
)

// legacySegmentVersions are the zapx versions scorch registers, oldest first.
// legacyCurrentVersion is the one this branch writes by default and the
// reference every other version is compared against.
var (
	legacySegmentVersions = []int{11, 12, 13, 14, 15, 16, 17}
	legacyCurrentVersion  = 18
)

// legacyCorpusDocs is small enough to keep the suite quick at 8 indexes per
// corpus, and large enough that idf, length normalisation and multi-segment
// merging all matter.
const (
	legacyCorpusBatches  = 4
	legacyCorpusPerBatch = 300
)

// legacyMapping exercises the field kinds whose on-disk representation differs
// across versions: an analyzed text field with term vectors (locations, needed
// for phrase queries and highlighting), a second analyzed field (multi-field
// scoring and per-field norms), a keyword field with doc values (facets and
// sorting), a numeric field and a date field (range queries), and stored fields
// (document retrieval and highlighting).
func legacyMapping() mapping.IndexMapping {
	body := mapping.NewTextFieldMapping()
	body.Analyzer = "en"
	body.Store = true
	body.IncludeTermVectors = true

	title := mapping.NewTextFieldMapping()
	title.Analyzer = "standard"
	title.Store = true
	title.IncludeTermVectors = true

	cat := mapping.NewKeywordFieldMapping()
	cat.Store = true
	cat.DocValues = true
	cat.IncludeTermVectors = false

	num := mapping.NewNumericFieldMapping()
	num.Store = true
	num.DocValues = true

	when := mapping.NewDateTimeFieldMapping()
	when.Store = true
	when.DocValues = true

	doc := mapping.NewDocumentMapping()
	doc.AddFieldMappingsAt("body", body)
	doc.AddFieldMappingsAt("title", title)
	doc.AddFieldMappingsAt("cat", cat)
	doc.AddFieldMappingsAt("num", num)
	doc.AddFieldMappingsAt("when", when)

	im := mapping.NewIndexMapping()
	im.DefaultMapping = doc
	im.DefaultAnalyzer = "en"
	return im
}

var legacyCats = []string{"alpha", "bravo", "charlie", "delta"}

// legacyDoc is deterministic in i so every version indexes byte-identical input.
// Term frequencies and field lengths both vary widely, which is what makes the
// BM25 length-normalisation term (and therefore §20) observable in the scores.
func legacyDoc(i int) map[string]interface{} {
	body := ""
	// "common" is in every doc (df = 100%, idf near zero — the case where a
	// scoring bug is easiest to mistake for a rounding difference).
	for rep := 0; rep <= i%4; rep++ {
		body += "common "
	}
	for ti, term := range []string{"quick", "brown", "foxes", "jumping"} {
		if (i+ti)%(ti+2) == 0 {
			for rep := 0; rep <= (i+ti)%3; rep++ {
				body += term + " "
			}
		}
	}
	// A phrase that occurs verbatim in a known subset, for phrase/location tests.
	if i%5 == 0 {
		body += "lazy dogs sleeping "
	}
	// Field length spread: 0..40 filler tokens.
	for f := 0; f < i%41; f++ {
		body += fmt.Sprintf("filler%d ", f)
	}
	if body == "" {
		body = "common"
	}
	return map[string]interface{}{
		"body":  body,
		"title": fmt.Sprintf("report %s number %d", legacyCats[i%len(legacyCats)], i%97),
		"cat":   legacyCats[i%len(legacyCats)],
		"num":   float64(i % 500),
		"when":  time.Unix(1600000000+int64(i)*3600, 0).UTC().Format(time.RFC3339),
	}
}

// buildLegacyIndex builds the shared corpus at the given zapx version.
// version == legacyCurrentVersion uses the default plugin path rather than
// forceSegmentVersion, so the reference index is built the way a normal
// application builds one.
func buildLegacyIndex(t *testing.T, version int, scoringModel string) Index {
	t.Helper()
	return buildLegacyIndexOpt(t, version, scoringModel, false)
}

// buildLegacyIndexMerged builds the corpus the same way and then merges it down
// to a single segment.
//
// This is required, not cosmetic, for any test that compares BM25 scores between
// two indexes. avgDocLength is math.Ceil(FieldCardinality(field) / docCount) (see
// bm25ScoreMetrics), and IndexSnapshot.FieldCardinality sums each segment's
// dictionary size — zapx Dictionary.Cardinality() is fst.Len(), the count of
// distinct terms in that one segment. A term present in four segments is
// therefore counted four times, so the sum shrinks as segments merge, and Ceil
// turns a small shrink into a whole-integer drop in avgDocLength. Every BM25
// score in the index moves as a result.
//
// Merges are asynchronous, so two indexes built from identical input can be in
// different merge states when searched and legitimately disagree on scores. That
// is upstream behaviour (FieldCardinality: ff43c15c, bm25ScoreMetrics: 314536b6),
// not something this branch introduced or can fix here, but it does mean a
// cross-index score comparison is only meaningful once both sides have a settled,
// identical segment layout. Collapsing to one segment is the cheapest way to get
// there, and it exercises the legacy version's own Merge implementation as a
// side effect.
func buildLegacyIndexMerged(t *testing.T, version int, scoringModel string) Index {
	t.Helper()
	return buildLegacyIndexOpt(t, version, scoringModel, true)
}

func buildLegacyIndexOpt(t *testing.T, version int, scoringModel string, merge bool) Index {
	t.Helper()

	dir, err := os.MkdirTemp("", fmt.Sprintf("legacy_v%d_", version))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	im := legacyMapping()
	im.(*mapping.IndexMappingImpl).ScoringModel = scoringModel

	path := dir + "/i.bleve"
	var idx Index
	if version == legacyCurrentVersion {
		idx, err = New(path, im)
	} else {
		idx, err = NewUsing(path, im, scorch.Name, scorch.Name, map[string]interface{}{
			"forceSegmentType":    "zap",
			"forceSegmentVersion": version,
		})
	}
	if err != nil {
		t.Fatalf("v%d: create: %v", version, err)
	}
	t.Cleanup(func() { _ = idx.Close() })

	for b := 0; b < legacyCorpusBatches; b++ {
		batch := idx.NewBatch()
		for n := 0; n < legacyCorpusPerBatch; n++ {
			i := b*legacyCorpusPerBatch + n
			if err := batch.Index(fmt.Sprintf("d%05d", i), legacyDoc(i)); err != nil {
				t.Fatalf("v%d: batch.Index: %v", version, err)
			}
		}
		if err := idx.Batch(batch); err != nil {
			t.Fatalf("v%d: Batch: %v", version, err)
		}
	}
	if merge {
		legacyMergeToOneSegment(t, idx, version)
	}
	return idx
}

// legacyMergeToOneSegment force-merges idx down to a single segment, so its
// segment layout — and therefore its FieldCardinality and avgDocLength — is
// deterministic rather than a function of background merge timing.
func legacyMergeToOneSegment(t *testing.T, idx Index, version int) {
	t.Helper()

	sc, ok := idx.(*indexImpl).i.(*scorch.Scorch)
	if !ok {
		t.Fatalf("v%d: not a scorch index", version)
	}
	opts := mergeplan.SingleSegmentMergePlanOptions
	if err := sc.ForceMerge(context.Background(), &opts); err != nil {
		t.Fatalf("v%d: ForceMerge: %v", version, err)
	}
	if n := legacySegmentCount(t, idx); n != 1 {
		t.Fatalf("v%d: %d segments after ForceMerge, want 1 — score comparisons "+
			"against another index would not be well defined", version, n)
	}
}

func legacySegmentCount(t *testing.T, idx Index) int {
	t.Helper()

	sc := idx.(*indexImpl).i.(*scorch.Scorch)
	r, err := sc.Reader()
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer func() { _ = r.Close() }()
	return len(r.(*scorch.IndexSnapshot).Segments())
}

// legacyFieldCardinality reports the stat avgDocLength is derived from, so a
// score mismatch between two indexes can be attributed to (or cleared of) a
// difference in this input rather than in the segment format under test.
func legacyFieldCardinality(t *testing.T, idx Index, field string) int {
	t.Helper()

	sc := idx.(*indexImpl).i.(*scorch.Scorch)
	r, err := sc.Reader()
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer func() { _ = r.Close() }()
	c, err := r.(index.BM25Reader).FieldCardinality(field)
	if err != nil {
		t.Fatalf("FieldCardinality(%s): %v", field, err)
	}
	return c
}

// legacyQueries are the query shapes whose scoring or iteration this branch
// changed.  Each must produce identical results on every version.
func legacyQueries() []struct {
	name string
	q    query.Query
} {
	term := func(field, t string) *query.TermQuery {
		q := query.NewTermQuery(t)
		q.SetField(field)
		return q
	}
	disj := func(qs ...query.Query) query.Query { return query.NewBooleanQuery(nil, qs, nil) }

	boosted := term("body", "quick")
	boosted.SetBoost(3.5)

	phrase := query.NewMatchPhraseQuery("lazy dogs sleeping")
	phrase.SetField("body")

	numMin, numMax := 100.0, 200.0
	numRange := query.NewNumericRangeQuery(&numMin, &numMax)
	numRange.SetField("num")

	prefix := query.NewPrefixQuery("filler1")
	prefix.SetField("body")

	fuzzy := query.NewFuzzyQuery("browm")
	fuzzy.SetField("body")
	fuzzy.SetFuzziness(1)

	wildcard := query.NewWildcardQuery("foxe*")
	wildcard.SetField("body")

	conj := query.NewConjunctionQuery([]query.Query{
		term("body", "common"), term("body", "quick"),
	})

	minShould := query.NewBooleanQuery(nil, []query.Query{
		term("body", "quick"), term("body", "brown"),
		term("body", "foxes"), term("body", "jumping"),
	}, nil)
	minShould.SetMinShould(2)

	return []struct {
		name string
		q    query.Query
	}{
		// A bare term query is scored without SetQueryNorm ever being called;
		// that is the shape whose §20 fast path silently returned 0.
		{"term-highDF", term("body", "common")},
		{"term-midDF", term("body", "quick")},
		{"term-lowDF", term("body", "jumping")},
		{"term-boosted", boosted},
		{"term-keyword", term("cat", "bravo")},
		{"disjunction-4", disj(term("body", "quick"), term("body", "brown"),
			term("body", "foxes"), term("body", "jumping"))},
		{"disjunction-withHighDF", disj(term("body", "common"), term("body", "quick"),
			term("body", "jumping"))},
		{"disjunction-crossField", disj(term("body", "quick"), term("title", "report"),
			term("cat", "delta"))},
		{"disjunction-minShould2", minShould},
		{"conjunction", conj},
		{"phrase", phrase},
		{"numeric-range", numRange},
		{"prefix", prefix},
		{"fuzzy", fuzzy},
		{"wildcard", wildcard},
		{"match-body", query.NewMatchQuery("quick brown foxes")},
	}
}

type legacyResult struct {
	ids    []string
	scores []float64
	total  uint64
	rel    string
}

func legacyRun(t *testing.T, idx Index, q query.Query, size int,
	tweak func(*SearchRequest)) legacyResult {
	t.Helper()
	req := NewSearchRequest(q)
	req.Size = size
	if tweak != nil {
		tweak(req)
	}
	res, err := idx.Search(req)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	out := legacyResult{total: res.Total, rel: res.TotalRelation}
	for _, h := range res.Hits {
		out.ids = append(out.ids, h.ID)
		out.scores = append(out.scores, h.Score)
	}
	return out
}

// legacyScoreTol is a relative tolerance. The §20 fast path and the norm
// round-trip path compute algebraically equal expressions in a different order,
// so agreement is to within float64 rounding, not bit-exact — around 1e-16
// relative in practice. Anything looser would hide a real scoring divergence:
// the smallest genuine difference this branch could introduce is the SmallFloat
// norm quantisation that an earlier revision used, worth several percent.
const legacyScoreTol = 1e-12

// compareLegacy asserts that got (an older version) matches want (v18).
func compareLegacy(t *testing.T, label string, want, got legacyResult) {
	t.Helper()
	compareLegacyOpt(t, label, want, got, true)
}

// compareLegacyOpt is compareLegacy with control over whether Total must match.
// Callers comparing top_scores against complete pass false, because pruning
// legitimately lowers Total. They must not fake that by zeroing the field: the
// real Total is what reveals a truncated result window, and the last rank of a
// truncated window can tie with the first document that did not fit, making its
// id undetermined.
func compareLegacyOpt(t *testing.T, label string, want, got legacyResult, checkTotal bool) {
	t.Helper()

	if len(want.ids) != len(got.ids) {
		t.Errorf("%s: v18 returned %d hits, legacy returned %d", label, len(want.ids), len(got.ids))
		return
	}
	if checkTotal && want.total != got.total {
		t.Errorf("%s: v18 Total=%d, legacy Total=%d", label, want.total, got.total)
	}

	// Scores first: a difference here means the scoring path itself diverged,
	// which is more informative than the id mismatch it also causes.
	tied := func(a, b float64) bool {
		return math.Abs(a-b) <= legacyScoreTol*math.Max(1, math.Max(math.Abs(a), math.Abs(b)))
	}
	for i := range want.scores {
		if !tied(want.scores[i], got.scores[i]) {
			t.Errorf("%s: rank %d score v18=%.17g (%s) legacy=%.17g (%s)",
				label, i, want.scores[i], want.ids[i], got.scores[i], got.ids[i])
			return
		}
	}

	// Ids are only determined where the score is distinct from its neighbours,
	// including the hidden neighbour just outside a truncated window: equal
	// scores are ordered by collection order, which is not part of the contract.
	truncated := uint64(len(want.ids)) < want.total
	for i := range want.ids {
		unique := true
		if i > 0 && tied(want.scores[i-1], want.scores[i]) {
			unique = false
		}
		if i+1 < len(want.scores) && tied(want.scores[i], want.scores[i+1]) {
			unique = false
		}
		if truncated && i == len(want.ids)-1 {
			unique = false
		}
		if unique && want.ids[i] != got.ids[i] {
			t.Errorf("%s: rank %d (untied score %.17g) v18 id=%s, legacy id=%s",
				label, i, want.scores[i], want.ids[i], got.ids[i])
			return
		}
	}
}

// TestLegacySegmentSearchParity is the core differential check: identical
// corpus, identical queries, every older zapx version against v18, in both
// scoring models and at several result-set sizes.
//
// BM25 is the interesting model — it is the one that consumes §20 (via
// TermFieldDoc.FieldLen) and §14 (via the WAND bound) — but TF-IDF is included
// because it shares the posting-iteration path and would catch a decoding
// regression that scoring happens to mask.
func TestLegacySegmentSearchParity(t *testing.T) {
	for _, model := range []string{index.BM25Scoring, index.TFIDFScoring} {
		t.Run(model, func(t *testing.T) {
			ref := buildLegacyIndexMerged(t, legacyCurrentVersion, model)
			queries := legacyQueries()

			for _, version := range legacySegmentVersions {
				t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
					legacy := buildLegacyIndexMerged(t, version, model)

					// Guard the scoring inputs before comparing outputs: if these
					// differ, scores must differ, and the cause is the stat rather
					// than the segment format.
					for _, field := range []string{"body", "title", "num"} {
						want := legacyFieldCardinality(t, ref, field)
						got := legacyFieldCardinality(t, legacy, field)
						if want != got {
							t.Errorf("FieldCardinality(%s): v%d=%d, v%d=%d — avgDocLength "+
								"differs, so score comparisons below are not meaningful",
								field, legacyCurrentVersion, want, version, got)
						}
					}

					for _, tc := range queries {
						for _, size := range []int{1, 10, 100} {
							label := fmt.Sprintf("%s size=%d", tc.name, size)
							want := legacyRun(t, ref, tc.q, size, nil)
							got := legacyRun(t, legacy, tc.q, size, nil)
							compareLegacy(t, label, want, got)
						}
					}
				})
			}
		})
	}
}

// TestLegacySegmentTopScoresEquivalence pins the WAND contract on every older
// version: ScoreMode=top_scores is an optimization, so it must return the same
// top-k as ScoreModeComplete.
//
// Older segments cannot supply the §14 per-term bound at all, and the failure
// mode that motivated this test is not a slightly-off bound but a catastrophic
// one — treating "no bound available" as "bound is zero" prunes every candidate
// once the heap fills, collapsing Total to size+1 and returning near-arbitrary
// documents. See maxTFNormUnavailable in index/scorch.
func TestLegacySegmentTopScoresEquivalence(t *testing.T) {
	queries := legacyQueries()
	for _, version := range append(legacySegmentVersions, legacyCurrentVersion) {
		t.Run(fmt.Sprintf("v%d", version), func(t *testing.T) {
			// Merged, even though this test compares two searches of the SAME index
			// and so looks immune to the cross-index problem buildLegacyIndexMerged
			// exists for. It is not: avgDocLength is ceil(FieldCardinality/docCount)
			// and FieldCardinality sums each segment's fst.Len(), so a background
			// merge landing BETWEEN the complete and top_scores searches shifts every
			// BM25 score and the comparison fails with no bug present. Observed once
			// in five runs before this change, as the 0.8614-vs-0.7980 pair that the
			// avgDocLength investigation had already identified.
			//
			// Cost is cross-segment §15 skipping, which this test no longer exercises;
			// TestWANDTopScoresMatchesComplete covers that on the current format, and
			// what matters here is the legacy format rather than the segment count.
			idx := buildLegacyIndexMerged(t, version, index.BM25Scoring)
			for _, tc := range queries {
				for _, size := range []int{1, 10, 100} {
					label := fmt.Sprintf("%s size=%d", tc.name, size)
					complete := legacyRun(t, idx, tc.q, size, func(r *SearchRequest) {
						r.ScoreMode = ScoreModeComplete
					})
					pruned := legacyRun(t, idx, tc.q, size, func(r *SearchRequest) {
						r.ScoreMode = ScoreModeTopScores
					})
					if pruned.total > complete.total {
						t.Errorf("%s: top_scores Total=%d exceeds complete Total=%d",
							label, pruned.total, complete.total)
					}
					// Total legitimately shrinks under pruning, so compare the
					// top-k only.
					compareLegacyOpt(t, "top_scores vs complete "+label, complete, pruned, false)
				}
			}
		})
	}
}

// TestLegacySegmentNoPruningWithoutBound documents the mechanism the previous
// test verifies behaviourally: with no §14 sidecar there is no valid ceiling, so
// pruning must be off entirely, which is observable as an exact Total (relation
// "eq") where v18 reports a lower bound ("gte").
//
// If a future change teaches WAND to derive a sound bound for legacy segments,
// this test is expected to fail and should be updated — the equivalence test
// above, not this one, is the correctness contract.
func TestLegacySegmentNoPruningWithoutBound(t *testing.T) {
	// A high-DF disjunction at a small size is where pruning fires hardest.
	q := query.NewBooleanQuery(nil, []query.Query{
		func() query.Query { q := query.NewTermQuery("common"); q.SetField("body"); return q }(),
		func() query.Query { q := query.NewTermQuery("quick"); q.SetField("body"); return q }(),
		func() query.Query { q := query.NewTermQuery("brown"); q.SetField("body"); return q }(),
		func() query.Query { q := query.NewTermQuery("foxes"); q.SetField("body"); return q }(),
	}, nil)

	cur := buildLegacyIndex(t, legacyCurrentVersion, index.BM25Scoring)
	curRes := legacyRun(t, cur, q, 10, func(r *SearchRequest) { r.ScoreMode = ScoreModeTopScores })
	if curRes.rel != TotalRelationGte {
		t.Errorf("v%d: expected pruning to fire (relation %q), got %q — the §14 bound "+
			"is not being used, so the legacy comparison below proves nothing",
			legacyCurrentVersion, TotalRelationGte, curRes.rel)
	}

	for _, version := range legacySegmentVersions {
		idx := buildLegacyIndex(t, version, index.BM25Scoring)
		got := legacyRun(t, idx, q, 10, func(r *SearchRequest) { r.ScoreMode = ScoreModeTopScores })
		if got.rel != TotalRelationEq {
			t.Errorf("v%d: TotalRelation=%q, want %q — pruning fired on a segment that "+
				"cannot supply a bound", version, got.rel, TotalRelationEq)
		}
		if got.total != curRes.total && got.total < curRes.total {
			t.Errorf("v%d: Total=%d is below v%d's pruned lower bound %d",
				version, got.total, legacyCurrentVersion, curRes.total)
		}
	}
}

// TestLegacySegmentExplainParity checks the Explain path separately, because it
// is deliberately routed around the §20 fast path (the scorer skips building the
// field-norm table when Explain is set). That makes Explain an independent
// witness: if Explain and non-Explain scores agree on v18 and match legacy, the
// fast path and the formula path agree.
func TestLegacySegmentExplainParity(t *testing.T) {
	q := query.NewTermQuery("quick")
	q.SetField("body")

	ref := buildLegacyIndexMerged(t, legacyCurrentVersion, index.BM25Scoring)
	refPlain := legacyRun(t, ref, q, 20, nil)
	refExpl := legacyRun(t, ref, q, 20, func(r *SearchRequest) { r.Explain = true })
	compareLegacy(t, fmt.Sprintf("v%d explain vs plain", legacyCurrentVersion), refPlain, refExpl)

	for _, version := range legacySegmentVersions {
		idx := buildLegacyIndexMerged(t, version, index.BM25Scoring)
		compareLegacy(t, fmt.Sprintf("v%d explain", version), refExpl,
			legacyRun(t, idx, q, 20, func(r *SearchRequest) { r.Explain = true }))
	}
}

// TestLegacySegmentLocationsAndHighlight covers the location/term-vector stream,
// which is decoded by a second chunked int decoder that never has PFOR mode
// enabled.  §4 pools those decoders across slots, so a decoder previously used
// for a frequency/norm stream can be handed out here; a leaked pforMode would
// mis-decode locations. Highlighting is the end-to-end consumer.
func TestLegacySegmentLocationsAndHighlight(t *testing.T) {
	phrase := query.NewMatchPhraseQuery("lazy dogs sleeping")
	phrase.SetField("body")

	collect := func(idx Index) (int, []string, [][]int) {
		req := NewSearchRequest(phrase)
		req.Size = 15
		req.Fields = []string{"body"}
		req.IncludeLocations = true
		req.Highlight = NewHighlight()
		// Many documents contain this phrase with equal scores, and the point of
		// this test is the location payloads rather than the ranking, so order by
		// id to make the compared subset and its order fully determined.
		req.SortBy([]string{"_id"})
		res, err := idx.Search(req)
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		var ids []string
		var poss [][]int
		for _, h := range res.Hits {
			ids = append(ids, h.ID)
			var p []int
			for _, term := range []string{"lazy", "dogs", "sleeping"} {
				for _, loc := range h.Locations["body"][term] {
					p = append(p, int(loc.Pos), int(loc.Start), int(loc.End))
				}
			}
			poss = append(poss, p)
			if len(h.Fragments["body"]) == 0 {
				t.Errorf("%s: no highlight fragment", h.ID)
			}
		}
		return int(res.Total), ids, poss
	}

	refTotal, refIDs, refPos := collect(buildLegacyIndexMerged(t, legacyCurrentVersion, index.BM25Scoring))
	if refTotal == 0 {
		t.Fatal("phrase matched nothing — the corpus no longer exercises locations")
	}
	for _, version := range legacySegmentVersions {
		total, ids, poss := collect(buildLegacyIndexMerged(t, version, index.BM25Scoring))
		if total != refTotal {
			t.Errorf("v%d: phrase Total=%d, want %d", version, total, refTotal)
			continue
		}
		for i := range refIDs {
			if ids[i] != refIDs[i] {
				t.Errorf("v%d: rank %d id=%s, want %s", version, i, ids[i], refIDs[i])
				break
			}
			if fmt.Sprint(poss[i]) != fmt.Sprint(refPos[i]) {
				t.Errorf("v%d: %s locations=%v, want %v", version, ids[i], poss[i], refPos[i])
				break
			}
		}
	}
}

// TestLegacySegmentFacetsAndSort covers the doc-values stream: facets read it
// per matching document, and sort-by-field reads it through a different entry
// point.  Neither goes through the norm column, so a divergence here points at
// posting/docvalue iteration rather than scoring.
func TestLegacySegmentFacetsAndSort(t *testing.T) {
	q := query.NewTermQuery("common")
	q.SetField("body")

	collect := func(idx Index) (map[string]int, []string, []string) {
		req := NewSearchRequest(q)
		req.Size = 25
		req.SortBy([]string{"num", "_id"})
		req.Fields = []string{"num", "cat"}
		req.AddFacet("cats", NewFacetRequest("cat", 10))
		req.AddFacet("nums", NewFacetRequest("num", 10))
		res, err := idx.Search(req)
		if err != nil {
			t.Fatalf("search: %v", err)
		}
		facets := map[string]int{}
		for _, tf := range res.Facets["cats"].Terms.Terms() {
			facets["cat:"+tf.Term] = tf.Count
		}
		facets["nums:total"] = res.Facets["nums"].Total
		facets["nums:missing"] = res.Facets["nums"].Missing
		var ids, nums []string
		for _, h := range res.Hits {
			ids = append(ids, h.ID)
			nums = append(nums, fmt.Sprint(h.Fields["num"]))
		}
		return facets, ids, nums
	}

	refFacets, refIDs, refNums := collect(buildLegacyIndexMerged(t, legacyCurrentVersion, index.BM25Scoring))
	if len(refFacets) < 3 {
		t.Fatalf("facets look empty: %v", refFacets)
	}
	for _, version := range legacySegmentVersions {
		facets, ids, nums := collect(buildLegacyIndexMerged(t, version, index.BM25Scoring))
		if fmt.Sprint(facets) != fmt.Sprint(refFacets) {
			t.Errorf("v%d: facets=%v, want %v", version, facets, refFacets)
		}
		if fmt.Sprint(ids) != fmt.Sprint(refIDs) {
			t.Errorf("v%d: sorted ids differ\n got %v\nwant %v", version, ids, refIDs)
		}
		if fmt.Sprint(nums) != fmt.Sprint(refNums) {
			t.Errorf("v%d: sorted num values differ\n got %v\nwant %v", version, nums, refNums)
		}
	}
}

// TestLegacySegmentStoredFieldsAndDictionary covers document retrieval and the
// term dictionary / FST iteration paths.  §19 previously cached FST term offsets
// and was removed; dictionary iteration is also what prefix, fuzzy and regexp
// queries are built on, so an ordering or termination bug here is broad.
func TestLegacySegmentStoredFieldsAndDictionary(t *testing.T) {
	collect := func(idx Index) ([]string, []string, uint64) {
		doc, err := idx.Document("d00042")
		if err != nil {
			t.Fatalf("Document: %v", err)
		}
		if doc == nil {
			t.Fatal("d00042 missing")
		}
		var stored []string
		doc.VisitFields(func(f index.Field) {
			stored = append(stored, fmt.Sprintf("%s=%s", f.Name(), string(f.Value())))
		})

		fd, err := idx.FieldDictPrefix("body", []byte("filler1"))
		if err != nil {
			t.Fatalf("FieldDictPrefix: %v", err)
		}
		defer fd.Close()
		var terms []string
		for {
			e, err := fd.Next()
			if err != nil {
				t.Fatalf("FieldDict.Next: %v", err)
			}
			if e == nil {
				break
			}
			terms = append(terms, fmt.Sprintf("%s:%d", e.Term, e.Count))
		}
		n, err := idx.DocCount()
		if err != nil {
			t.Fatalf("DocCount: %v", err)
		}
		return stored, terms, n
	}

	refStored, refTerms, refCount := collect(buildLegacyIndexMerged(t, legacyCurrentVersion, index.BM25Scoring))
	if len(refTerms) == 0 {
		t.Fatal("prefix dictionary iteration returned nothing")
	}
	for _, version := range legacySegmentVersions {
		stored, terms, count := collect(buildLegacyIndexMerged(t, version, index.BM25Scoring))
		if fmt.Sprint(stored) != fmt.Sprint(refStored) {
			t.Errorf("v%d: stored fields differ\n got %v\nwant %v", version, stored, refStored)
		}
		if fmt.Sprint(terms) != fmt.Sprint(refTerms) {
			t.Errorf("v%d: dictionary terms differ\n got %v\nwant %v", version, terms, refTerms)
		}
		if count != refCount {
			t.Errorf("v%d: DocCount=%d, want %d", version, count, refCount)
		}
	}
}

// TestLegacySegmentDeleteUpdateMergeParity checks that mutation keeps parity.
// Merging a legacy index goes through that version's Merge, so this covers the
// writer as well as the reader, and deletes exercise the obsolete-document
// bitmaps that posting iteration has to honour. Reopening afterwards confirms
// the version is still pinned from bolt meta and no segment was quietly upgraded.
func TestLegacySegmentDeleteUpdateMergeParity(t *testing.T) {
	q := query.NewBooleanQuery(nil, []query.Query{
		func() query.Query { q := query.NewTermQuery("common"); q.SetField("body"); return q }(),
		func() query.Query { q := query.NewTermQuery("quick"); q.SetField("body"); return q }(),
		func() query.Query { q := query.NewTermQuery("jumping"); q.SetField("body"); return q }(),
	}, nil)

	mutate := func(idx Index) {
		batch := idx.NewBatch()
		for i := 0; i < legacyCorpusBatches*legacyCorpusPerBatch; i += 7 {
			batch.Delete(fmt.Sprintf("d%05d", i))
		}
		for i := 3; i < 300; i += 11 {
			// Re-index with a different body so the old posting is obsolete and
			// the field length changes.
			d := legacyDoc(i)
			d["body"] = "common common quick quick quick brown"
			if err := batch.Index(fmt.Sprintf("d%05d", i), d); err != nil {
				t.Fatal(err)
			}
		}
		if err := idx.Batch(batch); err != nil {
			t.Fatal(err)
		}
	}

	ref := buildLegacyIndexMerged(t, legacyCurrentVersion, index.BM25Scoring)
	mutate(ref)
	legacyMergeToOneSegment(t, ref, legacyCurrentVersion)
	refRes := legacyRun(t, ref, q, 50, nil)
	refCount, err := ref.DocCount()
	if err != nil {
		t.Fatal(err)
	}

	for _, version := range legacySegmentVersions {
		idx := buildLegacyIndexMerged(t, version, index.BM25Scoring)
		mutate(idx)
		legacyMergeToOneSegment(t, idx, version)

		compareLegacy(t, fmt.Sprintf("v%d after delete+update", version), refRes,
			legacyRun(t, idx, q, 50, nil))
		if n, err := idx.DocCount(); err != nil {
			t.Fatal(err)
		} else if n != refCount {
			t.Errorf("v%d: DocCount=%d after mutation, want %d", version, n, refCount)
		}

		// top_scores must still agree with complete after mutation: deletes
		// change document frequencies and therefore every bound.
		complete := legacyRun(t, idx, q, 20, func(r *SearchRequest) { r.ScoreMode = ScoreModeComplete })
		pruned := legacyRun(t, idx, q, 20, func(r *SearchRequest) { r.ScoreMode = ScoreModeTopScores })
		compareLegacyOpt(t, fmt.Sprintf("v%d top_scores after mutation", version),
			complete, pruned, false)
	}
}

// TestLegacySegmentReopenKeepsVersion is the assumption every other test in this
// file rests on: opening an existing index with plain bleve.Open — no
// forceSegmentVersion — must keep using the version recorded in bolt meta, and
// documents added after that reopen must be written in the same old format.
// If this ever stopped holding, indexes would become mixed-version, and a
// cross-segment bound computed as a max over segments would no longer be valid.
func TestLegacySegmentReopenKeepsVersion(t *testing.T) {
	for _, version := range legacySegmentVersions {
		dir, err := os.MkdirTemp("", fmt.Sprintf("legacy_reopen_v%d_", version))
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(dir)
		path := dir + "/i.bleve"

		im := legacyMapping()
		im.(*mapping.IndexMappingImpl).ScoringModel = index.BM25Scoring
		idx, err := NewUsing(path, im, scorch.Name, scorch.Name, map[string]interface{}{
			"forceSegmentType":    "zap",
			"forceSegmentVersion": version,
		})
		if err != nil {
			t.Fatalf("v%d: create: %v", version, err)
		}
		batch := idx.NewBatch()
		for i := 0; i < 200; i++ {
			if err := batch.Index(fmt.Sprintf("d%05d", i), legacyDoc(i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := idx.Batch(batch); err != nil {
			t.Fatal(err)
		}
		if err := idx.Close(); err != nil {
			t.Fatalf("v%d: close: %v", version, err)
		}

		// Reopened with no config at all — the version must come from disk.
		reopened, err := Open(path)
		if err != nil {
			t.Fatalf("v%d: reopen: %v", version, err)
		}
		assertSegmentVersions(t, reopened, version, "after reopen")

		// Write after reopening, then confirm the version still has not moved
		// and the new documents are searchable.
		batch = reopened.NewBatch()
		for i := 200; i < 300; i++ {
			if err := batch.Index(fmt.Sprintf("d%05d", i), legacyDoc(i)); err != nil {
				t.Fatal(err)
			}
		}
		if err := reopened.Batch(batch); err != nil {
			t.Fatal(err)
		}
		assertSegmentVersions(t, reopened, version, "after writing post-reopen")
		q := query.NewTermQuery("common")
		q.SetField("body")
		res := legacyRun(t, reopened, q, 5, nil)
		if res.total == 0 {
			t.Errorf("v%d: no hits after reopen+write", version)
		}
		for _, s := range res.scores {
			if s <= 0 {
				t.Errorf("v%d: non-positive score %v after reopen — a scoring input is missing",
					version, s)
			}
		}
		if err := reopened.Close(); err != nil {
			t.Fatalf("v%d: close reopened: %v", version, err)
		}
	}
}

// TestLegacySegmentNonZeroScores is a blunt guard against the whole class of
// "fallback silently produces zero" bugs, of which this branch has now had two.
// Every query shape, every version, every scoring model: if a document matched,
// its score must be positive.
//
// A score of exactly 0 is not a plausible BM25 or TF-IDF output for a matching
// document here — no corpus term has idf 0, since none appears in literally
// every document after the "common" repeat pattern — so 0 always means an
// uninitialised or unavailable multiplicand.
func TestLegacySegmentNonZeroScores(t *testing.T) {
	queries := legacyQueries()
	for _, model := range []string{index.BM25Scoring, index.TFIDFScoring} {
		for _, version := range append(legacySegmentVersions, legacyCurrentVersion) {
			idx := buildLegacyIndex(t, version, model)
			for _, tc := range queries {
				for _, mode := range []string{ScoreModeComplete, ScoreModeTopScores} {
					res := legacyRun(t, idx, tc.q, 10, func(r *SearchRequest) {
						r.ScoreMode = mode
					})
					for i, s := range res.scores {
						if s <= 0 || math.IsNaN(s) || math.IsInf(s, 0) {
							t.Errorf("%s v%d %s %s: rank %d has score %v (id=%s)",
								model, version, tc.name, mode, i, s, res.ids[i])
							break
						}
					}
				}
			}
		}
	}
}

// assertSegmentVersions inspects the live segments themselves, rather than any
// configuration value, so it reports what is actually on disk.
func assertSegmentVersions(t *testing.T, idx Index, want int, when string) {
	t.Helper()

	sc, ok := idx.(*indexImpl).i.(*scorch.Scorch)
	if !ok {
		t.Fatalf("not a scorch index")
	}
	r, err := sc.Reader()
	if err != nil {
		t.Fatalf("Reader: %v", err)
	}
	defer func() { _ = r.Close() }()

	snap, ok := r.(*scorch.IndexSnapshot)
	if !ok {
		t.Fatalf("reader is %T, not *scorch.IndexSnapshot", r)
	}
	segs := snap.Segments()
	if len(segs) == 0 {
		t.Errorf("v%d %s: no segments to check", want, when)
	}
	for i, ss := range segs {
		v, ok := ss.Segment().(interface{ Version() uint32 })
		if !ok {
			// An in-memory segment that has not been persisted yet has no
			// on-disk version; nothing to assert about it.
			continue
		}
		if got := v.Version(); got != uint32(want) {
			t.Errorf("v%d %s: segment %d is version %d — the index was upgraded in "+
				"place, which would make it mixed-version", want, when, i, got)
		}
	}
}
