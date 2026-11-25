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

package query

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
)

func TestMoreLikeThisQueryParse(t *testing.T) {
	q, err := ParseQuery([]byte(`{
		"more_like_this": {
			"fields": ["description"],
			"like_ids": ["hotel_10025"],
			"max_query_terms": 5,
			"min": 2,
			"boost_terms": 1.5,
			"boost": 1.5
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	mlt, ok := q.(*MoreLikeThisQuery)
	if !ok {
		t.Fatalf("expected *MoreLikeThisQuery, got %T", q)
	}
	if len(mlt.Fields) != 1 || mlt.Fields[0] != "description" {
		t.Errorf("unexpected fields %v", mlt.Fields)
	}
	if len(mlt.LikeIDs) != 1 || mlt.LikeIDs[0] != "hotel_10025" {
		t.Errorf("unexpected like_ids %v", mlt.LikeIDs)
	}
	if mlt.MaxQueryTerms != 5 || mlt.Min != "2" || mlt.BoostTerms != 1.5 || mlt.Boost() != 1.5 {
		t.Errorf("unexpected values: max_query_terms %d min %q boost_terms %v boost %v",
			mlt.MaxQueryTerms, mlt.Min, mlt.BoostTerms, mlt.Boost())
	}
	// fields not given take the defaults
	if mlt.MinTermFreq != DefaultMoreLikeThisMinTermFreq ||
		mlt.MinDocFreq != DefaultMoreLikeThisMinDocFreq {
		t.Errorf("expected defaults, got min_term_freq %d min_doc_freq %d",
			mlt.MinTermFreq, mlt.MinDocFreq)
	}
	if err := mlt.Validate(); err != nil {
		t.Errorf("expected valid query, got %v", err)
	}

	// an absent node keeps all defaults, min included
	q, err = ParseQuery([]byte(`{"more_like_this": {"fields": ["f"], "like_text": "some text"}}`))
	if err != nil {
		t.Fatal(err)
	}
	mlt = q.(*MoreLikeThisQuery)
	if mlt.MaxQueryTerms != DefaultMoreLikeThisMaxQueryTerms || mlt.Min != DefaultMoreLikeThisMin ||
		mlt.BoostTerms != 0 || mlt.Boost() != 1.0 || mlt.LikeText != "some text" {
		t.Errorf("unexpected defaults %+v", mlt)
	}

	// min as a percentage string
	q, err = ParseQuery([]byte(`{"more_like_this": {"fields": ["f"], "like_ids": ["a"], "min": "50%"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if mlt = q.(*MoreLikeThisQuery); mlt.Min != "50%" {
		t.Errorf("expected min 50%%, got %q", mlt.Min)
	}

	// like_text as an object keyed by field
	q, err = ParseQuery([]byte(`{"more_like_this": {
		"like_text": {"title": "Login fails", "description": "the login page returns a 500"}}}`))
	if err != nil {
		t.Fatal(err)
	}
	mlt = q.(*MoreLikeThisQuery)
	if mlt.LikeText != "" || len(mlt.LikeTextByField) != 2 || mlt.LikeTextByField["title"] != "Login fails" {
		t.Errorf("expected a per-field like_text, got %+v", mlt)
	}
	// without fields, the compared fields are the ones with a text
	if err := mlt.Validate(); err != nil {
		t.Errorf("expected a valid query, got %v", err)
	}
	if fields := mlt.ComparedFields(); len(fields) != 2 || fields[0] != "description" || fields[1] != "title" {
		t.Errorf("expected the fields of the texts, sorted, got %v", fields)
	}

	for name, input := range map[string]string{
		"non-object node": `{"more_like_this": "oops"}`,
		"like_text array": `{"more_like_this": {"fields": ["f"], "like_text": ["a", "b"]}}`,
		"min object":      `{"more_like_this": {"fields": ["f"], "like_ids": ["a"], "min": {}}}`,
	} {
		if _, err = ParseQuery([]byte(input)); err == nil {
			t.Errorf("%s: expected a parse error", name)
		}
	}
}

func TestMoreLikeThisQueryMarshalRoundTrip(t *testing.T) {
	orig := NewMoreLikeThisQuery([]string{"a", "b"}, []string{"d1", "d2"})
	orig.SetMin(1)
	orig.BoostTerms = 2
	orig.SetBoost(2)
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(data), `{"more_like_this":{`) {
		t.Errorf("expected the more_like_this wrapper, got %s", data)
	}
	if !strings.Contains(string(data), `"min":1`) {
		t.Errorf("expected a count to marshal as a number, got %s", data)
	}
	parsed, err := ParseQuery(data)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := parsed.(*MoreLikeThisQuery)
	if !ok {
		t.Fatalf("expected *MoreLikeThisQuery, got %T", parsed)
	}
	if got.Boost() != 2 || got.Min != "1" || got.BoostTerms != 2 || len(got.Fields) != 2 || len(got.LikeIDs) != 2 ||
		got.MaxQueryTerms != DefaultMoreLikeThisMaxQueryTerms {
		t.Errorf("round trip mismatch: %+v", got)
	}

	// per-field text and a percentage survive the round trip
	orig = NewMoreLikeThisQuery(nil, nil)
	orig.LikeTextByField = map[string]string{"title": "x", "body": "y z"}
	orig.SetMinPercent(40)
	data, err = json.Marshal(orig)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"min":"40%"`) || !strings.Contains(string(data), `"like_text":{`) {
		t.Errorf("unexpected JSON %s", data)
	}
	parsed, err = ParseQuery(data)
	if err != nil {
		t.Fatal(err)
	}
	got = parsed.(*MoreLikeThisQuery)
	if got.Min != "40%" || len(got.LikeTextByField) != 2 || got.LikeTextByField["body"] != "y z" || got.LikeText != "" {
		t.Errorf("round trip mismatch: %+v", got)
	}
}

func TestMoreLikeThisQueryValidate(t *testing.T) {
	cases := map[string]*MoreLikeThisQuery{
		"no fields":            {LikeIDs: []string{"a"}, MaxQueryTerms: 1},
		"_all field":           {Fields: []string{"_all"}, LikeIDs: []string{"a"}, MaxQueryTerms: 1},
		"no input":             {Fields: []string{"f"}, MaxQueryTerms: 1},
		"both inputs":          {Fields: []string{"f"}, LikeIDs: []string{"a"}, LikeText: "x", MaxQueryTerms: 1},
		"both text forms":      {Fields: []string{"f"}, LikeText: "x", LikeTextByField: map[string]string{"f": "y"}, MaxQueryTerms: 1},
		"text for other field": {Fields: []string{"f"}, LikeTextByField: map[string]string{"g": "y"}, MaxQueryTerms: 1},
		"zero max terms":       {Fields: []string{"f"}, LikeIDs: []string{"a"}},
		"negative docfreq":     {Fields: []string{"f"}, LikeIDs: []string{"a"}, MaxQueryTerms: 1, MinDocFreq: -1},
		"negative boost_terms": {Fields: []string{"f"}, LikeIDs: []string{"a"}, MaxQueryTerms: 1, BoostTerms: -1},
		"negative min":         {Fields: []string{"f"}, LikeIDs: []string{"a"}, MaxQueryTerms: 1, Min: "-1"},
		"min over 100%":        {Fields: []string{"f"}, LikeIDs: []string{"a"}, MaxQueryTerms: 1, Min: "150%"},
		"min garbage":          {Fields: []string{"f"}, LikeIDs: []string{"a"}, MaxQueryTerms: 1, Min: "many"},
		"empty field text":     {LikeTextByField: map[string]string{"f": " "}, MaxQueryTerms: 1},
	}
	for name, q := range cases {
		if err := q.Validate(); err == nil {
			t.Errorf("%s: expected a validation error", name)
		}
	}
	if err := NewMoreLikeThisQuery([]string{"f"}, []string{"a"}).Validate(); err != nil {
		t.Errorf("expected a valid query, got %v", err)
	}
	// a per-field text may name a subset of the fields
	q := &MoreLikeThisQuery{Fields: []string{"f", "g"}, LikeTextByField: map[string]string{"f": "y"}, MaxQueryTerms: 1}
	if err := q.Validate(); err != nil {
		t.Errorf("expected a valid query, got %v", err)
	}
}

func TestMinShouldMatch(t *testing.T) {
	cases := []struct {
		min   MinShouldMatch
		terms int
		want  float64
	}{
		{"", 5, 1},     // the default 30% of 5, rounded down
		{"30%", 25, 7}, // 7.5 rounded down
		{"50%", 5, 2},
		{"100%", 5, 5},
		{"0%", 5, 0},
		{"2", 5, 2},
		{"7", 5, 5}, // a count is clamped to the terms kept
		{"0", 5, 0},
	}
	for _, c := range cases {
		if got := c.min.Resolve(c.terms); got != c.want {
			t.Errorf("Resolve(%q, %d): expected %v, got %v", c.min, c.terms, c.want, got)
		}
	}
	for _, bad := range []MinShouldMatch{"-1", "101%", "x", "%", "NaN", "Inf", "NaN%", "1.5"} {
		if err := bad.Validate(); err == nil {
			t.Errorf("expected %q to be refused", bad)
		}
	}
	// percentages round down exactly, whatever the term count
	for pct := 0; pct <= 100; pct++ {
		for n := 1; n <= 200; n++ {
			m := MinShouldMatch(strconv.Itoa(pct) + "%")
			if got, want := m.Resolve(n), float64(pct*n/100); got != want {
				t.Fatalf("Resolve(%q, %d): expected %v, got %v", m, n, want, got)
			}
		}
	}
	var m MinShouldMatch
	if err := json.Unmarshal([]byte(`3`), &m); err != nil || m != "3" {
		t.Errorf("expected a number to parse as a count, got %q %v", m, err)
	}
	if err := json.Unmarshal([]byte(`"30%"`), &m); err != nil || m != "30%" {
		t.Errorf("expected a percentage string to parse, got %q %v", m, err)
	}
	if err := json.Unmarshal([]byte(`[1]`), &m); err == nil {
		t.Error("expected an array to be refused")
	}
	// null leaves the preset value, so a node keeps its default
	q, err := ParseQuery([]byte(`{"more_like_this": {"fields": ["f"], "like_ids": ["a"], "min": null}}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := q.(*MoreLikeThisQuery).Min; got != DefaultMoreLikeThisMin {
		t.Errorf("expected null to keep the default min, got %q", got)
	}
}

// mltStats builds the merged form of the statistics, every term read from
// an index of docCount documents
func mltStats(docCount uint64, terms ...*search.MLTTermStat) *search.MLTStats {
	for _, st := range terms {
		st.DocCount = docCount
	}
	return &search.MLTStats{Terms: terms}
}

func rewriteStats() *search.MLTStats {
	return mltStats(1000,
		&search.MLTTermStat{Field: "f", Term: "hotel", TF: 1, DF: 900},
		&search.MLTTermStat{Field: "f", Term: "harbourfront", TF: 1, DF: 2, Sources: []string{"d1"}},
		&search.MLTTermStat{Field: "f", Term: "pool", TF: 1, DF: 14},
		&search.MLTTermStat{Field: "f", Term: "boutique", TF: 1, DF: 4},
		&search.MLTTermStat{Field: "f", Term: "typo", TF: 1, DF: 0},
	)
}

func rewrittenDisjunction(t *testing.T, rewritten Query) *DisjunctionQuery {
	t.Helper()
	bq, ok := rewritten.(*BooleanQuery)
	if !ok {
		t.Fatalf("expected a boolean query, got %T", rewritten)
	}
	must, ok := bq.Must.(*ConjunctionQuery)
	if !ok || len(must.Conjuncts) != 1 {
		t.Fatalf("expected a must conjunction with one clause, got %#v", bq.Must)
	}
	disj, ok := must.Conjuncts[0].(*DisjunctionQuery)
	if !ok {
		t.Fatalf("expected a disjunction, got %T", must.Conjuncts[0])
	}
	return disj
}

func TestMoreLikeThisQueryRewrite(t *testing.T) {
	q := NewMoreLikeThisQuery([]string{"f"}, []string{"d1"})
	q.MaxQueryTerms = 3
	q.SetMin(5) // larger than the number of terms: must be clamped

	rewritten := q.Rewrite(rewriteStats(), RewriteOptions{})
	disj := rewrittenDisjunction(t, rewritten)
	// hotel is the most common term and typo has df 0; with max 3 the
	// rarest three remain, rarest first
	wantOrder := []string{"harbourfront", "boutique", "pool"}
	if len(disj.Disjuncts) != len(wantOrder) {
		t.Fatalf("expected %d disjuncts, got %d", len(wantOrder), len(disj.Disjuncts))
	}
	for i, want := range wantOrder {
		tq, ok := disj.Disjuncts[i].(*TermQuery)
		if !ok {
			t.Fatalf("disjunct %d: expected a term query, got %T", i, disj.Disjuncts[i])
		}
		if tq.Term != want || tq.Field() != "f" {
			t.Errorf("disjunct %d: expected %s on f, got %s on %s", i, want, tq.Term, tq.Field())
		}
		// boost_terms is off by default: the terms are not boosted and
		// carry no notes
		if tq.BoostVal != nil || tq.Notes != nil {
			t.Errorf("disjunct %d: expected no boost and no notes by default, got %+v", i, tq)
		}
	}
	if disj.Min != 3 {
		t.Errorf("expected min clamped to 3, got %v", disj.Min)
	}
	if disj.Notes != nil {
		t.Error("expected no notes on the disjunction by default")
	}
	bq := rewritten.(*BooleanQuery)
	mustNot, ok := bq.MustNot.(*DisjunctionQuery)
	if !ok || len(mustNot.Disjuncts) != 1 {
		t.Fatalf("expected a must_not disjunction with one clause, got %#v", bq.MustNot)
	}
	ids, ok := mustNot.Disjuncts[0].(*DocIDQuery)
	if !ok || len(ids.IDs) != 1 || ids.IDs[0] != "d1" {
		t.Errorf("expected the source document excluded, got %#v", mustNot.Disjuncts[0])
	}
}

func TestMoreLikeThisQueryRewriteBoosts(t *testing.T) {
	q := NewMoreLikeThisQuery([]string{"f"}, []string{"d1"})
	q.MaxQueryTerms = 3

	// the query boost alone applies to every term
	q.SetBoost(2)
	disj := rewrittenDisjunction(t, q.Rewrite(rewriteStats(), RewriteOptions{}))
	for i, d := range disj.Disjuncts {
		if b := d.(*TermQuery).Boost(); b != 2 {
			t.Errorf("disjunct %d: expected the query boost 2, got %v", i, b)
		}
	}

	// boost_terms boosts by the weight relative to the best term, times
	// the factor and the query boost
	q.BoostTerms = 3
	disj = rewrittenDisjunction(t, q.Rewrite(rewriteStats(), RewriteOptions{}))
	first := disj.Disjuncts[0].(*TermQuery)
	last := disj.Disjuncts[2].(*TermQuery)
	if math.Abs(first.Boost()-6) > 1e-9 {
		t.Errorf("expected the best term boosted by 3 * 2, got %v", first.Boost())
	}
	wantLast := 6 * (1 + math.Log(1000.0/15)) / (1 + math.Log(1000.0/3))
	if math.Abs(last.Boost()-wantLast) > 1e-9 {
		t.Errorf("expected %v for the weakest term, got %v", wantLast, last.Boost())
	}
	if first.Boost() <= disj.Disjuncts[1].(*TermQuery).Boost() {
		t.Error("expected decreasing boosts")
	}

	// the BM25 idf ranks the same way here
	disj = rewrittenDisjunction(t, q.Rewrite(rewriteStats(), RewriteOptions{BM25: true}))
	if disj.Disjuncts[0].(*TermQuery).Term != "harbourfront" {
		t.Errorf("expected the rarest term first with bm25 idf, got %s", disj.Disjuncts[0].(*TermQuery).Term)
	}
}

func TestMoreLikeThisQueryRewriteMinPercent(t *testing.T) {
	q := NewMoreLikeThisQuery([]string{"f"}, []string{"d1"})
	// four terms survive (typo has df 0); the default 30% of 4 is 1
	disj := rewrittenDisjunction(t, q.Rewrite(rewriteStats(), RewriteOptions{}))
	if len(disj.Disjuncts) != 4 || disj.Min != 1 {
		t.Errorf("expected 4 terms with min 1 by default, got %d terms with min %v", len(disj.Disjuncts), disj.Min)
	}
	q.SetMinPercent(75)
	disj = rewrittenDisjunction(t, q.Rewrite(rewriteStats(), RewriteOptions{}))
	if disj.Min != 3 {
		t.Errorf("expected 75%% of 4 to resolve to 3, got %v", disj.Min)
	}
	q.SetMin(0)
	disj = rewrittenDisjunction(t, q.Rewrite(rewriteStats(), RewriteOptions{}))
	if disj.Min != 0 {
		t.Errorf("expected an explicit 0 to stay 0, got %v", disj.Min)
	}
}

func TestMoreLikeThisQueryRewriteNotes(t *testing.T) {
	q := NewMoreLikeThisQuery([]string{"f"}, []string{"d1"})
	q.MaxQueryTerms = 2
	disj := rewrittenDisjunction(t, q.Rewrite(rewriteStats(), RewriteOptions{Notes: true}))

	var termNotes MLTTermNotes
	tq := disj.Disjuncts[0].(*TermQuery)
	if err := json.Unmarshal(tq.Notes, &termNotes); err != nil {
		t.Fatalf("expected term notes, got %s: %v", tq.Notes, err)
	}
	wantIDF := 1 + math.Log(1000.0/3)
	if len(termNotes.Sources) != 1 || termNotes.Sources[0] != "d1" || termNotes.TF != 1 || termNotes.DF != 2 ||
		termNotes.N != 1000 || math.Abs(termNotes.IDF-wantIDF) > 1e-9 || math.Abs(termNotes.Weight-wantIDF) > 1e-9 {
		t.Errorf("unexpected term notes %+v", termNotes)
	}

	var queryNotes MLTQueryNotes
	if err := json.Unmarshal(disj.Notes, &queryNotes); err != nil {
		t.Fatalf("expected disjunction notes, got %s: %v", disj.Notes, err)
	}
	// every term of the input is a candidate, the df-0 one included
	if queryNotes.Candidates != 5 || queryNotes.Kept != 2 || queryNotes.Min != 0 ||
		len(queryNotes.LikeIDs) != 1 || len(queryNotes.Fields) != 1 {
		t.Errorf("unexpected disjunction notes %+v", queryNotes)
	}

	// the notes survive a JSON round trip of the rewritten query and are
	// ignored by the parser
	data, err := json.Marshal(q.Rewrite(rewriteStats(), RewriteOptions{Notes: true}))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseQuery(data)
	if err != nil {
		t.Fatal(err)
	}
	back := rewrittenDisjunction(t, parsed)
	if len(back.Disjuncts) != 2 || back.Notes == nil || back.Disjuncts[0].(*TermQuery).Notes == nil {
		t.Errorf("expected the notes to survive the round trip: %s", data)
	}
	if _, ok := parsed.(*BooleanQuery); !ok {
		t.Errorf("expected a boolean query back, got %T", parsed)
	}
}

func TestMoreLikeThisQueryRewriteFilters(t *testing.T) {
	stats := mltStats(100,
		&search.MLTTermStat{Field: "f", Term: "once", TF: 1, DF: 10},
		&search.MLTTermStat{Field: "f", Term: "twice", TF: 2, DF: 10},
		&search.MLTTermStat{Field: "f", Term: "rare", TF: 2, DF: 1},
	)
	q := NewMoreLikeThisQuery([]string{"f"}, nil)
	q.LikeText = "whatever"
	q.MinTermFreq = 2
	q.MinDocFreq = 2

	rewritten := q.Rewrite(stats, RewriteOptions{BM25: true})
	// like_text has no source document to exclude: a bare disjunction
	disj, ok := rewritten.(*DisjunctionQuery)
	if !ok {
		t.Fatalf("expected a disjunction, got %T", rewritten)
	}
	if len(disj.Disjuncts) != 1 || disj.Disjuncts[0].(*TermQuery).Term != "twice" {
		t.Errorf("expected only 'twice' to survive the filters, got %#v", disj.Disjuncts)
	}

	// nothing left, or no stats at all, becomes match_none
	q.MinTermFreq = 3
	if _, ok := q.Rewrite(stats, RewriteOptions{}).(*MatchNoneQuery); !ok {
		t.Error("expected match_none when no term survives")
	}
	if _, ok := q.Rewrite(nil, RewriteOptions{}).(*MatchNoneQuery); !ok {
		t.Error("expected match_none without stats")
	}
}

func TestMoreLikeThisStatsCandidates(t *testing.T) {
	// the per-document form of one index: the same term in two source
	// documents adds its frequencies up, keeps the index's df and
	// document count, and lists the documents it came from
	stats := &search.MLTStats{
		FoundIDs: []string{"a", "b"},
		SourceDocs: []*search.MLTSourceDoc{
			{ID: "a", Terms: []*search.MLTTermStat{
				{Field: "f", Term: "pool", TF: 3, DF: 4, DocCount: 10},
				{Field: "f", Term: "rooftop", TF: 1, DF: 2, DocCount: 10},
			}},
			{ID: "b", Terms: []*search.MLTTermStat{
				{Field: "f", Term: "pool", TF: 2, DF: 4, DocCount: 10},
			}},
		},
	}
	candidates := stats.Candidates()
	if len(candidates) != 2 {
		t.Fatalf("expected 2 candidates, got %+v", candidates)
	}
	if c := candidates[0]; c.Term != "pool" || c.TF != 5 || c.DF != 4 || c.DocCount != 10 ||
		len(c.Sources) != 2 || c.Sources[0] != "a" || c.Sources[1] != "b" {
		t.Errorf("unexpected candidate %+v", c)
	}
	if c := candidates[1]; c.Term != "rooftop" || c.TF != 1 || len(c.Sources) != 1 || c.Sources[0] != "a" {
		t.Errorf("unexpected candidate %+v", c)
	}
	// the input is left untouched
	if stats.SourceDocs[0].Terms[0].TF != 3 || stats.SourceDocs[0].Terms[0].Sources != nil {
		t.Error("Candidates must not modify the source document stats")
	}
	// the merged form is returned as is; nil is empty
	merged := &search.MLTStats{Terms: candidates}
	if got := merged.Candidates(); len(got) != 2 || got[0] != candidates[0] {
		t.Error("expected the merged terms to be returned as they are")
	}
	var none *search.MLTStats
	if none.Candidates() != nil {
		t.Error("expected no candidates from nil stats")
	}
}

func TestMoreLikeThisStatsJSONRoundTrip(t *testing.T) {
	stats := &search.MLTStats{
		FoundIDs: []string{"a"},
		SourceDocs: []*search.MLTSourceDoc{
			{ID: "a", Terms: []*search.MLTTermStat{{Field: "f", Term: "pool", TF: 3, DF: 4, DocCount: 10}}},
		},
	}
	data, err := json.Marshal(map[string]*search.MLTStats{"0": stats})
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]*search.MLTStats
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatal(err)
	}
	got := back["0"]
	if got == nil || len(got.SourceDocs) != 1 || got.SourceDocs[0].ID != "a" ||
		len(got.SourceDocs[0].Terms) != 1 || len(got.FoundIDs) != 1 {
		t.Errorf("round trip mismatch: %s -> %+v", data, got)
	}
	if st := got.SourceDocs[0].Terms[0]; st.Term != "pool" || st.TF != 3 || st.DF != 4 || st.DocCount != 10 {
		t.Errorf("round trip mismatch: %+v", st)
	}
}

func TestMoreLikeThisIDF(t *testing.T) {
	if got := mltIDF(false, 1000, 0); math.Abs(got-(1+math.Log(1000))) > 1e-9 {
		t.Errorf("tf-idf idf for df 0: got %v", got)
	}
	if mltIDF(false, 1000, 1) <= mltIDF(false, 1000, 999) {
		t.Error("tf-idf idf should decrease with df")
	}
	if mltIDF(true, 1000, 1) <= mltIDF(true, 1000, 999) {
		t.Error("bm25 idf should decrease with df")
	}
	if mltIDF(false, 0, 0) != 0 || mltIDF(true, 0, 0) != 0 {
		t.Error("idf of an empty index should be 0")
	}
}

func TestNotesOnQueriesRoundTrip(t *testing.T) {
	// notes are kept on the query types a rewrite generates and ignored
	// everywhere else
	q, err := ParseQuery([]byte(`{
		"must": {"conjuncts": [{"disjuncts": [
			{"term": "a", "field": "f", "notes": {"why": "test"}},
			{"match": "b", "field": "f", "notes": "dropped on a match query"}
		], "min": 1, "notes": [1, 2]}]},
		"must_not": {"disjuncts": [{"ids": ["x"]}]},
		"notes": {"app": "widget"}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	bq := q.(*BooleanQuery)
	if string(bq.Notes) != `{"app": "widget"}` {
		t.Errorf("expected the boolean notes kept, got %s", bq.Notes)
	}
	disj := bq.Must.(*ConjunctionQuery).Conjuncts[0].(*DisjunctionQuery)
	if string(disj.Notes) != `[1, 2]` {
		t.Errorf("expected the disjunction notes kept, got %s", disj.Notes)
	}
	if tq := disj.Disjuncts[0].(*TermQuery); string(tq.Notes) != `{"why": "test"}` {
		t.Errorf("expected the term notes kept, got %s", tq.Notes)
	}
	data, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"notes":{"why":"test"}`) || !strings.Contains(string(data), `"notes":{"app":"widget"}`) {
		t.Errorf("expected the notes to marshal, got %s", data)
	}
	// a query without notes marshals exactly as before
	plain, err := json.Marshal(NewTermQuery("a"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(plain), "notes") {
		t.Errorf("expected no notes key on a plain term query, got %s", plain)
	}
}

func TestExtractAndReplaceMoreLikeThisQueries(t *testing.T) {
	m1 := NewMoreLikeThisQuery([]string{"f"}, []string{"a"})
	m2 := NewMoreLikeThisQuery([]string{"g"}, nil)
	m2.LikeText = "text"
	tq := NewTermQuery("x")
	tq.SetField("f")

	// NewBooleanQuery wraps the must clauses in a conjunction and the
	// should clauses in a disjunction
	tree := NewBooleanQuery([]Query{m1, tq}, []Query{m2}, nil)

	found := ExtractMoreLikeThisQueries(tree)
	if len(found) != 2 || found[0] != m1 || found[1] != m2 {
		t.Fatalf("expected [m1 m2] in tree order, got %v", found)
	}
	if n := len(ExtractMoreLikeThisQueries(tq)); n != 0 {
		t.Errorf("expected no nodes in a term query, got %d", n)
	}

	replaced, err := ReplaceMoreLikeThisQueries(tree, func(idx int, mlt *MoreLikeThisQuery) (Query, error) {
		return NewMatchNoneQuery(), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(ExtractMoreLikeThisQueries(replaced)); n != 0 {
		t.Errorf("expected every node replaced, still found %d", n)
	}
	// the original tree is untouched and the untouched leaves are shared
	if n := len(ExtractMoreLikeThisQueries(tree)); n != 2 {
		t.Errorf("original tree was modified, found %d nodes", n)
	}
	rb := replaced.(*BooleanQuery)
	conj := rb.Must.(*ConjunctionQuery)
	if _, ok := conj.Conjuncts[0].(*MatchNoneQuery); !ok {
		t.Errorf("expected the first conjunct replaced, got %T", conj.Conjuncts[0])
	}
	if conj.Conjuncts[1] != tq {
		t.Error("expected the term query to be shared with the original tree")
	}
}

func TestMoreLikeThisQueryExtractFields(t *testing.T) {
	q := NewMoreLikeThisQuery([]string{"title", "body"}, []string{"a"})
	fs, err := ExtractFields(q, mapping.NewIndexMapping(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(fs) != 2 {
		t.Errorf("expected 2 fields, got %v", fs)
	}
	for _, f := range []string{"title", "body"} {
		if _, ok := fs[f]; !ok {
			t.Errorf("expected field %s, got %v", f, fs)
		}
	}
	// a per-field text without fields contributes the fields of the texts
	q = NewMoreLikeThisQuery(nil, nil)
	q.LikeTextByField = map[string]string{"title": "x"}
	fs, err = ExtractFields(q, mapping.NewIndexMapping(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := fs["title"]; !ok || len(fs) != 1 {
		t.Errorf("expected the text's field, got %v", fs)
	}
}
