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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
)

const (
	DefaultMoreLikeThisMinTermFreq   = 1
	DefaultMoreLikeThisMinDocFreq    = 1
	DefaultMoreLikeThisMaxQueryTerms = 25
	// DefaultMoreLikeThisMin is the share of the kept terms a hit must
	// match, as in Elasticsearch.
	DefaultMoreLikeThisMin MinShouldMatch = "30%"
)

// MinShouldMatch is how many of the kept terms a hit must match: a count,
// given as a JSON number or a numeric string ("2"), or a share of the kept
// terms given as a percentage ("30%"). Empty means the default.
type MinShouldMatch string

func (m MinShouldMatch) String() string {
	return string(m)
}

func (m MinShouldMatch) isPercent() bool {
	return strings.HasSuffix(string(m), "%")
}

// Validate checks the value is a non-negative count or a percentage
// between 0 and 100.
func (m MinShouldMatch) Validate() error {
	s := strings.TrimSpace(string(m))
	if s == "" {
		return nil
	}
	if strings.HasSuffix(s, "%") {
		pct, err := strconv.ParseFloat(strings.TrimSuffix(s, "%"), 64)
		if err != nil || math.IsNaN(pct) || pct < 0 || pct > 100 {
			return fmt.Errorf("min must be a whole number or a percentage between 0%% and 100%%, got %q", string(m))
		}
		return nil
	}
	count, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(count) || math.IsInf(count, 0) || count < 0 || count != math.Floor(count) {
		return fmt.Errorf("min must be a whole number or a percentage between 0%% and 100%%, got %q", string(m))
	}
	return nil
}

// Resolve turns the value into the number of terms a hit must match out of
// numTerms kept terms: a percentage is rounded down, a count is clamped.
func (m MinShouldMatch) Resolve(numTerms int) float64 {
	s := strings.TrimSpace(string(m))
	if s == "" {
		s = string(DefaultMoreLikeThisMin)
	}
	if strings.HasSuffix(s, "%") {
		pct, _ := strconv.ParseFloat(strings.TrimSuffix(s, "%"), 64)
		return math.Floor(pct * float64(numTerms) / 100)
	}
	count, _ := strconv.ParseFloat(s, 64)
	return math.Min(count, float64(numTerms))
}

func (m MinShouldMatch) MarshalJSON() ([]byte, error) {
	s := strings.TrimSpace(string(m))
	if s != "" && !strings.HasSuffix(s, "%") {
		if count, err := strconv.ParseFloat(s, 64); err == nil {
			return util.MarshalJSON(count)
		}
	}
	return util.MarshalJSON(s)
}

func (m *MinShouldMatch) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		// absent or null: whatever the caller preset, usually the default
		return nil
	}
	if len(data) > 0 && data[0] == '"' {
		var s string
		if err := util.UnmarshalJSON(data, &s); err != nil {
			return err
		}
		*m = MinShouldMatch(strings.TrimSpace(s))
		return nil
	}
	var count float64
	if err := util.UnmarshalJSON(data, &count); err != nil {
		return fmt.Errorf("min must be a count or a percentage string: %v", err)
	}
	*m = MinShouldMatch(strconv.FormatFloat(count, 'f', -1, 64))
	return nil
}

// Notes is free-form JSON carried with a query and ignored when it runs.
// It is its own type rather than json.RawMessage so that an empty value is
// omitted by every JSON library, and null decodes to empty.
type Notes []byte

func (n Notes) MarshalJSON() ([]byte, error) {
	if len(n) == 0 {
		return []byte("null"), nil
	}
	return n, nil
}

func (n *Notes) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		*n = nil
		return nil
	}
	*n = append(Notes(nil), data...)
	return nil
}

// MoreLikeThisQuery finds documents similar to the given documents (LikeIDs)
// or to the given text (LikeText). The input is distilled into its most
// distinctive terms, ranked by tf*idf, and the query is then executed as a
// disjunction of those terms. The source documents themselves are excluded
// from the results.
//
// The compared fields must be text fields with doc values, which is where
// a source document's terms are read from. The terms are matched as they
// were indexed; synonyms are not expanded for them.
//
// On a single index the query is resolved by the index with its own
// statistics. Across an index alias the terms are gathered during
// preSearch, since only the index holding a source document can read its
// terms, and the alias substitutes the node by Rewrite() before the actual
// search. Searcher() resolves the node locally for callers that drive a
// query directly.
type MoreLikeThisQuery struct {
	// Fields are the text fields the input and the candidates are compared
	// on. With a per-field LikeTextByField they default to its fields.
	Fields []string `json:"fields"`
	// LikeIDs are the IDs of the documents to be similar to.
	LikeIDs []string `json:"like_ids,omitempty"`
	// LikeText is raw text to be similar to, for input not in the index; it
	// is compared on every field. In JSON it is the string form of
	// "like_text".
	LikeText string `json:"-"`
	// LikeTextByField is raw text per field, for input not in the index
	// whose fields differ, such as a title and a body. In JSON it is the
	// object form of "like_text".
	LikeTextByField map[string]string `json:"-"`
	// MinTermFreq drops terms occurring fewer times in the input.
	MinTermFreq int `json:"min_term_freq"`
	// MinDocFreq drops terms found in fewer documents of the index.
	MinDocFreq int `json:"min_doc_freq"`
	// MaxQueryTerms is the number of top-ranked terms kept for the query.
	MaxQueryTerms int `json:"max_query_terms"`
	// Min is how many of the kept terms a hit must match, as a count or a
	// percentage of the kept terms; the default is 30%.
	Min MinShouldMatch `json:"min"`
	// BoostTerms, when greater than 0, boosts every kept term by its tf*idf
	// weight relative to the best term, times this factor; 0 leaves the
	// terms unboosted, as in Elasticsearch.
	BoostTerms float64 `json:"boost_terms,omitempty"`
	BoostVal   *Boost  `json:"boost,omitempty"`
}

// NewMoreLikeThisQuery creates a query for documents similar to the given
// document IDs, compared on the given fields, with the default thresholds.
func NewMoreLikeThisQuery(fields []string, likeIDs []string) *MoreLikeThisQuery {
	return &MoreLikeThisQuery{
		Fields:        fields,
		LikeIDs:       likeIDs,
		MinTermFreq:   DefaultMoreLikeThisMinTermFreq,
		MinDocFreq:    DefaultMoreLikeThisMinDocFreq,
		MaxQueryTerms: DefaultMoreLikeThisMaxQueryTerms,
		Min:           DefaultMoreLikeThisMin,
	}
}

func (q *MoreLikeThisQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *MoreLikeThisQuery) Boost() float64 {
	return q.BoostVal.Value()
}

// SetMin sets the number of kept terms a hit must match.
func (q *MoreLikeThisQuery) SetMin(count float64) {
	q.Min = MinShouldMatch(strconv.FormatFloat(count, 'f', -1, 64))
}

// SetMinPercent sets the share of the kept terms a hit must match.
func (q *MoreLikeThisQuery) SetMinPercent(pct float64) {
	q.Min = MinShouldMatch(strconv.FormatFloat(pct, 'f', -1, 64) + "%")
}

func (q *MoreLikeThisQuery) hasLikeText() bool {
	return q.LikeText != "" || len(q.LikeTextByField) > 0
}

// ComparedFields returns the fields to compare on: Fields, or with a
// per-field text and no Fields, the fields that have a text.
func (q *MoreLikeThisQuery) ComparedFields() []string {
	if len(q.Fields) > 0 || len(q.LikeTextByField) == 0 {
		return q.Fields
	}
	rv := make([]string, 0, len(q.LikeTextByField))
	for field := range q.LikeTextByField {
		rv = append(rv, field)
	}
	sort.Strings(rv)
	return rv
}

func (q *MoreLikeThisQuery) Validate() error {
	fields := q.ComparedFields()
	if len(fields) == 0 {
		return fmt.Errorf("more_like_this query must specify fields")
	}
	for _, field := range fields {
		if field == "" || field == "_all" || field == "_id" {
			return fmt.Errorf("more_like_this query cannot compare on field %q", field)
		}
	}
	if len(q.LikeIDs) == 0 && !q.hasLikeText() {
		return fmt.Errorf("more_like_this query must specify like_ids or like_text")
	}
	if len(q.LikeIDs) > 0 && q.hasLikeText() {
		return fmt.Errorf("more_like_this query accepts either like_ids or like_text, not both")
	}
	if q.LikeText != "" && len(q.LikeTextByField) > 0 {
		return fmt.Errorf("more_like_this like_text is either one text or a text per field, not both")
	}
	for field, text := range q.LikeTextByField {
		if strings.TrimSpace(text) == "" {
			return fmt.Errorf("more_like_this like_text for field %q is empty", field)
		}
	}
	if len(q.LikeTextByField) > 0 && len(q.Fields) > 0 {
		known := make(map[string]struct{}, len(q.Fields))
		for _, field := range q.Fields {
			known[field] = struct{}{}
		}
		for field := range q.LikeTextByField {
			if _, ok := known[field]; !ok {
				return fmt.Errorf("more_like_this like_text names field %q which is not in fields", field)
			}
		}
	}
	if q.MaxQueryTerms <= 0 {
		return fmt.Errorf("more_like_this max_query_terms must be greater than 0")
	}
	if q.MinTermFreq < 0 || q.MinDocFreq < 0 || q.BoostTerms < 0 {
		return fmt.Errorf("more_like_this min_term_freq, min_doc_freq and boost_terms must not be negative")
	}
	if err := q.Min.Validate(); err != nil {
		return fmt.Errorf("more_like_this %v", err)
	}
	return nil
}

// ValidateFields checks, when the mapping can tell, that every compared
// field is a text field with doc values. Without doc values the terms of a
// document can only be recovered by scanning the whole index, which is not
// what a similarity query should silently do.
func (q *MoreLikeThisQuery) ValidateFields(m mapping.IndexMapping) error {
	im, ok := m.(*mapping.IndexMappingImpl)
	if !ok {
		return nil
	}
	// doc values are what a source document's terms are read from; a
	// like_text only analyzes its input and reads the term dictionary
	needDocValues := len(q.LikeIDs) > 0
	nm, _ := mapping.IndexMapping(im).(mapping.NestedMapping)
	for _, field := range q.ComparedFields() {
		if nm != nil {
			// the values of a nested field live in child documents, which
			// the source document's doc values do not cover
			fs := search.NewFieldSet()
			fs.AddField(field)
			if _, depth := nm.NestedDepth(fs); depth > 0 {
				return fmt.Errorf("more_like_this: field %q is nested, which is not supported", field)
			}
		}
		fm := im.FieldMappingForPath(field)
		switch {
		case fm.Type == "":
			// not explicitly mapped: indexed dynamically, if at all
			if needDocValues && !im.DocValuesDynamic {
				return fmt.Errorf("more_like_this: field %q has no doc values", field)
			}
		case fm.Type != "text":
			return fmt.Errorf("more_like_this: field %q is not a text field", field)
		case needDocValues && !fm.DocValues:
			return fmt.Errorf("more_like_this: field %q has no doc values", field)
		}
	}
	return nil
}

// Searcher resolves the query against a single index reader: it collects
// the term statistics from it, rewrites itself into standard queries and
// returns their searcher.
func (q *MoreLikeThisQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping,
	options search.SearcherOptions) (search.Searcher, error) {
	stats, err := q.CollectStats(ctx, i, m)
	if err != nil {
		return nil, err
	}
	return q.Rewrite(stats, RewriteOptions{BM25: isBM25ScoringModel(m)}).Searcher(ctx, i, m, options)
}

// CollectStats gathers from one index reader what the rewrite needs.
//
// With LikeIDs, every source document held by this reader is reported with
// its terms in the compared fields, read from doc values; each term carries
// its frequency in that document (tf, from the term's posting entry), the
// number of documents of this reader containing it (df, from the term
// dictionary) and the reader's document count. With LikeText, the text is
// analyzed with each field's analyzer, tf is counted from the tokens, and
// df and the document count are looked up here.
func (q *MoreLikeThisQuery) CollectStats(ctx context.Context, r index.IndexReader,
	m mapping.IndexMapping) (*search.MLTStats, error) {
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if err := q.ValidateFields(m); err != nil {
		return nil, err
	}
	docCount, err := r.DocCount()
	if err != nil {
		return nil, err
	}
	rv := &search.MLTStats{}
	fields := q.ComparedFields()

	if q.hasLikeText() {
		byKey := make(map[string]*search.MLTTermStat)
		for _, field := range fields {
			text := q.LikeText
			if len(q.LikeTextByField) > 0 {
				text = q.LikeTextByField[field]
			}
			if text == "" {
				continue
			}
			analyzer := m.AnalyzerNamed(m.AnalyzerNameForPath(field))
			if analyzer == nil {
				return nil, fmt.Errorf("more_like_this: no analyzer for field %q", field)
			}
			for _, token := range analyzer.Analyze([]byte(text)) {
				key := field + "\x00" + string(token.Term)
				if st, ok := byKey[key]; ok {
					st.TF++
					continue
				}
				st := &search.MLTTermStat{Field: field, Term: string(token.Term), TF: 1, DocCount: docCount}
				byKey[key] = st
				rv.Terms = append(rv.Terms, st)
			}
		}
		for _, st := range rv.Terms {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			st.DF, err = docFreq(ctx, r, st.Field, st.Term)
			if err != nil {
				return nil, err
			}
		}
		return rv, nil
	}

	dvReader, err := r.DocValueReader(fields)
	if err != nil {
		return nil, err
	}
	defer func() {
		reportBytesRead(ctx, dvReader.BytesRead())
	}()
	docIDReader, err := r.DocIDReaderOnly(q.LikeIDs)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = docIDReader.Close()
	}()
	for {
		internalID, err := docIDReader.Next()
		if err != nil {
			return nil, err
		}
		if internalID == nil {
			break
		}
		internalID = append(index.IndexInternalID(nil), internalID...)
		id, err := r.ExternalID(internalID)
		if err != nil {
			return nil, err
		}
		doc := &search.MLTSourceDoc{ID: id}
		type fieldTerm struct{ field, term string }
		var docTerms []fieldTerm
		seen := make(map[fieldTerm]struct{})
		err = dvReader.VisitDocValues(internalID, func(field string, term []byte) {
			ft := fieldTerm{field, string(term)}
			if _, dup := seen[ft]; !dup {
				seen[ft] = struct{}{}
				docTerms = append(docTerms, ft)
			}
		})
		if err != nil {
			return nil, err
		}
		for _, ft := range docTerms {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			tf, df, err := termStats(ctx, r, ft.field, ft.term, internalID)
			if err != nil {
				return nil, err
			}
			doc.Terms = append(doc.Terms, &search.MLTTermStat{
				Field: ft.field, Term: ft.term, TF: tf, DF: df, DocCount: docCount,
			})
		}
		rv.SourceDocs = append(rv.SourceDocs, doc)
		rv.FoundIDs = append(rv.FoundIDs, id)
	}
	return rv, nil
}

// reportBytesRead accounts doc value reads towards the search cost, the way
// the collector does for the doc values of the hits.
func reportBytesRead(ctx context.Context, bytes uint64) {
	if bytes == 0 {
		return
	}
	if fn, ok := ctx.Value(search.SearchIOStatsCallbackKey).(search.SearchIOStatsCallbackFunc); ok {
		fn(bytes)
		search.RecordSearchCost(ctx, search.AddM, bytes)
	}
}

// termStats reads, from the posting list of the term, its frequency in the
// given document and the number of documents containing it.
func termStats(ctx context.Context, r index.IndexReader, field, term string,
	docID index.IndexInternalID) (tf, df uint64, err error) {
	tfr, err := r.TermFieldReader(ctx, []byte(term), field, true, false, false)
	if err != nil {
		return 0, 0, err
	}
	defer func() {
		if cerr := tfr.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	tf = 1
	tfd, err := tfr.Advance(docID, nil)
	if err != nil {
		return 0, 0, err
	}
	if tfd != nil && tfd.ID.Equals(docID) && tfd.Freq > 0 {
		tf = tfd.Freq
	}
	return tf, tfr.Count(), nil
}

// docFreq reads the number of documents containing the term.
func docFreq(ctx context.Context, r index.IndexReader, field, term string) (df uint64, err error) {
	tfr, err := r.TermFieldReader(ctx, []byte(term), field, false, false, false)
	if err != nil {
		return 0, err
	}
	defer func() {
		if cerr := tfr.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	return tfr.Count(), nil
}

// RewriteOptions controls how a more_like_this node is rewritten.
type RewriteOptions struct {
	// BM25 selects the BM25 form of idf for ranking the terms, matching an
	// index whose scoring model is BM25.
	BM25 bool
	// Notes attaches to the generated queries where their terms and
	// boosts came from; set when the rewritten query is returned.
	Notes bool
}

// MLTTermNotes explains a term of a rewritten more_like_this query.
type MLTTermNotes struct {
	// Sources are the source documents the term was found in.
	Sources []string `json:"sources,omitempty"`
	TF      uint64   `json:"tf"`
	DF      uint64   `json:"df"`
	N       uint64   `json:"n"`
	IDF     float64  `json:"idf"`
	Weight  float64  `json:"weight"`
}

// MLTQueryNotes explains the disjunction generated for a more_like_this
// node.
type MLTQueryNotes struct {
	Fields     []string          `json:"fields"`
	LikeIDs    []string          `json:"like_ids,omitempty"`
	LikeText   string            `json:"like_text,omitempty"`
	LikeTexts  map[string]string `json:"like_text_by_field,omitempty"`
	Candidates int               `json:"candidate_terms"`
	Kept       int               `json:"kept_terms"`
	Min        float64           `json:"min"`
	BoostTerms float64           `json:"boost_terms,omitempty"`
}

// Rewrite turns the collected statistics into the equivalent query built
// from standard query types: a disjunction of the top MaxQueryTerms terms
// by tf*idf, with Min resolved against the number of terms kept, and the
// source documents excluded through a must_not clause. With BoostTerms set
// each term is boosted by its weight relative to the best term; the query
// boost applies to every term either way. Missing or empty statistics
// yield a match_none query.
func (q *MoreLikeThisQuery) Rewrite(stats *search.MLTStats, opts RewriteOptions) Query {
	candidates := stats.Candidates()
	if len(candidates) == 0 {
		return NewMatchNoneQuery()
	}
	type weighted struct {
		stat   *search.MLTTermStat
		idf    float64
		weight float64
	}
	kept := make([]weighted, 0, len(candidates))
	for _, st := range candidates {
		if st.DF == 0 || st.TF < uint64(q.MinTermFreq) || st.DF < uint64(q.MinDocFreq) {
			continue
		}
		idf := mltIDF(opts.BM25, st.DocCount, st.DF)
		kept = append(kept, weighted{stat: st, idf: idf, weight: float64(st.TF) * idf})
	}
	sort.SliceStable(kept, func(i, j int) bool {
		if kept[i].weight != kept[j].weight {
			return kept[i].weight > kept[j].weight
		}
		if kept[i].stat.Field != kept[j].stat.Field {
			return kept[i].stat.Field < kept[j].stat.Field
		}
		return kept[i].stat.Term < kept[j].stat.Term
	})
	if len(kept) > q.MaxQueryTerms {
		kept = kept[:q.MaxQueryTerms]
	}
	if len(kept) == 0 {
		return NewMatchNoneQuery()
	}

	queryBoost := q.BoostVal.Value()
	bestWeight := kept[0].weight
	disjunction := NewDisjunctionQuery(nil)
	for _, w := range kept {
		tq := NewTermQuery(w.stat.Term)
		tq.SetField(w.stat.Field)
		boost := queryBoost
		if q.BoostTerms > 0 && bestWeight > 0 {
			boost *= q.BoostTerms * w.weight / bestWeight
		}
		if boost != 1 {
			tq.SetBoost(boost)
		}
		if opts.Notes {
			notes, _ := util.MarshalJSON(&MLTTermNotes{
				Sources: w.stat.Sources,
				TF:      w.stat.TF,
				DF:      w.stat.DF,
				N:       w.stat.DocCount,
				IDF:     w.idf,
				Weight:  w.weight,
			})
			tq.Notes = Notes(notes)
		}
		disjunction.AddQuery(tq)
	}
	min := q.Min.Resolve(len(kept))
	disjunction.SetMin(min)
	if opts.Notes {
		notes, _ := util.MarshalJSON(&MLTQueryNotes{
			Fields:     q.ComparedFields(),
			LikeIDs:    q.LikeIDs,
			LikeText:   q.LikeText,
			LikeTexts:  q.LikeTextByField,
			Candidates: len(candidates),
			Kept:       len(kept),
			Min:        min,
			BoostTerms: q.BoostTerms,
		})
		disjunction.Notes = Notes(notes)
	}

	if len(q.LikeIDs) == 0 {
		return disjunction
	}
	return NewBooleanQuery([]Query{disjunction}, nil, []Query{NewDocIDQuery(q.LikeIDs)})
}

// mltIDF mirrors the idf of the index's scoring model (see
// scorer.TermQueryScorer.computeIDF) so that term selection ranks the way
// the terms will later be scored.
func mltIDF(bm25 bool, docCount, docFreq uint64) float64 {
	if docCount == 0 {
		return 0
	}
	if bm25 {
		return math.Log(1 + (float64(docCount)-float64(docFreq)+0.5)/(float64(docFreq)+0.5))
	}
	return 1.0 + math.Log(float64(docCount)/float64(docFreq+1))
}

func isBM25ScoringModel(m mapping.IndexMapping) bool {
	if im, ok := m.(*mapping.IndexMappingImpl); ok {
		return im.ScoringModel == index.BM25Scoring
	}
	return false
}

// moreLikeThisJSON is the wire shape of the node under the "more_like_this"
// key; like_text is kept raw since it is a string or an object.
type moreLikeThisJSON struct {
	Fields        []string        `json:"fields,omitempty"`
	LikeIDs       []string        `json:"like_ids,omitempty"`
	LikeText      json.RawMessage `json:"like_text,omitempty"`
	MinTermFreq   int             `json:"min_term_freq"`
	MinDocFreq    int             `json:"min_doc_freq"`
	MaxQueryTerms int             `json:"max_query_terms"`
	Min           MinShouldMatch  `json:"min"`
	BoostTerms    float64         `json:"boost_terms,omitempty"`
	BoostVal      *Boost          `json:"boost,omitempty"`
}

func (q *MoreLikeThisQuery) MarshalJSON() ([]byte, error) {
	inner := moreLikeThisJSON{
		Fields:        q.Fields,
		LikeIDs:       q.LikeIDs,
		MinTermFreq:   q.MinTermFreq,
		MinDocFreq:    q.MinDocFreq,
		MaxQueryTerms: q.MaxQueryTerms,
		Min:           q.Min,
		BoostTerms:    q.BoostTerms,
		BoostVal:      q.BoostVal,
	}
	var err error
	switch {
	case len(q.LikeTextByField) > 0:
		inner.LikeText, err = util.MarshalJSON(q.LikeTextByField)
	case q.LikeText != "":
		inner.LikeText, err = util.MarshalJSON(q.LikeText)
	}
	if err != nil {
		return nil, err
	}
	return util.MarshalJSON(map[string]*moreLikeThisJSON{
		"more_like_this": &inner,
	})
}

func (q *MoreLikeThisQuery) UnmarshalJSON(data []byte) error {
	defaults := NewMoreLikeThisQuery(nil, nil)
	inner := moreLikeThisJSON{
		MinTermFreq:   defaults.MinTermFreq,
		MinDocFreq:    defaults.MinDocFreq,
		MaxQueryTerms: defaults.MaxQueryTerms,
		Min:           defaults.Min,
	}
	tmp := struct {
		MLT *moreLikeThisJSON `json:"more_like_this"`
	}{MLT: &inner}
	if err := util.UnmarshalJSON(data, &tmp); err != nil {
		return err
	}
	if tmp.MLT == nil {
		return fmt.Errorf("more_like_this must be an object")
	}
	rv := MoreLikeThisQuery{
		Fields:        inner.Fields,
		LikeIDs:       inner.LikeIDs,
		MinTermFreq:   inner.MinTermFreq,
		MinDocFreq:    inner.MinDocFreq,
		MaxQueryTerms: inner.MaxQueryTerms,
		Min:           inner.Min,
		BoostTerms:    inner.BoostTerms,
		BoostVal:      inner.BoostVal,
	}
	if likeText := bytes.TrimSpace(inner.LikeText); len(likeText) > 0 && string(likeText) != "null" {
		if likeText[0] == '{' {
			if err := util.UnmarshalJSON(likeText, &rv.LikeTextByField); err != nil {
				return fmt.Errorf("more_like_this like_text: %v", err)
			}
		} else if err := util.UnmarshalJSON(likeText, &rv.LikeText); err != nil {
			return fmt.Errorf("more_like_this like_text must be a string or an object keyed by field: %v", err)
		}
	}
	*q = rv
	return nil
}

// ExtractMoreLikeThisQueries returns the more_like_this nodes of a query
// tree in a fixed depth-first order, so that callers on different sides of
// a preSearch round can address the same node by its position.
func ExtractMoreLikeThisQueries(q Query) []*MoreLikeThisQuery {
	return collectMoreLikeThis(q, nil)
}

func collectMoreLikeThis(q Query, acc []*MoreLikeThisQuery) []*MoreLikeThisQuery {
	switch q := q.(type) {
	case *MoreLikeThisQuery:
		return append(acc, q)
	case *BooleanQuery:
		for _, sub := range []Query{q.Must, q.Should, q.MustNot, q.Filter} {
			acc = collectMoreLikeThis(sub, acc)
		}
	case *ConjunctionQuery:
		for _, sub := range q.Conjuncts {
			acc = collectMoreLikeThis(sub, acc)
		}
	case *DisjunctionQuery:
		for _, sub := range q.Disjuncts {
			acc = collectMoreLikeThis(sub, acc)
		}
	case *CustomFilterQuery:
		acc = collectMoreLikeThis(q.Query, acc)
	case *CustomScoreQuery:
		acc = collectMoreLikeThis(q.Query, acc)
	}
	return acc
}

// ReplaceMoreLikeThisQueries returns a copy of the query tree in which every
// more_like_this node is substituted by the query the callback returns for
// it. Nodes are numbered in the order ExtractMoreLikeThisQueries lists them.
// The input tree is left untouched.
func ReplaceMoreLikeThisQueries(q Query,
	replace func(idx int, mlt *MoreLikeThisQuery) (Query, error)) (Query, error) {
	idx := 0
	return replaceMoreLikeThis(q, &idx, replace)
}

func replaceMoreLikeThis(q Query, idx *int,
	replace func(idx int, mlt *MoreLikeThisQuery) (Query, error)) (Query, error) {
	var err error
	switch q := q.(type) {
	case *MoreLikeThisQuery:
		i := *idx
		*idx++
		return replace(i, q)
	case *BooleanQuery:
		rv := *q
		if rv.Must, err = replaceMoreLikeThis(q.Must, idx, replace); err != nil {
			return nil, err
		}
		if rv.Should, err = replaceMoreLikeThis(q.Should, idx, replace); err != nil {
			return nil, err
		}
		if rv.MustNot, err = replaceMoreLikeThis(q.MustNot, idx, replace); err != nil {
			return nil, err
		}
		if rv.Filter, err = replaceMoreLikeThis(q.Filter, idx, replace); err != nil {
			return nil, err
		}
		return &rv, nil
	case *ConjunctionQuery:
		rv := *q
		rv.Conjuncts = make([]Query, len(q.Conjuncts))
		for i, sub := range q.Conjuncts {
			if rv.Conjuncts[i], err = replaceMoreLikeThis(sub, idx, replace); err != nil {
				return nil, err
			}
		}
		return &rv, nil
	case *DisjunctionQuery:
		rv := *q
		rv.Disjuncts = make([]Query, len(q.Disjuncts))
		for i, sub := range q.Disjuncts {
			if rv.Disjuncts[i], err = replaceMoreLikeThis(sub, idx, replace); err != nil {
				return nil, err
			}
		}
		return &rv, nil
	case *CustomFilterQuery:
		rv := *q
		if rv.Query, err = replaceMoreLikeThis(q.Query, idx, replace); err != nil {
			return nil, err
		}
		return &rv, nil
	case *CustomScoreQuery:
		rv := *q
		if rv.Query, err = replaceMoreLikeThis(q.Query, idx, replace); err != nil {
			return nil, err
		}
		return &rv, nil
	}
	return q, nil
}
