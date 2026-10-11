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
	"context"
	"fmt"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

// PerSegmentQuery is implemented by the queries that the per segment search path
// can serve (see SupportsPerSegment): they build the searcher of that path, which
// is not a search.Searcher, as another way to be searched than Searcher.
//
// PerSegmentSearcher returns search.ErrPerSegmentUnsupported when this query, on
// this index, can't be served that way after all; the caller searches it with
// Searcher then. A searcher that is returned is closed by the caller; one that is
// not (an error) is closed by the query that built it.
type PerSegmentQuery interface {
	Query
	PerSegmentSearcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping,
		options search.SearcherOptions) (search.PerSegmentSearcher, error)
}

var (
	_ PerSegmentQuery = (*TermQuery)(nil)
	_ PerSegmentQuery = (*BoolFieldQuery)(nil)
	_ PerSegmentQuery = (*MatchQuery)(nil)
	_ PerSegmentQuery = (*ConjunctionQuery)(nil)
	_ PerSegmentQuery = (*DisjunctionQuery)(nil)
	_ PerSegmentQuery = (*BooleanQuery)(nil)
	_ PerSegmentQuery = (*QueryStringQuery)(nil)
)

// perSegmentSearcherOf is the per segment searcher of the query, which has to be
// one that has such a searcher.
func perSegmentSearcherOf(ctx context.Context, q Query, i index.IndexReader, m mapping.IndexMapping,
	options search.SearcherOptions) (search.PerSegmentSearcher, error) {
	pq, ok := q.(PerSegmentQuery)
	if !ok {
		return nil, search.ErrPerSegmentUnsupported
	}
	return pq.PerSegmentSearcher(ctx, i, m, options)
}

// closePerSegmentSearchers closes the searchers a query built, when it can't
// go on to make the searcher of its own.
func closePerSegmentSearchers(ss []search.PerSegmentSearcher) {
	for _, s := range ss {
		if s != nil {
			_ = s.Close()
		}
	}
}

// PerSegmentSearcher implements PerSegmentQuery.
func (q *TermQuery) PerSegmentSearcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping,
	options search.SearcherOptions) (search.PerSegmentSearcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}
	s, err := searcher.NewPerSegmentTermSearcher(ctx, i, q.Term, field, q.BoostVal.Value(), options)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// PerSegmentSearcher implements PerSegmentQuery.
func (q *BoolFieldQuery) PerSegmentSearcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping,
	options search.SearcherOptions) (search.PerSegmentSearcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}
	term := "F"
	if q.Bool {
		term = "T"
	}
	s, err := searcher.NewPerSegmentTermSearcher(ctx, i, term, field, q.BoostVal.Value(), options)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// PerSegmentSearcher implements PerSegmentQuery. A fuzzy match is made of fuzzy
// queries, not of terms, which is not served.
func (q *MatchQuery) PerSegmentSearcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping,
	options search.SearcherOptions) (search.PerSegmentSearcher, error) {
	if q.Fuzziness != 0 || q.autoFuzzy {
		return nil, search.ErrPerSegmentUnsupported
	}
	field, tokens, err := q.analyze(m)
	if err != nil {
		return nil, err
	}
	if len(tokens) == 0 {
		return searcher.NewPerSegmentMatchNoneSearcher(), nil
	}

	tqs := make([]Query, len(tokens))
	for n, token := range tokens {
		tq := NewTermQuery(string(token.Term))
		tq.SetField(field)
		tq.SetBoost(q.BoostVal.Value())
		tqs[n] = tq
	}

	switch q.Operator {
	case MatchQueryOperatorOr:
		shouldQuery := NewDisjunctionQuery(tqs)
		shouldQuery.SetMin(1)
		shouldQuery.SetBoost(q.BoostVal.Value())
		return shouldQuery.PerSegmentSearcher(ctx, i, m, options)

	case MatchQueryOperatorAnd:
		mustQuery := NewConjunctionQuery(tqs)
		mustQuery.SetBoost(q.BoostVal.Value())
		return mustQuery.PerSegmentSearcher(ctx, i, m, options)

	default:
		return nil, fmt.Errorf("unhandled operator %d", q.Operator)
	}
}

// PerSegmentSearcher implements PerSegmentQuery. In nested mode the conjunction
// is of another kind, which is not served.
func (q *ConjunctionQuery) PerSegmentSearcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping,
	options search.SearcherOptions) (search.PerSegmentSearcher, error) {
	if nested, _ := ctx.Value(search.NestedSearchKey).(bool); nested {
		return nil, search.ErrPerSegmentUnsupported
	}
	ss := make([]search.PerSegmentSearcher, 0, len(q.Conjuncts))
	for _, conjunct := range q.Conjuncts {
		sr, err := perSegmentSearcherOf(ctx, conjunct, i, m, options)
		if err != nil {
			closePerSegmentSearchers(ss)
			return nil, err
		}
		if _, ok := sr.(*searcher.PerSegmentMatchNoneSearcher); ok && q.queryStringMode {
			// in query string mode, skip match none
			continue
		}
		ss = append(ss, sr)
	}

	if len(ss) < 1 {
		return searcher.NewPerSegmentMatchNoneSearcher(), nil
	}
	rv, err := searcher.NewPerSegmentConjunctionSearcher(ss, options)
	if err != nil {
		closePerSegmentSearchers(ss)
		return nil, err
	}
	return rv, nil
}

// PerSegmentSearcher implements PerSegmentQuery. A disjunction that breaks the
// scores down by clause is for KNN, which the per segment path is not made for.
func (q *DisjunctionQuery) PerSegmentSearcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping,
	options search.SearcherOptions) (search.PerSegmentSearcher, error) {
	if q.retrieveScoreBreakdown {
		return nil, search.ErrPerSegmentUnsupported
	}
	ss := make([]search.PerSegmentSearcher, 0, len(q.Disjuncts))
	for _, disjunct := range q.Disjuncts {
		sr, err := perSegmentSearcherOf(ctx, disjunct, i, m, options)
		if err != nil {
			closePerSegmentSearchers(ss)
			return nil, err
		}
		if _, ok := sr.(*searcher.PerSegmentMatchNoneSearcher); ok && q.queryStringMode {
			// in query string mode, skip match none
			continue
		}
		ss = append(ss, sr)
	}

	if len(ss) < 1 {
		return searcher.NewPerSegmentMatchNoneSearcher(), nil
	}
	rv, err := searcher.NewPerSegmentDisjunctionSearcher(ss, q.Min, options)
	if err != nil {
		closePerSegmentSearchers(ss)
		return nil, err
	}
	return rv, nil
}

// PerSegmentSearcher implements PerSegmentQuery. A filter is a searcher that is
// walked a doc at a time alongside, and a boolean with nothing to start from but
// what it excludes needs a searcher of all the docs: neither is served.
func (q *BooleanQuery) PerSegmentSearcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping,
	options search.SearcherOptions) (search.PerSegmentSearcher, error) {
	if q.Filter != nil || (q.Must == nil && q.Should == nil) {
		return nil, search.ErrPerSegmentUnsupported
	}

	// a clause that can't match is left out, like the regular searchers do
	build := func(clause Query, opts search.SearcherOptions) (search.PerSegmentSearcher, error) {
		if clause == nil {
			return nil, nil
		}
		sr, err := perSegmentSearcherOf(ctx, clause, i, m, opts)
		if err != nil {
			return nil, err
		}
		if _, ok := sr.(*searcher.PerSegmentMatchNoneSearcher); ok {
			return nil, nil
		}
		return sr, nil
	}

	// what is excluded is not scored, which spares decoding the frequencies and
	// norms of its postings
	mustNotOptions := options
	mustNotOptions.Score = "none"
	mustNot, err := build(q.MustNot, mustNotOptions)
	if err != nil {
		return nil, err
	}
	must, err := build(q.Must, options)
	if err != nil {
		closePerSegmentSearchers([]search.PerSegmentSearcher{mustNot})
		return nil, err
	}
	should, err := build(q.Should, options)
	if err != nil {
		closePerSegmentSearchers([]search.PerSegmentSearcher{mustNot, must})
		return nil, err
	}

	switch {
	case must == nil && should == nil && mustNot == nil:
		return searcher.NewPerSegmentMatchNoneSearcher(), nil
	case must != nil && should == nil && mustNot == nil:
		return must, nil // just the one clause
	case must == nil && should != nil && mustNot == nil:
		return should, nil
	case must == nil && should == nil:
		// only what is excluded: it needs all the docs to start from
		closePerSegmentSearchers([]search.PerSegmentSearcher{mustNot})
		return nil, search.ErrPerSegmentUnsupported
	}
	rv, err := searcher.NewPerSegmentBooleanSearcher(must, should, mustNot, options)
	if err != nil {
		closePerSegmentSearchers([]search.PerSegmentSearcher{must, should, mustNot})
		return nil, err
	}
	return rv, nil
}

// PerSegmentSearcher implements PerSegmentQuery: the string is parsed into the
// query that is searched.
func (q *QueryStringQuery) PerSegmentSearcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping,
	options search.SearcherOptions) (search.PerSegmentSearcher, error) {
	newQuery, err := parseQuerySyntax(q.Query)
	if err != nil {
		return nil, err
	}
	return perSegmentSearcherOf(ctx, newQuery, i, m, options)
}
