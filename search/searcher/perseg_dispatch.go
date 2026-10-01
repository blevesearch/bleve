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

package searcher

import (
	"context"
	"errors"

	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// Whether a search takes the per segment path is decided by the searchers, not by
// the queries that build them. A request that can be served that way says so in
// its context (search.PerSegmentSearchKey); then
//
//   - NewTermSearcherBytes, which every query that searches a term goes through,
//     builds a per segment term searcher if the index reader can provide one;
//   - the constructors of the conjunction, disjunction and boolean searchers build
//     the per segment kind when what they are given is made of those.
//
// A query that makes its searchers through these needs to know nothing of it.
// Whether the tree of searchers is made of the per segment kind throughout is
// the index reader's call and, for a whole index snapshot, all or nothing.

// ErrPerSegmentMixed is returned when searchers of the per segment kind and
// regular ones are to be combined, which no combination can run. Queries
// opt in only for shapes whose searchers are per segment ones throughout,
// whenever the index reader can do it at all, so this is a bug if it's seen.
var ErrPerSegmentMixed = errors.New("searcher: per segment searchers cannot be combined with regular ones")

func perSegmentOptedIn(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	enabled, _ := ctx.Value(search.PerSegmentSearchKey).(bool)
	return enabled
}

// perSegmentTermSearcherFor is the per segment searcher of a term if the caller
// has opted in and the index reader can provide one, and nil otherwise, in which
// case the regular term searcher is what has to be built.
func perSegmentTermSearcherFor(ctx context.Context, indexReader index.IndexReader,
	term []byte, field string, boost float64, options search.SearcherOptions) (search.Searcher, error) {
	if !perSegmentOptedIn(ctx) {
		return nil, nil
	}
	psReader, ok := indexReader.(PerSegmentIndexReader)
	if !ok {
		return nil, nil
	}
	rv, err := NewPerSegmentTermSearcher(ctx, psReader, string(term), field, boost, options)
	if err != nil || rv == nil {
		return nil, err // not a nil *PerSegmentTermSearcher in a search.Searcher
	}
	return rv, nil
}

// isNoMatch reports whether a regular searcher can't match anything: one of no
// matches, a conjunction that has such a clause, a disjunction that has no other.
func isNoMatch(q search.Searcher) bool {
	switch s := q.(type) {
	case *MatchNoneSearcher:
		return true
	case *ConjunctionSearcher:
		for _, c := range s.searchers {
			if isNoMatch(c) {
				return true
			}
		}
	case *DisjunctionSliceSearcher:
		return allNoMatch(s.searchers)
	case *DisjunctionHeapSearcher:
		return allNoMatch(s.searchers)
	}
	return false
}

func allNoMatch(qs []search.Searcher) bool {
	for _, q := range qs {
		if !isNoMatch(q) {
			return false
		}
	}
	return len(qs) > 0
}

// perSegmentKind says what the searchers are, for combining them into a
// composite: of the per segment kind, or regular. A searcher that can't match
// anything fits in with either kind, so it doesn't count for one.
func perSegmentKind(qsearchers []search.Searcher) (perSegment, regular int) {
	for _, q := range qsearchers {
		switch q.(type) {
		case *MatchNoneSearcher:
			// goes with either
		case perSegChild:
			perSegment++
		default:
			if !isNoMatch(q) {
				regular++
			}
		}
	}
	return perSegment, regular
}

// withNoMatchAsMatchNone gives the searchers, but for those that can't match
// anything and are regular ones, which are made into searchers of no matches --
// the one kind of regular searcher that can be a clause of a composite of the
// per segment kind.
func withNoMatchAsMatchNone(indexReader index.IndexReader, qsearchers []search.Searcher) ([]search.Searcher, error) {
	var rv []search.Searcher
	for i, q := range qsearchers {
		if _, ok := q.(perSegChild); ok || !isNoMatch(q) {
			if rv != nil {
				rv[i] = q
			}
			continue
		}
		if rv == nil {
			rv = append([]search.Searcher(nil), qsearchers...)
		}
		none, err := NewMatchNoneSearcher(indexReader)
		if err != nil {
			return nil, err
		}
		_ = q.Close()
		rv[i] = none
	}
	if rv == nil {
		return qsearchers, nil
	}
	return rv, nil
}

func closeSearchers(qsearchers []search.Searcher) {
	for _, q := range qsearchers {
		if q != nil {
			_ = q.Close()
		}
	}
}

// perSegmentConjunction is the conjunction of the searchers if it has to be of the
// per segment kind, nil if it doesn't.
func perSegmentConjunction(ctx context.Context, indexReader index.IndexReader, qsearchers []search.Searcher,
	options search.SearcherOptions) (search.Searcher, error) {
	if !perSegmentOptedIn(ctx) || len(qsearchers) == 0 {
		return nil, nil
	}
	perSegment, regular := perSegmentKind(qsearchers)
	switch {
	case perSegment == 0:
		return nil, nil // regular searchers throughout
	case regular > 0:
		closeSearchers(qsearchers)
		return nil, ErrPerSegmentMixed
	}
	clauses, err := withNoMatchAsMatchNone(indexReader, qsearchers)
	if err != nil {
		return nil, err
	}
	return NewPerSegmentConjunctionSearcher(clauses, options), nil
}

// perSegmentDisjunction is the disjunction of the searchers if it has to be of the
// per segment kind, nil if it doesn't. limit is whether the number of clauses is
// to be kept to DisjunctionMaxClauseCount.
func perSegmentDisjunction(ctx context.Context, indexReader index.IndexReader, qsearchers []search.Searcher,
	min float64, options search.SearcherOptions, limit bool) (search.Searcher, error) {
	if !perSegmentOptedIn(ctx) || len(qsearchers) == 0 {
		return nil, nil
	}
	// scores broken down by clause are for KNN, which this isn't made for
	if breakdown, _ := ctx.Value(search.IncludeScoreBreakdownKey).(bool); breakdown {
		return nil, nil
	}
	perSegment, regular := perSegmentKind(qsearchers)
	switch {
	case perSegment == 0:
		return nil, nil
	case regular > 0:
		closeSearchers(qsearchers)
		return nil, ErrPerSegmentMixed
	case limit && tooManyClauses(len(qsearchers)):
		// the very error the regular searchers give
		closeSearchers(qsearchers)
		return nil, tooManyClausesErr("", len(qsearchers))
	}
	clauses, err := withNoMatchAsMatchNone(indexReader, qsearchers)
	if err != nil {
		return nil, err
	}
	return NewPerSegmentDisjunctionSearcher(clauses, min, options), nil
}

// NewBooleanSearcherOrPerSegment is NewBooleanSearcher, or the boolean searcher of
// the per segment kind if the caller has opted in and the clauses are of that
// kind.
func NewBooleanSearcherOrPerSegment(ctx context.Context, indexReader index.IndexReader,
	mustSearcher, shouldSearcher, mustNotSearcher search.Searcher,
	options search.SearcherOptions) (search.Searcher, error) {
	if perSegmentOptedIn(ctx) {
		var clauses []search.Searcher
		for _, c := range []search.Searcher{mustSearcher, shouldSearcher, mustNotSearcher} {
			if c != nil {
				clauses = append(clauses, c)
			}
		}
		perSegment, regular := perSegmentKind(clauses)
		switch {
		case perSegment == 0:
			// regular searchers throughout
		case regular > 0:
			closeSearchers(clauses)
			return nil, ErrPerSegmentMixed
		default:
			// whether the should clause is required is the clause's to say, and
			// has to be asked before it's made into a searcher of no matches,
			// whose minimum is nothing
			shouldRequired := shouldSearcher != nil && shouldSearcher.Min() > 0

			// the clauses that can't match, made into the searchers that don't
			var err error
			for _, c := range []*search.Searcher{&mustSearcher, &shouldSearcher, &mustNotSearcher} {
				if *c == nil {
					continue
				}
				fixed, err2 := withNoMatchAsMatchNone(indexReader, []search.Searcher{*c})
				if err2 != nil {
					err = err2
					break
				}
				*c = fixed[0]
			}
			if err != nil {
				return nil, err
			}
			if rv := NewPerSegmentBooleanSearcher(mustSearcher, shouldSearcher, mustNotSearcher, options); rv != nil {
				rv.shouldRequired = shouldRequired
				return rv, nil
			}
			closeSearchers(clauses)
			return nil, ErrPerSegmentMixed
		}
	}
	bs, err := NewBooleanSearcher(ctx, indexReader, mustSearcher, shouldSearcher, mustNotSearcher, options)
	if err != nil {
		return nil, err
	}
	return bs, nil
}
