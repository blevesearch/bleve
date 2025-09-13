//  Copyright (c) 2014 Couchbase, Inc.
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
	"encoding/json"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
)

type ConjunctionQuery struct {
	Conjuncts       []Query `json:"conjuncts"`
	BoostVal        *Boost  `json:"boost,omitempty"`
	queryStringMode bool
}

// NewConjunctionQuery creates a new compound Query.
// Result documents must satisfy all of the queries.
func NewConjunctionQuery(conjuncts []Query) *ConjunctionQuery {
	return &ConjunctionQuery{
		Conjuncts: conjuncts,
	}
}

func (q *ConjunctionQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *ConjunctionQuery) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *ConjunctionQuery) AddQuery(aq ...Query) {
	q.Conjuncts = append(q.Conjuncts, aq...)
}

func (q *ConjunctionQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	// check if the mapping has any nested prefixes
	var nestedPrefixes search.FieldSet
	if nm, ok := m.(mapping.NestedMapping); ok {
		nestedPrefixes = nm.NestedPrefixes()
	}
	// if we have nested prefixes to check against, then we need to check
	// each subquery to see if it has any of those fields
	// if so, then we need to use a nested conjunction searcher
	// if not, then we can use a regular conjunction searcher
	// if there are no nested prefixes at all, then we can use a regular
	// conjunction searcher
	var useNestedSearcher bool
	ss := make([]search.Searcher, 0, len(q.Conjuncts))
	cleanup := func() {
		for _, searcher := range ss {
			if searcher != nil {
				_ = searcher.Close()
			}
		}
	}
	for _, conjunct := range q.Conjuncts {
		// if we haven't already determined we need a nested searcher,
		// and we have nested prefixes to check against, do so now
		if !useNestedSearcher && nestedPrefixes != nil {
			// once we know we need a nested searcher, no need to keep checking
			// the rest of the queries
			fs, err := ExtractFields(conjunct, m, nil)
			if err != nil {
				cleanup()
				return nil, err
			}
			if fs != nil && fs.IntersectsPrefix(nestedPrefixes) {
				useNestedSearcher = true
			}
		}
		sr, err := conjunct.Searcher(ctx, i, m, options)
		if err != nil {
			cleanup()
			return nil, err
		}
		if _, ok := sr.(*searcher.MatchNoneSearcher); ok && q.queryStringMode {
			// in query string mode, skip match none
			continue
		}
		ss = append(ss, sr)
	}

	if len(ss) < 1 {
		return searcher.NewMatchNoneSearcher(i)
	}

	if useNestedSearcher {
		return searcher.NewNestedConjunctionSearcher(ctx, i, ss, options)
	}

	return searcher.NewConjunctionSearcher(ctx, i, ss, options)
}

func (q *ConjunctionQuery) Validate() error {
	for _, q := range q.Conjuncts {
		if q, ok := q.(ValidatableQuery); ok {
			err := q.Validate()
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (q *ConjunctionQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Conjuncts []json.RawMessage `json:"conjuncts"`
		Boost     *Boost            `json:"boost,omitempty"`
		Nested    bool              `json:"nested,omitempty"`
	}{}
	err := util.UnmarshalJSON(data, &tmp)
	if err != nil {
		return err
	}
	q.Conjuncts = make([]Query, len(tmp.Conjuncts))
	for i, term := range tmp.Conjuncts {
		query, err := ParseQuery(term)
		if err != nil {
			return err
		}
		q.Conjuncts[i] = query
	}
	q.BoostVal = tmp.Boost
	return nil
}
