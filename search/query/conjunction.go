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
	ss := make([]search.Searcher, 0, len(q.Conjuncts))
	cleanup := func() {
		for _, searcher := range ss {
			if searcher != nil {
				_ = searcher.Close()
			}
		}
	}
	nestedMode, _ := ctx.Value(search.NestedSearchKey).(bool)
	var nm mapping.NestedMapping
	if nestedMode {
		var ok bool
		// get the nested mapping
		if nm, ok = m.(mapping.NestedMapping); !ok {
			// shouldn't be in nested mode if no nested mapping
			nestedMode = false
		}
	}
	// set of fields used in this query
	var qfs search.FieldSet
	var err error

	for _, conjunct := range q.Conjuncts {
		// Gather fields when nested mode is enabled
		if nestedMode {
			qfs, err = ExtractFields(conjunct, m, qfs)
			if err != nil {
				cleanup()
				return nil, err
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

	if nestedMode {
		// first determine the nested depth info for the query fields
		commonDepth, maxDepth := nm.NestedDepth(qfs)
		// if we have common depth == max depth then we can just use
		// the normal conjunction searcher, as all fields share the same
		// nested context, otherwise we need to use the nested conjunction searcher
		// also, if we are querying the _all or _id fields, we need to use
		// the nested conjunction searcher as well, with common depth 0
		// indicating matches happen only at the root level
		if qfs.HasAll() || qfs.HasID() {
			commonDepth = 0
		}
		if commonDepth < maxDepth {
			return searcher.NewNestedConjunctionSearcher(ctx, i, ss, commonDepth, options)
		}
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
