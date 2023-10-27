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
	"fmt"

	"github.com/blevesearch/bleve/v2/analysis/token/synonym"
	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	index "github.com/blevesearch/bleve_index_api"
)

type TermQuery struct {
	Term          string `json:"term"`
	FieldVal      string `json:"field,omitempty"`
	BoostVal      *Boost `json:"boost,omitempty"`
	SynonymSource string `json:"synonym_source,omitempty"`
}

// NewTermQuery creates a new Query for finding an
// exact term match in the index.
func NewTermQuery(term string) *TermQuery {
	return &TermQuery{
		Term: term,
	}
}

func (q *TermQuery) SetBoost(b float64) {
	boost := Boost(b)
	q.BoostVal = &boost
}

func (q *TermQuery) Boost() float64 {
	return q.BoostVal.Value()
}

func (q *TermQuery) SetField(f string) {
	q.FieldVal = f
}

func (q *TermQuery) Field() string {
	return q.FieldVal
}

func (q *TermQuery) SynonymSourceName(m mapping.IndexMapping) []string {
	synonymSourceName := ""
	if q.SynonymSource != "" {
		synonymSourceName = q.SynonymSource
	} else {
		field := q.FieldVal
		if q.FieldVal == "" {
			field = m.DefaultSearchField()
		}
		synonymSourceName = m.SynonymSourceNameForPath(field)
	}
	if synonymSourceName != "" {
		return []string{synonymSourceName}
	}
	return []string{}
}

func (q *TermQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	field := q.FieldVal
	if q.FieldVal == "" {
		field = m.DefaultSearchField()
	}

	synonymSourceName := q.SynonymSourceName(m)
	if len(synonymSourceName) > 0 {
		// Switch to Synonym Search
		synonymSource := m.SynonymSourceNamed(synonymSourceName[0])
		if synonymSource == nil {
			return nil, fmt.Errorf("no synonym source named '%s' registered", synonymSourceName)
		}
		// there is no base analyzer in term queries, so the created analyzer will only be a
		// synonym filter
		analyzer, err := synonym.AddSynonymFilter(ctx, nil, i, synonymSource.MetadataKey(), synonymSourceName[0], 0, 0)
		if err != nil {
			return nil, err
		}

		tokens := analyzer.Analyze([]byte(q.Term))
		if len(tokens) > 0 {
			return searcher.NewSynonymSearcher(ctx, i, tokens, field, q.BoostVal.Value(), 0, 0, 0, options)
		}
		noneQuery := NewMatchNoneQuery()
		return noneQuery.Searcher(ctx, i, m, options)

	}
	return searcher.NewTermSearcher(ctx, i, q.Term, field, q.BoostVal.Value(), options)
}
