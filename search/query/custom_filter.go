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
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/search"
	"github.com/blevesearch/bleve/v2/search/searcher"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
)

// CustomFilterQuery wraps a child query and filters its candidate matches via
// an embedder-provided per-hit callback. Fields lists stored fields to expose
// to the callback, Params carries caller-provided values passed as the second
// UDF argument, and Source carries the embedder-defined callback source.
type CustomFilterQuery struct {
	Query  Query                  `json:"query"`
	Fields []string               `json:"fields,omitempty"`
	Params map[string]interface{} `json:"params,omitempty"`
	Source string                 `json:"source"`
}

func NewCustomFilterQuery(query Query, source string) *CustomFilterQuery {
	return &CustomFilterQuery{
		Query:  query,
		Source: source,
	}
}

func (q *CustomFilterQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	if q == nil {
		return nil, fmt.Errorf("custom filter query is nil")
	}

	// Build the inner searcher first; custom filtering wraps its output.
	childSearcher, err := q.Query.Searcher(ctx, i, m, options)
	if err != nil {
		return nil, err
	}

	// Resolve the request-scoped callback builder injected by the embedder.
	if ctx == nil {
		return nil, fmt.Errorf("no custom filter factory registered in context")
	}
	factory, _ := ctx.Value(CustomFilterContextKey).(CustomFilterFactory)
	if factory == nil {
		return nil, fmt.Errorf("no custom filter factory registered in context")
	}

	// Build the per-hit filter callback from query-provided source/params/fields.
	filterFunc, err := factory(q.Source, q.Params, q.Fields)
	if err != nil {
		return nil, err
	}

	// Wrap the child so Next/Advance applies the callback on each candidate hit.
	return searcher.NewFilteringSearcher(ctx, childSearcher, filterFunc), nil
}

func (q *CustomFilterQuery) Validate() error {
	if q == nil {
		return fmt.Errorf("custom filter query is nil")
	}
	if q.Query == nil {
		return fmt.Errorf("custom filter query must have a query")
	}
	if q.Source == "" {
		return fmt.Errorf("custom filter query must have source")
	}
	if vq, ok := q.Query.(ValidatableQuery); ok {
		return vq.Validate()
	}
	return nil
}

func (q *CustomFilterQuery) MarshalJSON() ([]byte, error) {
	type customFilterInner struct {
		Query  Query                  `json:"query"`
		Fields []string               `json:"fields,omitempty"`
		Params map[string]interface{} `json:"params,omitempty"`
		Source string                 `json:"source"`
	}

	return json.Marshal(map[string]interface{}{
		"custom_filter": customFilterInner{
			Query:  q.Query,
			Fields: q.Fields,
			Params: q.Params,
			Source: q.Source,
		},
	})
}

func (q *CustomFilterQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		CustomFilter struct {
			Query  json.RawMessage        `json:"query"`
			Fields []string               `json:"fields,omitempty"`
			Params map[string]interface{} `json:"params,omitempty"`
			Source string                 `json:"source,omitempty"`
		} `json:"custom_filter"`
	}{}
	err := util.UnmarshalJSON(data, &tmp)
	if err != nil {
		return err
	}

	if tmp.CustomFilter.Query != nil {
		q.Query, err = ParseQuery(tmp.CustomFilter.Query)
		if err != nil {
			return err
		}
	}
	q.Fields = tmp.CustomFilter.Fields
	q.Params = tmp.CustomFilter.Params
	q.Source = tmp.CustomFilter.Source

	return nil
}
