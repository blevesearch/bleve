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

type CustomScoreQuery struct {
	// QueryVal is the child query whose candidate matches are re-scored.
	QueryVal Query `json:"query"`
	// Fields lists stored fields to load into doc.fields for UDF execution.
	Fields []string `json:"fields,omitempty"`
	// Params carries caller-provided values passed as the second UDF argument.
	Params map[string]interface{} `json:"params,omitempty"`
	// Source carries embedding-defined callback source that travels with the query.
	Source string `json:"source,omitempty"`
}

func NewCustomScoreQuery(query Query, source string) *CustomScoreQuery {
	return &CustomScoreQuery{
		QueryVal: query,
		Source:   source,
	}
}

func (q *CustomScoreQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	// Build the inner searcher first; custom scoring wraps its output.
	childSearcher, err := q.QueryVal.Searcher(ctx, i, m, options)
	if err != nil {
		return nil, err
	}

	// Resolve the request-scoped callback builder injected by the embedder.
	factory, _ := ctx.Value(CustomScoreContextKey).(CustomScoreFactory)
	if factory == nil {
		return nil, fmt.Errorf("no custom score factory registered in context")
	}

	// Build the per-hit score callback from query-provided source/params/fields.
	scoreFunc, err := factory(q.Source, q.Params, q.Fields)
	if err != nil {
		return nil, err
	}

	// Wrap the child so Next/Advance mutates score on each candidate hit.
	return searcher.NewScoreMutatingSearcher(ctx, childSearcher, scoreFunc), nil
}

func (q *CustomScoreQuery) Validate() error {
	if q.QueryVal == nil {
		return fmt.Errorf("custom score query must have a query")
	}
	if q.Source == "" {
		return fmt.Errorf("custom score query must have source")
	}
	if vq, ok := q.QueryVal.(ValidatableQuery); ok {
		return vq.Validate()
	}
	return nil
}

func (q *CustomScoreQuery) MarshalJSON() ([]byte, error) {
	inner := map[string]interface{}{
		"query": q.QueryVal,
	}
	if len(q.Fields) > 0 {
		inner["fields"] = q.Fields
	}
	if len(q.Params) > 0 {
		inner["params"] = q.Params
	}
	if q.Source != "" {
		inner["source"] = q.Source
	}
	return json.Marshal(map[string]interface{}{
		"custom_score": inner,
	})
}

func (q *CustomScoreQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		CustomScore struct {
			Query  json.RawMessage        `json:"query"`
			Fields []string               `json:"fields,omitempty"`
			Params map[string]interface{} `json:"params,omitempty"`
			Source string                 `json:"source,omitempty"`
		} `json:"custom_score"`
	}{}
	err := util.UnmarshalJSON(data, &tmp)
	if err != nil {
		return err
	}

	if tmp.CustomScore.Query != nil {
		q.QueryVal, err = ParseQuery(tmp.CustomScore.Query)
		if err != nil {
			return err
		}
	}
	q.Fields = tmp.CustomScore.Fields
	q.Params = tmp.CustomScore.Params
	q.Source = tmp.CustomScore.Source

	return nil
}
