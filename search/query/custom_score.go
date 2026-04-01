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
	index "github.com/blevesearch/bleve_index_api"
)

// CustomScoreQuery wraps a child query and re-scores its candidate matches via
// an embedder-provided per-hit callback.
type CustomScoreQuery struct {
	Query Query `json:"query"`

	scoreFunc searcher.ScoreFunc
	payload map[string]interface{}
}

func NewCustomScoreQuery(query Query) *CustomScoreQuery {
	return &CustomScoreQuery{
		Query: query,
	}
}

func NewCustomScoreQueryWithScorer(query Query, score searcher.ScoreFunc) *CustomScoreQuery {
	return &CustomScoreQuery{
		Query:     query,
		scoreFunc: score,
	}
}

func NewCustomScoreQueryWithScorerAndPayload(query Query, score searcher.ScoreFunc, payload map[string]interface{}) *CustomScoreQuery {
	return &CustomScoreQuery{
		Query:     query,
		scoreFunc: score,
		payload:   cloneCustomQueryPayload(payload),
	}
}

func (q *CustomScoreQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	if q == nil {
		return nil, fmt.Errorf("custom score query is nil")
	}
	if q.Query == nil {
		return nil, fmt.Errorf("custom score query must have a query")
	}
	if q.scoreFunc == nil {
		return nil, fmt.Errorf("custom score query must have a score callback")
	}

	// Build the inner searcher first; custom scoring wraps its output.
	childSearcher, err := q.Query.Searcher(ctx, i, m, options)
	if err != nil {
		return nil, err
	}

	// Wrap the child so Next/Advance mutates score on each candidate hit.
	return searcher.NewScoreMutatingSearcher(ctx, childSearcher, q.scoreFunc), nil
}

func (q *CustomScoreQuery) Validate() error {
	if q == nil {
		return fmt.Errorf("custom score query is nil")
	}
	if q.Query == nil {
		return fmt.Errorf("custom score query must have a query")
	}
	if vq, ok := q.Query.(ValidatableQuery); ok {
		return vq.Validate()
	}
	return nil
}

func (q *CustomScoreQuery) MarshalJSON() ([]byte, error) {
	type customScoreInner struct {
		Query Query `json:"query"`
	}

	if len(q.payload) > 0 {
		inner := cloneCustomQueryPayload(q.payload)
		inner["query"] = q.Query
		return json.Marshal(map[string]interface{}{
			"custom_score": inner,
		})
	}

	return json.Marshal(map[string]interface{}{
		"custom_score": customScoreInner{
			Query: q.Query,
		},
	})
}

func (q *CustomScoreQuery) UnmarshalJSON(data []byte) error {
	child, payload, err := unmarshalCustomQueryPayload(data, "custom_score")
	if err != nil {
		return err
	}
	q.Query = child
	q.payload = payload
	return nil
}
