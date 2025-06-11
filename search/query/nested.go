//  Copyright (c) 2025 Couchbase, Inc.
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

type NestedQuery struct {
	Path       string `json:"path"`
	InnerQuery Query  `json:"query"`
}

func NewNestedQuery(path string, innerQuery Query) *NestedQuery {
	return &NestedQuery{
		Path:       path,
		InnerQuery: innerQuery,
	}
}

func (q *NestedQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	nr, ok := i.(index.NestedReader)
	if !ok {
		return nil, fmt.Errorf("nested searcher requires an index reader that supports nested documents")
	}
	childCount := nr.ChildCount(q.Path)
	if childCount == 0 {
		return nil, fmt.Errorf("nested searcher: path %q has no child documents", q.Path)
	}
	innerSearchers := make([]search.Searcher, 0, childCount)
	for arrayPos := range childCount {
		nctx := context.WithValue(ctx, search.NestedInfoCallbackKey, &search.NestedInfo{
			Path:          q.Path,
			ArrayPosition: arrayPos,
		})
		innerSearcher, err := q.InnerQuery.Searcher(nctx, i, m, options)
		if err != nil {
			return nil, fmt.Errorf("nested searcher: failed to create inner searcher at pos %d: %w", arrayPos, err)
		}
		innerSearchers = append(innerSearchers, innerSearcher)
	}
	return searcher.NewDisjunctionSearcher(ctx, i, innerSearchers, 0, options)
}

func (q *NestedQuery) Validate() error {
	if q.Path == "" {
		return fmt.Errorf("nested query must have a path")
	}
	if q.InnerQuery == nil {
		return fmt.Errorf("nested query must have a query")
	}
	if vq, ok := q.InnerQuery.(ValidatableQuery); ok {
		if err := vq.Validate(); err != nil {
			return fmt.Errorf("nested query must have a valid query: %v", err)
		}
	}
	return nil
}

func (q *NestedQuery) UnmarshalJSON(data []byte) error {
	tmp := struct {
		Path  string          `json:"path"`
		Query json.RawMessage `json:"query"`
	}{}
	err := util.UnmarshalJSON(data, &tmp)
	if err != nil {
		return err
	}
	if tmp.Path == "" {
		return fmt.Errorf("nested query must have a path")
	}
	if tmp.Query == nil {
		return fmt.Errorf("nested query must have a query")
	}
	q.Path = tmp.Path
	q.InnerQuery, err = ParseQuery(tmp.Query)
	if err != nil || q.InnerQuery == nil {
		return fmt.Errorf("nested query must have a valid query: %v", err)
	}
	return nil
}
