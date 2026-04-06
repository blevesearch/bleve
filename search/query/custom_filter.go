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

// CustomFilterQuery wraps a child query and filters its candidate matches via
// an embedder-provided per-hit callback.
type CustomFilterQuery struct {
	Query      Query `json:"query"`
	filterFunc searcher.FilterFunc
	payload    map[string]interface{}
}

// CustomFilterQueryParser lets an embedder override parsing of
// {"custom_filter": ...} nodes. It is intended to be assigned once during
// process startup or init, before any queries are parsed; callers must not
// mutate it concurrently with ParseQuery(). For example:
//
//	func init() {
//		query.CustomFilterQueryParser = parseCustomFilterQuery
//	}
var CustomFilterQueryParser func([]byte) (Query, error)

func NewCustomFilterQuery(query Query) *CustomFilterQuery {
	return &CustomFilterQuery{
		Query: query,
	}
}

func NewCustomFilterQueryWithFilter(query Query, filter searcher.FilterFunc, payload map[string]interface{}) *CustomFilterQuery {
	return &CustomFilterQuery{
		Query:      query,
		filterFunc: filter,
		payload:    payload,
	}
}

func (q *CustomFilterQuery) Searcher(ctx context.Context, i index.IndexReader, m mapping.IndexMapping, options search.SearcherOptions) (search.Searcher, error) {
	if q == nil {
		return nil, fmt.Errorf("custom filter query is nil")
	}
	if q.Query == nil {
		return nil, fmt.Errorf("custom filter query must have a query")
	}
	if q.filterFunc == nil {
		return nil, fmt.Errorf("custom filter query must have a filter callback")
	}

	// Build the inner searcher first; custom filtering wraps its output.
	childSearcher, err := q.Query.Searcher(ctx, i, m, options)
	if err != nil {
		return nil, err
	}

	// Create a doc value reader for the requested fields (if any) so the
	// searcher can populate d.Fields before invoking the callback.
	fields := payloadFields(q.payload, "fields")
	var dvReader index.DocValueReader
	var fieldTypes map[string]string
	if len(fields) > 0 {
		var err2 error
		dvReader, err2 = i.DocValueReader(fields)
		if err2 != nil {
			_ = childSearcher.Close()
			return nil, err2
		}
		fieldTypes = resolveFieldTypes(fields, m)
	}

	return searcher.NewCustomFilterSearcher(ctx, childSearcher, q.filterFunc, dvReader, i, fieldTypes), nil
}

func (q *CustomFilterQuery) Validate() error {
	if q == nil {
		return fmt.Errorf("custom filter query is nil")
	}
	if q.Query == nil {
		return fmt.Errorf("custom filter query must have a query")
	}
	if vq, ok := q.Query.(ValidatableQuery); ok {
		return vq.Validate()
	}
	return nil
}

func (q *CustomFilterQuery) MarshalJSON() ([]byte, error) {
	inner := make(map[string]interface{}, len(q.payload)+1)
	for k, v := range q.payload {
		inner[k] = v
	}
	inner["query"] = q.Query
	return json.Marshal(map[string]interface{}{
		"custom_filter": inner,
	})
}

func (q *CustomFilterQuery) UnmarshalJSON(data []byte) error {
	child, payload, err := unmarshalCustomQueryPayload(data, "custom_filter")
	if err != nil {
		return err
	}
	q.Query = child
	q.payload = payload
	return nil
}
