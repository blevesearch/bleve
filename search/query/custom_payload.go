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
	"encoding/json"
	"fmt"

	"github.com/blevesearch/bleve/v2/mapping"
	"github.com/blevesearch/bleve/v2/util"
	index "github.com/blevesearch/bleve_index_api"
)

func unmarshalCustomQueryPayload(data []byte, key string) (Query, []string, map[string]interface{}, error) {
	tmp := map[string]json.RawMessage{}
	err := util.UnmarshalJSON(data, &tmp)
	if err != nil {
		return nil, nil, nil, err
	}

	innerRaw, ok := tmp[key]
	if !ok || innerRaw == nil {
		return nil, nil, nil, nil
	}

	var inner map[string]json.RawMessage
	err = util.UnmarshalJSON(innerRaw, &inner)
	if err != nil || inner == nil {
		return nil, nil, nil, fmt.Errorf("%s query must be a JSON object", key)
	}

	var child Query
	if childQuery, ok := inner["query"]; ok && childQuery != nil {
		child, err = ParseQuery(childQuery)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	var fields []string
	if rawFields, ok := inner["fields"]; ok && rawFields != nil {
		if err := util.UnmarshalJSON(rawFields, &fields); err != nil {
			return nil, nil, nil, fmt.Errorf("%s query has invalid %q: %w",
				key, "fields", err)
		}
	}

	payload := make(map[string]interface{}, len(inner))
	for k, raw := range inner {
		if k == "query" || k == "fields" {
			continue
		}
		var v interface{}
		if raw != nil {
			err = util.UnmarshalJSON(raw, &v)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("%s query has invalid %q payload: %w",
					key, k, err)
			}
		}
		payload[k] = v
	}

	return child, fields, payload, nil
}

// resolveFieldTypes looks up each field name in the index mapping and returns
// a map of field name → mapping type (e.g. "datetime", "number", "text").
// This is used by the searcher layer to correctly decode doc value bytes.
func resolveFieldTypes(fields []string, m mapping.IndexMapping) map[string]string {
	if m == nil || len(fields) == 0 {
		return nil
	}
	types := make(map[string]string, len(fields))
	for _, f := range fields {
		fm := m.FieldMappingForPath(f)
		if fm.Type != "" {
			types[f] = fm.Type
		}
	}
	if len(types) == 0 {
		return nil
	}
	return types
}

// expandFieldWildcard replaces a "*" entry in the requested fields with the
// concrete set of fields in the index, mirroring how the standard stored-field
// loader treats "*" (see LoadAndHighlightFields). Custom-query callbacks read
// field values via the doc-value reader, which matches field names literally
// and has no wildcard handling of its own; without this expansion a request for
// fields:["*"] resolves to a single field literally named "*", matches nothing,
// and leaves d.Fields empty in the callback.
//
// The internal composite/identifier fields ("_all", "_id") are excluded so the
// callback sees the same fields the stored-field "*" path would surface rather
// than the tokenized _all blob or the document id (already available as the
// hit's ID). When fields does not contain "*", it is returned unchanged.
func expandFieldWildcard(fields []string, i index.IndexReader) ([]string, error) {
	hasWildcard := false
	for _, f := range fields {
		if f == "*" {
			hasWildcard = true
			break
		}
	}
	if !hasWildcard {
		return fields, nil
	}

	allFields, err := i.Fields()
	if err != nil {
		return nil, err
	}

	expanded := make([]string, 0, len(allFields))
	for _, f := range allFields {
		if f == "_all" || f == "_id" {
			continue
		}
		expanded = append(expanded, f)
	}
	return expanded, nil
}
