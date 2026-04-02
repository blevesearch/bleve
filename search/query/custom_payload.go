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

	"github.com/blevesearch/bleve/v2/util"
)

func cloneCustomQueryPayload(in map[string]interface{}) map[string]interface{} {
	if len(in) == 0 {
		return nil
	}

	out := make(map[string]interface{}, len(in))
	for k, v := range in {
		switch t := v.(type) {
		case []interface{}:
			out[k] = append([]interface{}(nil), t...)
		case []string:
			out[k] = append([]string(nil), t...)
		case []float64:
			out[k] = append([]float64(nil), t...)
		case []int:
			out[k] = append([]int(nil), t...)
		case []int64:
			out[k] = append([]int64(nil), t...)
		default:
			out[k] = t
		}
	}
	return out
}

func unmarshalCustomQueryPayload(data []byte, key string) (Query, map[string]interface{}, error) {
	tmp := map[string]json.RawMessage{}
	err := util.UnmarshalJSON(data, &tmp)
	if err != nil {
		return nil, nil, err
	}

	innerRaw, ok := tmp[key]
	if !ok || innerRaw == nil {
		return nil, nil, nil
	}

	var inner map[string]json.RawMessage
	err = util.UnmarshalJSON(innerRaw, &inner)
	if err != nil || inner == nil {
		return nil, nil, fmt.Errorf("%s query must be a JSON object", key)
	}

	var child Query
	if childQuery, ok := inner["query"]; ok && childQuery != nil {
		child, err = ParseQuery(childQuery)
		if err != nil {
			return nil, nil, err
		}
	}

	payload := make(map[string]interface{}, len(inner))
	for k, raw := range inner {
		if k == "query" {
			continue
		}
		var v interface{}
		if raw != nil {
			err = util.UnmarshalJSON(raw, &v)
			if err != nil {
				return nil, nil, fmt.Errorf("%s query has invalid %q payload: %w",
					key, k, err)
			}
		}
		payload[k] = v
	}

	return child, payload, nil
}
