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
	"testing"
)

func TestCustomFilterQueryJSON(t *testing.T) {
	jsonBytes := []byte(`{
		"custom_filter": {
			"query": {
				"match": "beer"
			},
			"source": "function my_filter(doc, params){ return true; }",
			"fields": ["abv", "style"],
			"params": {
				"abv": 5.0
			}
		}
	}`)

	q, err := ParseQuery(jsonBytes)
	if err != nil {
		t.Fatal(err)
	}

	cfq, ok := q.(*CustomFilterQuery)
	if !ok {
		t.Fatalf("expected CustomFilterQuery, got %T", q)
	}

	if cfq.Source == "" {
		t.Errorf("expected Source to be set")
	}

	if len(cfq.Fields) != 2 || cfq.Fields[0] != "abv" || cfq.Fields[1] != "style" {
		t.Errorf("expected fields ['abv', 'style'], got %v", cfq.Fields)
	}

	if cfq.Params["abv"] != 5.0 {
		t.Errorf("expected abv 5.0, got %v", cfq.Params["abv"])
	}

	mq, ok := cfq.QueryVal.(*MatchQuery)
	if !ok {
		t.Fatalf("expected inner query to be MatchQuery, got %T", cfq.QueryVal)
	}

	if mq.Match != "beer" {
		t.Errorf("expected match 'beer', got '%s'", mq.Match)
	}
}

func TestCustomScoreQueryJSON(t *testing.T) {
	jsonBytes := []byte(`{
		"custom_score": {
			"query": {
				"match": "beer"
			},
			"source": "function my_score(doc, params){ return doc.score; }"
		}
	}`)

	q, err := ParseQuery(jsonBytes)
	if err != nil {
		t.Fatal(err)
	}

	csq, ok := q.(*CustomScoreQuery)
	if !ok {
		t.Fatalf("expected CustomScoreQuery, got %T", q)
	}

	if csq.Source == "" {
		t.Errorf("expected Source to be set")
	}
}
