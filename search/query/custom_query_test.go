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
	"testing"

	"github.com/blevesearch/bleve/v2/search"
)

func TestCustomFilterQueryUnmarshalJSON(t *testing.T) {
	jsonBytes := []byte(`{
		"custom_filter": {
			"query": {
				"match": "beer"
			}
		}
	}`)

	var cfq CustomFilterQuery
	err := cfq.UnmarshalJSON(jsonBytes)
	if err != nil {
		t.Fatal(err)
	}

	mq, ok := cfq.Query.(*MatchQuery)
	if !ok {
		t.Fatalf("expected inner query to be MatchQuery, got %T", cfq.Query)
	}

	if mq.Match != "beer" {
		t.Errorf("expected match 'beer', got '%s'", mq.Match)
	}
}

func TestCustomScoreQueryUnmarshalJSON(t *testing.T) {
	jsonBytes := []byte(`{
		"custom_score": {
			"query": {
				"match": "beer"
			}
		}
	}`)

	var csq CustomScoreQuery
	err := csq.UnmarshalJSON(jsonBytes)
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := csq.Query.(*MatchQuery); !ok {
		t.Fatalf("expected inner query to be MatchQuery, got %T", csq.Query)
	}
}

func TestCustomFilterQueryMarshalJSONPreservesPayloadAndRewritesChild(t *testing.T) {
	payload := map[string]interface{}{
		"fields": []string{"abv"},
		"params": map[string]interface{}{"min": float64(5)},
		"source": "function keep(doc, params){ return true; }",
	}

	q := NewCustomFilterQueryWithFilterAndPayload(NewMatchQuery("ipa"),
		func(sctx *search.SearchContext, d *search.DocumentMatch) bool { return true }, payload)

	out, err := q.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	var decoded struct {
		CustomFilter struct {
			Query  json.RawMessage        `json:"query"`
			Fields []string               `json:"fields"`
			Params map[string]interface{} `json:"params"`
			Source string                 `json:"source"`
		} `json:"custom_filter"`
	}
	err = json.Unmarshal(out, &decoded)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.CustomFilter.Source != "function keep(doc, params){ return true; }" {
		t.Fatalf("unexpected source: %q", decoded.CustomFilter.Source)
	}
	if len(decoded.CustomFilter.Fields) != 1 || decoded.CustomFilter.Fields[0] != "abv" {
		t.Fatalf("unexpected fields: %v", decoded.CustomFilter.Fields)
	}
	if got := decoded.CustomFilter.Params["min"]; got != float64(5) {
		t.Fatalf("unexpected params: %v", decoded.CustomFilter.Params)
	}

	child, err := ParseQuery(decoded.CustomFilter.Query)
	if err != nil {
		t.Fatal(err)
	}
	mq, ok := child.(*MatchQuery)
	if !ok {
		t.Fatalf("expected match query child, got %T", child)
	}
	if mq.Match != "ipa" {
		t.Fatalf("expected marshaled child query to be rewritten to ipa, got %q", mq.Match)
	}
}

func TestCustomScoreQueryMarshalJSONPreservesPayloadAndRewritesChild(t *testing.T) {
	payload := map[string]interface{}{
		"fields": []string{"ibu"},
		"params": map[string]interface{}{"weight": 0.05},
		"source": "function score(doc, params){ return doc.score; }",
	}

	q := NewCustomScoreQueryWithScorerAndPayload(NewMatchQuery("ipa"),
		func(sctx *search.SearchContext, d *search.DocumentMatch) float64 { return d.Score }, payload)

	out, err := q.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

	var decoded struct {
		CustomScore struct {
			Query  json.RawMessage        `json:"query"`
			Fields []string               `json:"fields"`
			Params map[string]interface{} `json:"params"`
			Source string                 `json:"source"`
		} `json:"custom_score"`
	}
	err = json.Unmarshal(out, &decoded)
	if err != nil {
		t.Fatal(err)
	}

	if decoded.CustomScore.Source != "function score(doc, params){ return doc.score; }" {
		t.Fatalf("unexpected source: %q", decoded.CustomScore.Source)
	}
	if len(decoded.CustomScore.Fields) != 1 || decoded.CustomScore.Fields[0] != "ibu" {
		t.Fatalf("unexpected fields: %v", decoded.CustomScore.Fields)
	}
	if got := decoded.CustomScore.Params["weight"]; got != 0.05 {
		t.Fatalf("unexpected params: %v", decoded.CustomScore.Params)
	}

	child, err := ParseQuery(decoded.CustomScore.Query)
	if err != nil {
		t.Fatal(err)
	}
	mq, ok := child.(*MatchQuery)
	if !ok {
		t.Fatalf("expected match query child, got %T", child)
	}
	if mq.Match != "ipa" {
		t.Fatalf("expected marshaled child query to be rewritten to ipa, got %q", mq.Match)
	}
}
