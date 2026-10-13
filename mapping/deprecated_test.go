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

package mapping

import (
	"errors"
	"strings"
	"testing"

	_ "github.com/blevesearch/bleve/v2/analysis/analyzer/custom"
	_ "github.com/blevesearch/bleve/v2/analysis/token/stop"
	_ "github.com/blevesearch/bleve/v2/analysis/tokenizer/exception"
	_ "github.com/blevesearch/bleve/v2/analysis/tokenizer/unicode"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/blevesearch/bleve/v2/util"
)

func TestDeprecatedAnalysisComponents(t *testing.T) {
	tests := []struct {
		name    string
		mapping string
		// expected Validate outcome: "" for nil, "deprecated" for an error
		// wrapping registry.ErrDeprecatedComponent, anything else is a
		// substring of a non-deprecated error
		expect string
	}{
		{
			name:    "clean mapping",
			mapping: `{"default_mapping":{"properties":{"f":{"fields":[{"name":"f","type":"text","analyzer":"en"}]}}}}`,
			expect:  "",
		},
		{
			name:    "field analyzer",
			mapping: `{"default_mapping":{"properties":{"f":{"fields":[{"name":"f","type":"text","analyzer":"he"}]}}}}`,
			expect:  "deprecated",
		},
		{
			name:    "index default analyzer",
			mapping: `{"default_analyzer":"he"}`,
			expect:  "deprecated",
		},
		{
			name:    "document default analyzer",
			mapping: `{"types":{"t":{"enabled":true,"default_analyzer":"he"}}}`,
			expect:  "deprecated",
		},
		{
			name: "disabled type mapping still counts",
			mapping: `{"types":{"t":{"enabled":false,"properties":{"f":{"fields":[` +
				`{"name":"f","type":"text","analyzer":"he"}]}}}}}`,
			expect: "deprecated",
		},
		{
			name: "custom analyzer on deprecated tokenizer and filter",
			mapping: `{"analysis":{"analyzers":{"my_he":{"type":"custom","tokenizer":"hebrew",` +
				`"token_filters":["niqqud_he","stop_he"]}}},` +
				`"default_mapping":{"properties":{"f":{"fields":[{"name":"f","type":"text","analyzer":"my_he"}]}}}}`,
			expect: "deprecated",
		},
		{
			name: "custom analyzer declared but not used",
			mapping: `{"analysis":{"analyzers":{"my_he":{"type":"custom","tokenizer":"unicode",` +
				`"token_filters":["lemmatizer_he"]}}}}`,
			expect: "deprecated",
		},
		{
			name: "custom token filter on deprecated token map",
			mapping: `{"analysis":{"token_filters":{"my_stop":{"type":"stop_tokens","stop_token_map":"stop_he"}},` +
				`"analyzers":{"a":{"type":"custom","tokenizer":"unicode","token_filters":["my_stop"]}}},` +
				`"default_mapping":{"properties":{"f":{"fields":[{"name":"f","type":"text","analyzer":"a"}]}}}}`,
			expect: "deprecated",
		},
		{
			// "outer" may be attempted before "inner" is; it must still end up
			// reported as deprecated, not as a missing tokenizer
			name: "custom tokenizer on custom deprecated tokenizer",
			mapping: `{"analysis":{"tokenizers":{` +
				`"inner":{"type":"hebrew"},` +
				`"outer":{"type":"exception","exceptions":["x"],"tokenizer":"inner"}},` +
				`"analyzers":{"a":{"type":"custom","tokenizer":"outer"}}},` +
				`"default_mapping":{"properties":{"f":{"fields":[{"name":"f","type":"text","analyzer":"a"}]}}}}`,
			expect: "deprecated",
		},
		{
			name: "synonym source on deprecated analyzer",
			mapping: `{"analysis":{"synonym_sources":{"syn":{"collection":"c","analyzer":"he"}}},` +
				`"default_mapping":{"properties":{"f":{"fields":[{"name":"f","type":"text",` +
				`"analyzer":"en","synonym_source":"syn"}]}}}}`,
			expect: "deprecated",
		},
		{
			name: "other errors take precedence",
			mapping: `{"default_mapping":{"properties":{` +
				`"a":{"fields":[{"name":"a","type":"text","analyzer":"he"}]},` +
				`"b":{"fields":[{"name":"b","type":"text","analyzer":"nope"}]}}}}`,
			expect: "no analyzer with name or type 'nope' registered",
		},
		{
			name:    "unknown analyzer is not deprecated",
			mapping: `{"default_mapping":{"properties":{"f":{"fields":[{"name":"f","type":"text","analyzer":"nope"}]}}}}`,
			expect:  "no analyzer with name or type 'nope' registered",
		},
		{
			name:    "custom analyzer on unknown tokenizer still fails unmarshal",
			mapping: `{"analysis":{"analyzers":{"a":{"type":"custom","tokenizer":"nope"}}}}`,
			expect:  "unmarshal:no tokenizer with name or type 'nope' registered",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var im *IndexMappingImpl
			err := util.UnmarshalJSON([]byte(test.mapping), &im)
			if strings.HasPrefix(test.expect, "unmarshal:") {
				if err == nil || !strings.Contains(err.Error(), test.expect[len("unmarshal:"):]) {
					t.Fatalf("expected unmarshal error %q, got %v", test.expect, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unmarshal: %v", err)
			}

			err = im.Validate()
			switch test.expect {
			case "":
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			case "deprecated":
				if !errors.Is(err, registry.ErrDeprecatedComponent) {
					t.Fatalf("expected a deprecated component error, got %v", err)
				}
			default:
				if err == nil || errors.Is(err, registry.ErrDeprecatedComponent) ||
					!strings.Contains(err.Error(), test.expect) {
					t.Fatalf("expected error containing %q, got %v", test.expect, err)
				}
			}

			// lookups of the deprecated name keep failing, and cheaply
			if test.expect == "deprecated" && im.AnalyzerNamed("he") != nil {
				t.Fatalf("expected no analyzer for 'he'")
			}
		})
	}
}

func TestDeprecatedComponentMessage(t *testing.T) {
	cache := registry.NewCache()
	_, err := cache.TokenFilterNamed("lemmatizer_he")
	if !errors.Is(err, registry.ErrDeprecatedComponent) {
		t.Fatalf("expected deprecated error, got %v", err)
	}
	if !strings.Contains(err.Error(), "token filter 'lemmatizer_he' is no longer supported") {
		t.Fatalf("unexpected message: %v", err)
	}
}
