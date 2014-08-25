//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build cld2

package detect_lang_analyzer

import (
	"github.com/couchbaselabs/bleve/analysis"
	"github.com/couchbaselabs/bleve/analysis/token_filters/cld2"
	"github.com/couchbaselabs/bleve/analysis/token_filters/lower_case_filter"
	"github.com/couchbaselabs/bleve/analysis/tokenizers/single_token"
	"github.com/couchbaselabs/bleve/registry"
)

const Name = "detect_lang"

func AnalyzerConstructor(config map[string]interface{}, cache *registry.Cache) (*analysis.Analyzer, error) {
	keywordTokenizer, err := cache.TokenizerNamed(single_token.Name)
	if err != nil {
		return nil, err
	}
	toLowerFilter, err := cache.TokenFilterNamed(lower_case_filter.Name)
	if err != nil {
		return nil, err
	}
	detectLangFilter, err := cache.TokenFilterNamed(cld2.Name)
	if err != nil {
		return nil, err
	}
	rv := analysis.Analyzer{
		Tokenizer: keywordTokenizer,
		TokenFilters: []analysis.TokenFilter{
			toLowerFilter,
			detectLangFilter,
		},
	}
	return &rv, nil
}

func init() {
	registry.RegisterAnalyzer(Name, AnalyzerConstructor)
}
