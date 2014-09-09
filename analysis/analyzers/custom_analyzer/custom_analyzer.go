//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package standard_analyzer

import (
	"fmt"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

const Name = "custom"

func AnalyzerConstructor(config map[string]interface{}, cache *registry.Cache) (*analysis.Analyzer, error) {

	var charFilters []analysis.CharFilter
	charFilterNames, ok := config["char_filters"].([]interface{})
	if ok {
		charFilters = make([]analysis.CharFilter, len(charFilterNames))
		for i, charFilterName := range charFilterNames {
			charFilterNameString, ok := charFilterName.(string)
			if ok {
				charFilter, err := cache.CharFilterNamed(charFilterNameString)
				if err != nil {
					return nil, err
				}
				charFilters[i] = charFilter
			} else {
				return nil, fmt.Errorf("char filter name must be a string")
			}
		}
	}

	tokenizerName, ok := config["tokenizer"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify tokenizer")
	}

	tokenizer, err := cache.TokenizerNamed(tokenizerName)
	if err != nil {
		return nil, err
	}

	var tokenFilters []analysis.TokenFilter
	tokenFilterNames, ok := config["token_filters"].([]interface{})
	if ok {
		tokenFilters = make([]analysis.TokenFilter, len(tokenFilterNames))
		for i, tokenFilterName := range tokenFilterNames {
			tokenFilterNameString, ok := tokenFilterName.(string)
			if ok {
				tokenFilter, err := cache.TokenFilterNamed(tokenFilterNameString)
				if err != nil {
					return nil, err
				}
				tokenFilters[i] = tokenFilter
			} else {
				return nil, fmt.Errorf("token filter name must be a string")
			}
		}
	}

	rv := analysis.Analyzer{
		Tokenizer: tokenizer,
	}
	if charFilters != nil {
		rv.CharFilters = charFilters
	}
	if tokenFilters != nil {
		rv.TokenFilters = tokenFilters
	}
	return &rv, nil
}

func init() {
	registry.RegisterAnalyzer(Name, AnalyzerConstructor)
}
