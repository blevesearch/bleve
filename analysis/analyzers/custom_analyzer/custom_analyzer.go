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
	"reflect"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

const Name = "custom"

func AnalyzerConstructor(config map[string]interface{}, cache *registry.Cache) (*analysis.Analyzer, error) {

	var charFilters []analysis.CharFilter
	if reflect.TypeOf(config["char_filters"]).String() == "[]string" {
		charFilters, err := getCharFilters(config["char_filters"])
	} else if reflect.TypeOf(config["char_filters"]).String == "[]interface{}" {
		charFiltersNames, err := convertInterfaceSliceToStringSlice(config["char_filters"], "char filter")
		if err != nil {
			return nil, err
		}
		charFilters, err = getCharFilters(charFiltersNames)
		if err != nil {
			return nil, err
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
	if reflect.TypeOf(config["token_filters"]).String() == "[]string" {
		tokenFilters, err := getTokenFilters(config["token_filters"])
	} else if reflect.TypeOf(config["token_filters"]).String == "[]interface{}" {
		tokenFiltersNames, err := convertInterfaceSliceToStringSlice(config["token_filters"], "token filter")
		if err != nil {
			return nil, err
		}
		tokenFilters, err = getTokenFilters(charFiltersNames)
		if err != nil {
			return nil, err
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

func getCharFilters(charFilterNames []string) ([]analysis.CharFilter, error) {
	charFilters = make([]analysis.CharFilter, len(charFilterNames))
	for i, charFilterName := range charFilterNames {
		charFilter, err := cache.CharFilterNamed(charFilterName)
		if err != nil {
			return nil, err
		}
		charFilters[i] = charFilter
	}

	return charFilters, nil
}

func getTokenFilters(tokenFilterNames []string) ([]analysis.TokenFilter, error) {
	tokenFilters = make([]analysis.TokenFilter, len(tokenFilterNames))
	for i, tokenFilterName := range tokenFilterNames {
		tokenFilter, err := cache.TokenFilterNamed(tokenFilterName)
		if err != nil {
			return nil, err
		}
		tokenFilters[i] = tokenFilter
	}

	return tokenFilters, nil
}

func convertInterfaceSliceToStringSlice(interfaceSlice []interface{}, objType string) []string {
	stringSlice := make([]string, len(interfaceSlice))
	for i, interfaceObj := range interfaceSlice {
		stringObj, ok := interfaceObj.(string)
		if ok {
			stringSlice[i] = stringObj
		} else {
			return nil, fmt.Errorf(objType + " name must be a string")
		}
	}

	return stringSlice, nil
}
