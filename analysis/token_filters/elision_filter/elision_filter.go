//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package elision_filter

import (
	"bytes"
	"fmt"

	"github.com/couchbaselabs/bleve/analysis"
	"github.com/couchbaselabs/bleve/registry"
)

const Name = "elision"

const RIGHT_SINGLE_QUOTATION_MARK = "’"
const APOSTROPHE = "'"

const APOSTROPHES = APOSTROPHE + RIGHT_SINGLE_QUOTATION_MARK

type ElisionFilter struct {
	articles analysis.TokenMap
}

func NewElisionFilter(articles analysis.TokenMap) *ElisionFilter {
	return &ElisionFilter{
		articles: articles,
	}
}

func (s *ElisionFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	rv := make(analysis.TokenStream, 0)

	for _, token := range input {
		firstApostrophe := bytes.IndexAny(token.Term, APOSTROPHES)
		if firstApostrophe >= 0 {
			// found an apostrophe
			prefix := token.Term[0:firstApostrophe]
			// see if the prefix matches one of the articles
			_, articleMatch := s.articles[string(prefix)]
			if articleMatch {
				token.Term = token.Term[firstApostrophe+1:]
			}
		}
		rv = append(rv, token)
	}

	return rv
}

func ElisionFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	articlesTokenMapName, ok := config["articles_token_map"].(string)
	if !ok {
		return nil, fmt.Errorf("must specify articles_token_map")
	}
	articlesTokenMap, err := cache.TokenMapNamed(articlesTokenMapName)
	if err != nil {
		return nil, fmt.Errorf("error building elision filter: %v", err)
	}
	return NewElisionFilter(articlesTokenMap), nil
}

func init() {
	registry.RegisterTokenFilter(Name, ElisionFilterConstructor)
}
