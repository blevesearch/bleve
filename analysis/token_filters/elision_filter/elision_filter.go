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

	"github.com/couchbaselabs/bleve/analysis"
)

const RIGHT_SINGLE_QUOTATION_MARK = "’"
const APOSTROPHE = "'"

const APOSTROPHES = APOSTROPHE + RIGHT_SINGLE_QUOTATION_MARK

type ElisionFilter struct {
	articles analysis.WordMap
}

func NewElisionFilter(articles analysis.WordMap) *ElisionFilter {
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
