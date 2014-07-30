//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package stop_words_filter

import (
	"github.com/couchbaselabs/bleve/analysis"
)

var DEFAULT_STOP_WORDS []string = []string{
	"a", "an", "and", "are", "as", "at", "be", "but", "by",
	"for", "if", "in", "into", "is", "it",
	"no", "not", "of", "on", "or", "such",
	"that", "the", "their", "then", "there", "these",
	"they", "this", "to", "was", "will", "with",
}

type StopWordsFilter struct {
	stopWords map[string]bool
}

func NewStopWordsFilter() *StopWordsFilter {
	return &StopWordsFilter{
		stopWords: buildStopWordMap(DEFAULT_STOP_WORDS),
	}
}

func (f *StopWordsFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	rv := make(analysis.TokenStream, 0)

	for _, token := range input {
		word := string(token.Term)
		_, isStopWord := f.stopWords[word]
		if !isStopWord {
			rv = append(rv, token)
		}
	}

	return rv
}

func buildStopWordMap(words []string) map[string]bool {
	rv := make(map[string]bool, len(words))
	for _, word := range words {
		rv[word] = true
	}
	return rv
}
