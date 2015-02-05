//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package ar

import (
	"bytes"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

const StemmerName = "stemmer_ar"

// These were obtained from org.apache.lucene.analysis.ar.ArabicStemmer
var prefixes = [][]byte{
	[]byte("ال"),
	[]byte("وال"),
	[]byte("بال"),
	[]byte("كال"),
	[]byte("فال"),
	[]byte("لل"),
	[]byte("و"),
}
var suffixes = [][]byte{
	[]byte("ها"),
	[]byte("ان"),
	[]byte("ات"),
	[]byte("ون"),
	[]byte("ين"),
	[]byte("يه"),
	[]byte("ية"),
	[]byte("ه"),
	[]byte("ة"),
	[]byte("ي"),
}

type ArabicStemmerFilter struct{}

func NewArabicStemmerFilter() *ArabicStemmerFilter {
	return &ArabicStemmerFilter{}
}

func (s *ArabicStemmerFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	for _, token := range input {
		term := stem(token.Term)
		token.Term = term
	}
	return input
}

func stem(input []byte) []byte {
	// Strip a single prefix.
	for _, p := range prefixes {
		if bytes.HasPrefix(input, p) {
			input = input[len(p):]
			break
		}
	}
	// Strip off multiple suffixes, in their order in the suffixes array.
	for _, s := range suffixes {
		if bytes.HasSuffix(input, s) {
			input = input[:len(input)-len(s)]
		}
	}
	return input
}

func StemmerFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return NewArabicStemmerFilter(), nil
}

func init() {
	registry.RegisterTokenFilter(StemmerName, StemmerFilterConstructor)
}
