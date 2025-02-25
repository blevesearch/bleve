//  Copyright (c) 2017 Couchbase, Inc.
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

package es

import (
	"bytes"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const NormalizeName = "normalize_es"

type SpanishNormalizeFilter struct {
}

func NewSpanishNormalizeFilter() *SpanishNormalizeFilter {
	return &SpanishNormalizeFilter{}
}

func (s *SpanishNormalizeFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	for _, token := range input {
		term := normalize(token.Term)
		token.Term = term
	}
	return input
}

func normalize(input []byte) []byte {
	runes := bytes.Runes(input)
	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case 'à', 'á', 'â', 'ä':
			runes[i] = 'a'
		case 'ò', 'ó', 'ô', 'ö':
			runes[i] = 'o'
		case 'è', 'é', 'ê', 'ë':
			runes[i] = 'e'
		case 'ù', 'ú', 'û', 'ü':
			runes[i] = 'u'
		case 'ì', 'í', 'î', 'ï':
			runes[i] = 'i'
		}
	}

	return analysis.BuildTermFromRunes(runes)
}

func NormalizerFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return NewSpanishNormalizeFilter(), nil
}

func init() {
	err := registry.RegisterTokenFilter(NormalizeName, NormalizerFilterConstructor)
	if err != nil {
		panic(err)
	}
}
