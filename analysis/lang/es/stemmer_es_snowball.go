//  Copyright (c) 2020 Couchbase, Inc.
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
	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"

	"github.com/blevesearch/snowballstem"
	"github.com/blevesearch/snowballstem/spanish"
)

const SnowballStemmerName = "stemmer_es_snowball"

type SpanishStemmerFilter struct {
}

func NewSpanishStemmerFilter() *SpanishStemmerFilter {
	return &SpanishStemmerFilter{}
}

func (s *SpanishStemmerFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	for _, token := range input {
		env := snowballstem.NewEnv(string(token.Term))
		spanish.Stem(env)
		token.Term = []byte(env.Current())
	}
	return input
}

func SpanishStemmerFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return NewSpanishStemmerFilter(), nil
}

func init() {
	err := registry.RegisterTokenFilter(SnowballStemmerName, SpanishStemmerFilterConstructor)
	if err != nil {
		panic(err)
	}
}
