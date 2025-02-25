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

package fr

import (
	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"

	"github.com/blevesearch/snowballstem"
	"github.com/blevesearch/snowballstem/french"
)

const SnowballStemmerName = "stemmer_fr_snowball"

type FrenchStemmerFilter struct {
}

func NewFrenchStemmerFilter() *FrenchStemmerFilter {
	return &FrenchStemmerFilter{}
}

func (s *FrenchStemmerFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	for _, token := range input {
		env := snowballstem.NewEnv(string(token.Term))
		french.Stem(env)
		token.Term = []byte(env.Current())
	}
	return input
}

func FrenchStemmerFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return NewFrenchStemmerFilter(), nil
}

func init() {
	err := registry.RegisterTokenFilter(SnowballStemmerName, FrenchStemmerFilterConstructor)
	if err != nil {
		panic(err)
	}
}
