//  Copyright (c) 2018 Couchbase, Inc.
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

package pl

import (
	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
	"github.com/blevesearch/stempel"
	"log"
)

const SnowballStemmerName = "stemmer_pl_snowball"
const TrieStempelFileName = "stemmer_20000.tbl"

type PolishStemmerFilter struct {
	trie stempel.Trie
}

func NewPolishStemmerFilter() *PolishStemmerFilter {
	trie, err := stempel.Open(TrieStempelFileName)
	if err != nil {
		log.Fatal(err)
	}
	return &PolishStemmerFilter{
		trie: trie,
	}
}

func (s *PolishStemmerFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	for _, token := range input {
		buff := []rune(string(token.Term))
		diff := s.trie.GetLastOnPath(buff)
		buff = stempel.Diff(buff, diff)
		token.Term = []byte(string(buff))
	}
	return input
}

func PolishStemmerFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return NewPolishStemmerFilter(), nil
}

func init() {
	registry.RegisterTokenFilter(SnowballStemmerName, PolishStemmerFilterConstructor)
}
