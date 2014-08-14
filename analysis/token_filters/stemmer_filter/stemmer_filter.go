//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package stemmer_filter

import (
	"bitbucket.org/tebeka/snowball"
	"github.com/couchbaselabs/bleve/analysis"
	"github.com/couchbaselabs/bleve/registry"
)

const Name = "stem"

type StemmerFilter struct {
	lang    string
	stemmer *snowball.Stemmer
}

func NewStemmerFilter(lang string) (*StemmerFilter, error) {
	stemmer, err := snowball.New(lang)
	if err != nil {
		return nil, err
	}
	return &StemmerFilter{
		lang:    lang,
		stemmer: stemmer,
	}, nil
}

func MustNewStemmerFilter(lang string) *StemmerFilter {
	sf, err := NewStemmerFilter(lang)
	if err != nil {
		panic(err)
	}
	return sf
}

func (s *StemmerFilter) List() []string {
	return snowball.LangList()
}

func (s *StemmerFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	rv := make(analysis.TokenStream, 0)

	for _, token := range input {
		// if not protected keyword, stem it
		if !token.KeyWord {
			stemmed := s.stemmer.Stem(string(token.Term))
			token.Term = []byte(stemmed)
		}
		rv = append(rv, token)
	}

	return rv
}

func StemmerFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	lang := "en"
	langVal, ok := config["lang"].(string)
	if ok {
		lang = langVal
	}
	return NewStemmerFilter(lang)
}

func init() {
	registry.RegisterTokenFilter(Name, StemmerFilterConstructor)
}
