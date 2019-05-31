//  Copyright (c) 2019 Couchbase, Inc.
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

package reverse

import (
	"unicode/utf8"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

// Name is the name used to register ReverseFilter in the bleve registry
const Name = "reverse"

type ReverseFilter struct {
}

func NewReverseFilter() *ReverseFilter {
	return &ReverseFilter{}
}

func (f *ReverseFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	for _, token := range input {
		token.Term = reverse(token.Term)
	}
	return input
}

func ReverseFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return NewReverseFilter(), nil
}

func init() {
	registry.RegisterTokenFilter(Name, ReverseFilterConstructor)
}

// reverse(..) will generate a reversed version of the provided
// utf-8 encoded byte array and return it back to its caller.
func reverse(s []byte) []byte {
	j := len(s)
	rv := make([]byte, len(s))
	for i := 0; i < len(s); {
		wid := 1
		r := rune(s[i])
		if r >= utf8.RuneSelf {
			r, wid = utf8.DecodeRune(s[i:])
		}

		copy(rv[j-wid:j], s[i:i+wid])
		i += wid
		j -= wid
	}
	return rv
}
