//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package ja

import (
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"

	"github.com/ikawaha/kagome.ipadic/tokenizer"
)

const TokenizerName = "kagome"

type KagomeMorphTokenizer struct {
	tok tokenizer.Tokenizer
}

func init() {
	_ = tokenizer.SysDic() // prepare system dictionary
}

func NewKagomeMorphTokenizer() *KagomeMorphTokenizer {
	return &KagomeMorphTokenizer{
		tok: tokenizer.New(),
	}
}

func NewKagomeMorphTokenizerWithUserDic(userdic tokenizer.UserDic) *KagomeMorphTokenizer {
	k := tokenizer.New()
	k.SetUserDic(userdic)
	return &KagomeMorphTokenizer{
		tok: k,
	}
}

func (t *KagomeMorphTokenizer) Tokenize(input []byte) analysis.TokenStream {
	var (
		morphs    []tokenizer.Token
		prevstart int
	)

	rv := make(analysis.TokenStream, 0, len(input))
	if len(input) < 1 {
		return rv
	}

	morphs = t.tok.Analyze(string(input), tokenizer.Search)

	for i, m := range morphs {
		if m.Surface == "EOS" || m.Surface == "BOS" {
			continue
		}

		surfacelen := len(m.Surface)
		token := &analysis.Token{
			Term:     []byte(m.Surface),
			Position: i,
			Start:    prevstart,
			End:      prevstart + surfacelen,
			Type:     analysis.Ideographic,
		}

		prevstart = prevstart + surfacelen
		rv = append(rv, token)
	}

	return rv
}

func KagomeMorphTokenizerConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.Tokenizer, error) {
	return NewKagomeMorphTokenizer(), nil
}

func init() {
	registry.RegisterTokenizer(TokenizerName, KagomeMorphTokenizerConstructor)
}
