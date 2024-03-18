//  Copyright (c) 2014 Couchbase, Inc.
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

package unicode

import (
	"github.com/clipperhouse/uax29/iterators/filter"
	"github.com/clipperhouse/uax29/words"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const Name = "unicode"

type UnicodeTokenizer struct {
}

func NewUnicodeTokenizer() *UnicodeTokenizer {
	return &UnicodeTokenizer{}
}

func (rt *UnicodeTokenizer) Tokenize(input []byte) analysis.TokenStream {
	inputBytes := len(input)

	// An optimization to pre-allocate & avoid re-sizing
	const guessBytesPerToken = 6
	guessTokens := (inputBytes / guessBytesPerToken) | 1 // ensure minimum of 1

	result := make(analysis.TokenStream, 0, guessTokens)

	// Pre-allocate token pool
	pool := make([]analysis.Token, guessTokens)
	poolIndex := 0

	segmenter := words.NewSegmenter(input)
	segmenter.Filter(filter.AlphaNumeric)

	for segmenter.Next() {
		if poolIndex >= len(pool) {
			bytesSoFar := segmenter.End()
			tokensSoFar := len(result) | 1
			avgBytesPerToken := (bytesSoFar / tokensSoFar) | 1
			guessTokensRemaining := ((inputBytes - bytesSoFar) / avgBytesPerToken) | 1
			pool = make([]analysis.Token, guessTokensRemaining)
			poolIndex = 0
		}

		token := &pool[poolIndex]
		poolIndex++

		token.Term = segmenter.Bytes()
		token.Start = segmenter.Start()
		token.End = segmenter.End()
		token.Position = len(result) + 1 // 1-indexed
		token.Type = getType(segmenter.Bytes())

		result = append(result, token)
	}

	return result
}

func UnicodeTokenizerConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.Tokenizer, error) {
	return NewUnicodeTokenizer(), nil
}

func init() {
	registry.RegisterTokenizer(Name, UnicodeTokenizerConstructor)
}

func getType(segment []byte) analysis.TokenType {
	switch {
	case words.BleveIdeographic(segment):
		return analysis.Ideographic
	case words.BleveNumeric(segment):
		return analysis.Numeric
	}
	return analysis.AlphaNumeric
}
