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
	"unicode"
	"unicode/utf8"

	"github.com/clipperhouse/uax29/v2/words"

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
	rvx := make([]analysis.TokenStream, 0, 10) // When rv gets full, append to rvx.
	rv := make(analysis.TokenStream, 0, 1)

	ta := []analysis.Token(nil)
	taNext := 0

	segmenter := words.FromBytes(input)
	start := 0
	pos := 1

	guessRemaining := func(end int) int {
		avgSegmentLen := end / (len(rv) + 1)
		if avgSegmentLen < 1 {
			avgSegmentLen = 1
		}

		remainingLen := len(input) - end

		return remainingLen / avgSegmentLen
	}

	for segmenter.Next() {
		segmentBytes := segmenter.Value()
		if !alphaNumericBackwardsCompat(segmentBytes) {
			continue
		}
		end := start + len(segmentBytes)
		if taNext >= len(ta) {
			remainingSegments := guessRemaining(end)
			if remainingSegments > 1000 {
				remainingSegments = 1000
			}
			if remainingSegments < 1 {
				remainingSegments = 1
			}

			ta = make([]analysis.Token, remainingSegments)
			taNext = 0
		}

		token := &ta[taNext]
		taNext++

		token.Term = segmentBytes
		token.Start = segmenter.Start()
		token.End = segmenter.End()
		token.Position = pos
		token.Type = getType(segmentBytes)

		if len(rv) >= cap(rv) { // When rv is full, save it into rvx.
			rvx = append(rvx, rv)

			rvCap := cap(rv) * 2
			if rvCap > 256 {
				rvCap = 256
			}

			rv = make(analysis.TokenStream, 0, rvCap) // Next rv cap is bigger.
		}

		rv = append(rv, token)
		pos++

		start = end
	}

	if len(rvx) > 0 {
		n := len(rv)
		for _, r := range rvx {
			n += len(r)
		}
		rall := make(analysis.TokenStream, 0, n)
		for _, r := range rvx {
			rall = append(rall, r...)
		}
		return append(rall, rv...)
	}

	return rv
}

func UnicodeTokenizerConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.Tokenizer, error) {
	return NewUnicodeTokenizer(), nil
}

func init() {
	err := registry.RegisterTokenizer(Name, UnicodeTokenizerConstructor)
	if err != nil {
		panic(err)
	}
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

// alphaNumeric is a filter which returns only tokens
// that contain a Letter or Number, as defined by Unicode.
func alphaNumeric(token []byte) bool {
	pos := 0
	for pos < len(token) {
		r, w := utf8.DecodeRune(token[pos:])
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			// we use these methods instead of unicode.In for
			// performance; these methods have ASCII fast paths
			return true
		}
		pos += w
	}

	return false
}

// alphaNumericBackwardsCompat is a filter which returns only tokens
// that contain a Letter or Number, as defined by Unicode.
// It filters out Thai characters as the old segmenter did.
func alphaNumericBackwardsCompat(token []byte) bool {
	for pos := 0; pos < len(token); {
		r, w := utf8.DecodeRune(token[pos:])

		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			// Filter out Thai characters (except numbers) to match old segmenter behavior
			if unicode.Is(unicode.Thai, r) && !unicode.IsNumber(r) {
				return false
			}
			return true
		}
		pos += w
	}

	return false
}
