//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package rune_tokenizer

import (
	"unicode/utf8"

	"github.com/couchbaselabs/bleve/analysis"
)

type RuneTokenizer struct {
	c RuneTokenClassifer
}

func NewRuneTokenizer(c RuneTokenClassifer) *RuneTokenizer {
	return &RuneTokenizer{
		c: c,
	}
}

func (rt *RuneTokenizer) Tokenize(input []byte) analysis.TokenStream {
	// rv := make(analysis.TokenStream, 0)
	// runes := bytes.Runes(input)
	// nextTokenRunes := make([]rune, 0)
	// for _, r := range runes {

	// }
	// return rv

	rv := make(analysis.TokenStream, 0)

	currentInputPos := 0
	nextTokenRunes := make([]rune, 0)
	tokenPos := 1
	tokenStart := 0

	nextRune, nextRuneLen := utf8.DecodeRune(input[currentInputPos:])
	for nextRune != utf8.RuneError && currentInputPos < len(input) {
		if rt.c.InToken(nextRune) {
			nextTokenRunes = append(nextTokenRunes, nextRune)
		} else {
			// end the last loken, if one is building
			if len(nextTokenRunes) > 0 {
				nextToken := analysis.Token{
					Term:     buildTermFromRunes(nextTokenRunes),
					Position: tokenPos,
					Start:    tokenStart,
					End:      currentInputPos,
					Type:     analysis.AlphaNumeric,
				}
				rv = append(rv, &nextToken)
				nextTokenRunes = make([]rune, 0)
				tokenPos++
			}
			tokenStart = currentInputPos + nextRuneLen
		}

		currentInputPos += nextRuneLen
		nextRune, nextRuneLen = utf8.DecodeRune(input[currentInputPos:])
	}
	// build one last token if we didn't end on whitespace
	if len(nextTokenRunes) > 0 {
		nextToken := analysis.Token{
			Term:     buildTermFromRunes(nextTokenRunes),
			Position: tokenPos,
			Start:    tokenStart,
			End:      len(input),
			Type:     analysis.AlphaNumeric,
		}
		rv = append(rv, &nextToken)
		nextTokenRunes = make([]rune, 0)
		tokenPos++
	}

	return rv
}

func buildTermFromRunes(runes []rune) []byte {
	rv := make([]byte, 0, len(runes)*4)
	for _, r := range runes {
		runeBytes := make([]byte, utf8.RuneLen(r))
		utf8.EncodeRune(runeBytes, r)
		rv = append(rv, runeBytes...)
	}
	return rv
}
