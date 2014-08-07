//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package edge_ngram_filter

import (
	"bytes"
	"unicode/utf8"

	"github.com/couchbaselabs/bleve/analysis"
)

type Side bool

const BACK Side = true
const FRONT Side = false

type EdgeNgramFilter struct {
	back      Side
	minLength int
	maxLength int
}

func NewEdgeNgramFilter(side Side, minLength, maxLength int) *EdgeNgramFilter {
	return &EdgeNgramFilter{
		back:      side,
		minLength: minLength,
		maxLength: maxLength,
	}
}

func (s *EdgeNgramFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	rv := make(analysis.TokenStream, 0)

	for _, token := range input {
		runeCount := utf8.RuneCount(token.Term)
		runes := bytes.Runes(token.Term)
		if s.back {
			i := runeCount
			// index of the starting rune for this token
			for ngramSize := s.minLength; ngramSize <= s.maxLength; ngramSize++ {
				// build an ngram of this size starting at i
				if i-ngramSize > 0 {
					ngramTerm := buildTermFromRunes(runes[i-ngramSize : i])
					token := analysis.Token{
						Position: token.Position,
						Start:    token.Start,
						End:      token.End,
						Type:     token.Type,
						Term:     ngramTerm,
					}
					rv = append(rv, &token)
				}
			}
		} else {
			i := 0
			// index of the starting rune for this token
			for ngramSize := s.minLength; ngramSize <= s.maxLength; ngramSize++ {
				// build an ngram of this size starting at i
				if i+ngramSize <= runeCount {
					ngramTerm := buildTermFromRunes(runes[i : i+ngramSize])
					token := analysis.Token{
						Position: token.Position,
						Start:    token.Start,
						End:      token.End,
						Type:     token.Type,
						Term:     ngramTerm,
					}
					rv = append(rv, &token)
				}
			}
		}
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
