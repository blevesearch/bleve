//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package en

import (
	"bytes"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

const PossessiveName = "possessive_en"

const rightSingleQuotationMark = '’'
const apostrophe = '\''
const fullWidthApostrophe = '＇'

const apostropheChars = rightSingleQuotationMark + apostrophe + fullWidthApostrophe

type PossessiveFilter struct {
}

func NewPossessiveFilter() *PossessiveFilter {
	return &PossessiveFilter{}
}

func (s *PossessiveFilter) Filter(input analysis.TokenStream) analysis.TokenStream {

	for _, token := range input {
		runes := bytes.Runes(token.Term)
		if len(runes) >= 2 {
			secondToLastRune := runes[len(runes)-2]
			lastRune := runes[len(runes)-1]
			if (secondToLastRune == rightSingleQuotationMark ||
				secondToLastRune == apostrophe ||
				secondToLastRune == fullWidthApostrophe) &&
				(lastRune == 's' || lastRune == 'S') {
				token.Term = analysis.TruncateRunes(token.Term, 2)
			}
		}
	}

	return input
}

func PossessiveFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return NewPossessiveFilter(), nil
}

func init() {
	registry.RegisterTokenFilter(PossessiveName, PossessiveFilterConstructor)
}
