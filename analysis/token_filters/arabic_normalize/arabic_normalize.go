//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package arabic_normalize

import (
	"bytes"

	"github.com/couchbaselabs/bleve/analysis"
)

const (
	ALEF             = '\u0627'
	ALEF_MADDA       = '\u0622'
	ALEF_HAMZA_ABOVE = '\u0623'
	ALEF_HAMZA_BELOW = '\u0625'
	YEH              = '\u064A'
	DOTLESS_YEH      = '\u0649'
	TEH_MARBUTA      = '\u0629'
	HEH              = '\u0647'
	TATWEEL          = '\u0640'
	FATHATAN         = '\u064B'
	DAMMATAN         = '\u064C'
	KASRATAN         = '\u064D'
	FATHA            = '\u064E'
	DAMMA            = '\u064F'
	KASRA            = '\u0650'
	SHADDA           = '\u0651'
	SUKUN            = '\u0652'
)

type ArabicNormalizeFilter struct {
}

func NewArabicNormalizeFilter() *ArabicNormalizeFilter {
	return &ArabicNormalizeFilter{}
}

func (s *ArabicNormalizeFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	rv := make(analysis.TokenStream, 0)

	for _, token := range input {
		term := normalize(token.Term)
		token.Term = term
		rv = append(rv, token)
	}

	return rv
}

func normalize(input []byte) []byte {
	runes := bytes.Runes(input)
	for i := 0; i < len(runes); i++ {
		switch runes[i] {
		case ALEF_MADDA, ALEF_HAMZA_ABOVE, ALEF_HAMZA_BELOW:
			runes[i] = ALEF
		case DOTLESS_YEH:
			runes[i] = YEH
		case TEH_MARBUTA:
			runes[i] = HEH
		case TATWEEL, KASRATAN, DAMMATAN, FATHATAN, FATHA, DAMMA, KASRA, SHADDA, SUKUN:
			runes = analysis.DeleteRune(runes, i)
			i--
		}
	}
	return analysis.BuildTermFromRunes(runes)
}
