//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package persian_normalize

import (
	"bytes"

	"github.com/couchbaselabs/bleve/analysis"
)

const (
	YEH         = '\u064A'
	FARSI_YEH   = '\u06CC'
	YEH_BARREE  = '\u06D2'
	KEHEH       = '\u06A9'
	KAF         = '\u0643'
	HAMZA_ABOVE = '\u0654'
	HEH_YEH     = '\u06C0'
	HEH_GOAL    = '\u06C1'
	HEH         = '\u0647'
)

type PersianNormalizeFilter struct {
}

func NewPersianNormalizeFilter() *PersianNormalizeFilter {
	return &PersianNormalizeFilter{}
}

func (s *PersianNormalizeFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
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
		case FARSI_YEH, YEH_BARREE:
			runes[i] = YEH
		case KEHEH:
			runes[i] = KAF
		case HEH_YEH, HEH_GOAL:
			runes[i] = HEH
		case HAMZA_ABOVE: // necessary for HEH + HAMZA
			runes = analysis.DeleteRune(runes, i)
			i--
		}
	}
	return analysis.BuildTermFromRunes(runes)
}
