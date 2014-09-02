//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package unicode_normalize

import (
	"fmt"

	"code.google.com/p/go.text/unicode/norm"
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

const Name = "normalize_unicode"

const NFC = "nfc"
const NFD = "nfd"
const NFKC = "nfkc"
const NFKD = "nfkd"

var forms = map[string]norm.Form{
	NFC:  norm.NFC,
	NFD:  norm.NFD,
	NFKC: norm.NFKC,
	NFKD: norm.NFKC,
}

type UnicodeNormalizeFilter struct {
	form norm.Form
}

func NewUnicodeNormalizeFilter(formName string) (*UnicodeNormalizeFilter, error) {
	form, ok := forms[formName]
	if !ok {
		return nil, fmt.Errorf("no form named %s", formName)
	}
	return &UnicodeNormalizeFilter{
		form: form,
	}, nil
}

func MustNewUnicodeNormalizeFilter(formName string) *UnicodeNormalizeFilter {
	filter, err := NewUnicodeNormalizeFilter(formName)
	if err != nil {
		panic(err)
	}
	return filter
}

func (s *UnicodeNormalizeFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	rv := make(analysis.TokenStream, 0)

	for _, token := range input {
		token.Term = s.form.Bytes(token.Term)
		rv = append(rv, token)
	}

	return rv
}

func UnicodeNormalizeFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	form := NFKC
	formVal, ok := config["form"].(string)
	if ok {
		form = formVal
	}
	return NewUnicodeNormalizeFilter(form)
}

func init() {
	registry.RegisterTokenFilter(Name, UnicodeNormalizeFilterConstructor)
}
