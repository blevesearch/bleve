//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build cld2 full

package cld2

// #cgo LDFLAGS: -lcld2_full
// #include "cld2_filter.h"
// #include <string.h>
import "C"

import (
	"unsafe"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

const Name = "detect_lang"

type Cld2Filter struct {
}

func NewCld2Filter() *Cld2Filter {
	return &Cld2Filter{}
}

func (f *Cld2Filter) Filter(input analysis.TokenStream) analysis.TokenStream {
	rv := make(analysis.TokenStream, 0)

	offset := 0
	for _, token := range input {
		var err error
		token.Term, err = f.detectLanguage(token.Term)
		if err != nil {
			token.Term = []byte("error")
		}
		token.Start = offset
		token.End = token.Start + len(token.Term)
		token.Type = analysis.AlphaNumeric
		rv = append(rv, token)
		offset = token.End + 1
	}

	return rv
}

func (f *Cld2Filter) detectLanguage(input []byte) ([]byte, error) {
	cstr := C.CString(string(input))
	res := C.DetectLang(cstr)
	return C.GoBytes(unsafe.Pointer(res), C.int(C.strlen(res))), nil
}

func Cld2FilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return NewCld2Filter(), nil
}

func init() {
	registry.RegisterTokenFilter(Name, Cld2FilterConstructor)
}
