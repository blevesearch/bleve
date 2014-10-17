//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// +build icu full

package icu

// #cgo LDFLAGS: -licuuc -licudata
// #include <stdio.h>
// #include <stdlib.h>
// #include "unicode/utypes.h"
// #include "unicode/uchar.h"
// #include "unicode/ubrk.h"
// #include "unicode/ustring.h"
import "C"

import (
	"log"
	"unsafe"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

const Name = "icu"

type UnicodeWordBoundaryTokenizer struct {
	locale *C.char
}

func NewUnicodeWordBoundaryTokenizer() *UnicodeWordBoundaryTokenizer {
	return &UnicodeWordBoundaryTokenizer{}
}

func NewUnicodeWordBoundaryCustomLocaleTokenizer(locale string) *UnicodeWordBoundaryTokenizer {
	return &UnicodeWordBoundaryTokenizer{
		locale: C.CString(locale),
	}
}

func (t *UnicodeWordBoundaryTokenizer) Tokenize(input []byte) analysis.TokenStream {
	rv := make(analysis.TokenStream, 0)

	if len(input) < 1 {
		return rv
	}

	// works
	var myUnsafePointer = unsafe.Pointer(&(input[0]))
	var myCCharPointer *C.char = (*C.char)(myUnsafePointer)

	var inlen C.int32_t = C.int32_t(len(input))
	var buflen C.int32_t = C.int32_t(2*len(input) + 1) // worse case each byte becomes 2
	var stringToExamine []C.UChar = make([]C.UChar, buflen)
	var myUnsafePointerToExamine = unsafe.Pointer(&(stringToExamine[0]))
	var myUCharPointer *C.UChar = (*C.UChar)(myUnsafePointerToExamine)
	C.u_uastrncpy(myUCharPointer, myCCharPointer, inlen)

	var err C.UErrorCode = C.U_ZERO_ERROR
	bi := C.ubrk_open(C.UBRK_WORD, t.locale, myUCharPointer, -1, &err)

	if err > C.U_ZERO_ERROR {
		log.Printf("error opening boundary iterator")
		return rv
	}

	defer C.ubrk_close(bi)

	position := 0
	var prev C.int32_t
	p := C.ubrk_first(bi)
	for p != C.UBRK_DONE {

		q := C.ubrk_getRuleStatus(bi)

		// convert boundaries back to utf8 positions
		var nilCString *C.char
		var indexA C.int32_t

		C.u_strToUTF8(nilCString, 0, &indexA, myUCharPointer, prev, &err)
		if err > C.U_ZERO_ERROR && err != C.U_BUFFER_OVERFLOW_ERROR {
			log.Printf("error converting boundary %d", err)
			return rv
		} else {
			err = C.U_ZERO_ERROR
		}

		var indexB C.int32_t
		C.u_strToUTF8(nilCString, 0, &indexB, myUCharPointer, p, &err)
		if err > C.U_ZERO_ERROR && err != C.U_BUFFER_OVERFLOW_ERROR {
			log.Printf("error converting boundary %d", err)
			return rv
		} else {
			err = C.U_ZERO_ERROR
		}

		if q != 0 {
			position += 1
			token := analysis.Token{
				Start:    int(indexA),
				End:      int(indexB),
				Term:     input[indexA:indexB],
				Position: position,
				Type:     analysis.AlphaNumeric,
			}
			if q == 100 {
				token.Type = analysis.Numeric
			}
			if q == 400 {
				token.Type = analysis.Ideographic
			}
			rv = append(rv, &token)
		}
		prev = p
		p = C.ubrk_next(bi)
	}

	return rv
}

func UnicodeWordBoundaryTokenizerConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.Tokenizer, error) {
	locale := ""
	localeVal, ok := config["locale"].(string)
	if ok {
		locale = localeVal
	}
	if locale == "" {
		return NewUnicodeWordBoundaryTokenizer(), nil
	} else {
		return NewUnicodeWordBoundaryCustomLocaleTokenizer(locale), nil
	}
}

func init() {
	registry.RegisterTokenizer(Name, UnicodeWordBoundaryTokenizerConstructor)
}
