package cld2

// #cgo LDFLAGS: -Lcld2-read-only/internal/ -lcld2_full
// #include "cld2_tokenizer.h"
// #include <string.h>
import "C"

import (
	"unsafe"

	"github.com/couchbaselabs/bleve/analysis"
)

type Cld2Tokenizer struct {
}

func NewCld2Tokenizer() *Cld2Tokenizer {
	return &Cld2Tokenizer{}
}

func (rt *Cld2Tokenizer) Tokenize(input []byte) analysis.TokenStream {
	rv := make(analysis.TokenStream, 0)
	lang, err := rt.detectLanguage(input)
	if err != nil {
		return rv
	}
	token := analysis.Token{
		Term:     lang,
		Start:    0,
		End:      len(lang),
		Position: 1,
	}
	rv = append(rv, &token)
	return rv
}

func (rt *Cld2Tokenizer) detectLanguage(input []byte) ([]byte, error) {
	cstr := C.CString(string(input))
	res := C.DetectLang(cstr)
	return C.GoBytes(unsafe.Pointer(res), C.int(C.strlen(res))), nil
}
