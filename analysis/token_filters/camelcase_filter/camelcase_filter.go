package camelcase_filter

import (
	"bytes"
	"unicode/utf8"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/registry"
)

const Name = "camelCase"

// CamelCaseFilter splits a given token into a set of tokens where each resulting token
// falls into one the following classes:
// 1) Upper case followed by lower case letters.
//		Terminated by a number, an upper case letter, and a non alpha-numeric symbol.
// 2) Upper case followed by upper case letters.
//		Terminated by a number, an upper case followed by a lower case letter, and a non alpha-numeric symbol.
// 3) Lower case followed by lower case letters.
//		Terminated by a number, an upper case letter, and a non alpha-numeric symbol.
// 4) Number followed by numbers.
//		Terminated by a letter, and a non alpha-numeric symbol.
// 5) Non alpha-numeric symbol followed by non alpha-numeric symbols.
//		Terminated by a number, and a letter.
//
// It does a one-time sequential pass over an input token, from left to right.
// The scan is greedy and generates the longest substring that fits into one of the classes.
//
// See the test file for examples of classes and their parsings.
type CamelCaseFilter struct{}

func NewCamelCaseFilter() *CamelCaseFilter {
	return &CamelCaseFilter{}
}

func (f *CamelCaseFilter) Filter(input analysis.TokenStream) analysis.TokenStream {
	rv := make(analysis.TokenStream, 0, len(input))

	for _, token := range input {
		runeCount := utf8.RuneCount(token.Term)
		runes := bytes.Runes(token.Term)

		p := NewParser(runeCount)
		for i := 0; i < runeCount; i++ {
			if i+1 >= runeCount {
				p.Push(runes[i], nil)
			} else {
				p.Push(runes[i], &runes[i+1])
			}
		}
		rv = append(rv, p.FlushTokens()...)
	}
	return rv
}

func CamelCaseFilterConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.TokenFilter, error) {
	return NewCamelCaseFilter(), nil
}

func init() {
	registry.RegisterTokenFilter(Name, CamelCaseFilterConstructor)
}
