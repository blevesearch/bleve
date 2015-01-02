//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

//go:generate nex query_string.nex
//go:generate sed -i "" -e s/Lexer/lexer/g query_string.nn.go
//go:generate sed -i "" -e s/Newlexer/newLexer/g query_string.nn.go
//go:generate sed -i "" -e s/debuglexer/debugLexer/g query_string.nn.go
//go:generate go fmt query_string.nn.go
//go:generate go tool yacc -o query_string.y.go query_string.y
//go:generate sed -i "" -e 1d query_string.y.go

package bleve

import (
	"fmt"
	"strings"
)

var debugParser bool
var debugLexer bool

func parseQuerySyntax(query string, mapping *IndexMapping) (rq Query, err error) {
	lex := newLexerWrapper(newLexer(strings.NewReader(query)))
	doParse(lex)

	if len(lex.errs) > 0 {
		return nil, fmt.Errorf(strings.Join(lex.errs, "\n"))
	} else {
		return lex.query, nil
	}
}

func doParse(lex *lexerWrapper) {
	defer func() {
		r := recover()
		if r != nil {
			lex.Error("Errors while parsing.")
		}
	}()

	yyParse(lex)
}

const (
	queryShould = iota
	queryMust
	queryMustNot
)

type lexerWrapper struct {
	nex   yyLexer
	errs  []string
	query *booleanQuery
}

func newLexerWrapper(nex yyLexer) *lexerWrapper {
	return &lexerWrapper{
		nex:   nex,
		errs:  []string{},
		query: NewBooleanQuery(nil, nil, nil),
	}
}

func (this *lexerWrapper) Lex(lval *yySymType) int {
	return this.nex.Lex(lval)
}

func (this *lexerWrapper) Error(s string) {
	this.errs = append(this.errs, s)
}
