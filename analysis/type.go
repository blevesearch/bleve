//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package analysis

import (
	"fmt"
)

type CharFilter interface {
	Filter([]byte) []byte
}

type TokenType int

const (
	AlphaNumeric TokenType = iota
	Numeric
)

type Token struct {
	Start    int
	End      int
	Term     []byte
	Position int
	Type     TokenType
}

func (t *Token) String() string {
	return fmt.Sprintf("Start: %d  End: %d  Position: %d  Token: %s", t.Start, t.End, t.Position, string(t.Term))
}

type TokenStream []*Token

type Tokenizer interface {
	Tokenize([]byte) TokenStream
}

type TokenFilter interface {
	Filter(TokenStream) TokenStream
}

type Analyzer struct {
	CharFilters  []CharFilter
	Tokenizer    Tokenizer
	TokenFilters []TokenFilter
}

func (a *Analyzer) Analyze(input []byte) TokenStream {
	if a.CharFilters != nil {
		for _, cf := range a.CharFilters {
			input = cf.Filter(input)
		}
	}
	tokens := a.Tokenizer.Tokenize(input)
	if a.TokenFilters != nil {
		for _, tf := range a.TokenFilters {
			tokens = tf.Filter(tokens)
		}
	}
	return tokens
}
