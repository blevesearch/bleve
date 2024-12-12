//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package analysis

import (
	"fmt"
	"time"
)

type CharFilter interface {
	Filter([]byte) []byte
}

type TokenType int

const (
	AlphaNumeric TokenType = iota
	Ideographic
	Numeric
	DateTime
	Shingle
	Single
	Double
	Boolean
	IP
)

// Token represents one occurrence of a term at a particular location in a
// field.
type Token struct {
	// Start specifies the byte offset of the beginning of the term in the
	// field.
	Start int `json:"start"`

	// End specifies the byte offset of the end of the term in the field.
	End  int    `json:"end"`
	Term []byte `json:"term"`

	// Position specifies the 1-based index of the token in the sequence of
	// occurrences of its term in the field.
	Position int       `json:"position"`
	Type     TokenType `json:"type"`
	KeyWord  bool      `json:"keyword"`
}

func (t *Token) String() string {
	return fmt.Sprintf("Start: %d  End: %d  Position: %d  Token: %s  Type: %d", t.Start, t.End, t.Position, string(t.Term), t.Type)
}

type TokenStream []*Token

// A Tokenizer splits an input string into tokens, the usual behaviour being to
// map words to tokens.
type Tokenizer interface {
	Tokenize([]byte) TokenStream
}

// A TokenFilter adds, transforms or removes tokens from a token stream.
type TokenFilter interface {
	Filter(TokenStream) TokenStream
}

// -----------------------------------------------------------------------------

type AnalyzerType int

const (
	TokensAnalyzer AnalyzerType = iota
	VectorAnalyzer
)

type Analyzer interface {
	Type() AnalyzerType
	Analyze([]byte) (any, error)
}

// Convenience method to analyze given input using an Analyzer.
// Performs type assertion to ensure that the output is a token stream.
func AnalyzeForTokens(analyzer Analyzer, input []byte) (TokenStream, error) {
	if analyzer.Type() != TokensAnalyzer {
		return nil, fmt.Errorf("incompatible analyzer type")
	}

	output, err := analyzer.Analyze(input)
	if err != nil {
		return nil, err
	}

	rv, ok := output.(TokenStream)
	if !ok {
		return nil, fmt.Errorf("unexpected output type, expected TokenStream")
	}

	return rv, nil
}

// Convenience method to analyze given input using an Analyzer.
// Performs type assertion to ensure that the output is a vector.
func AnalyzeForVectors(analyzer Analyzer, input []byte) ([]float32, error) {
	if analyzer.Type() != VectorAnalyzer {
		return nil, fmt.Errorf("incompatible analyzer type")
	}

	output, err := analyzer.Analyze(input)
	if err != nil {
		return nil, err
	}

	rv, ok := output.([]float32)
	if !ok {
		return nil, fmt.Errorf("unexpected output type, expected []float32")
	}

	return rv, nil
}

type DefaultAnalyzer struct {
	CharFilters  []CharFilter
	Tokenizer    Tokenizer
	TokenFilters []TokenFilter
}

func (a *DefaultAnalyzer) Analyze(input []byte) (any, error) {
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
	return tokens, nil
}

func (a *DefaultAnalyzer) Type() AnalyzerType {
	return TokensAnalyzer
}

// -----------------------------------------------------------------------------

var ErrInvalidDateTime = fmt.Errorf("unable to parse datetime with any of the layouts")

var ErrInvalidTimestampString = fmt.Errorf("unable to parse timestamp string")
var ErrInvalidTimestampRange = fmt.Errorf("timestamp out of range")

type DateTimeParser interface {
	ParseDateTime(string) (time.Time, string, error)
}

type ByteArrayConverter interface {
	Convert([]byte) (interface{}, error)
}
