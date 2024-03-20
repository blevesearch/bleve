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

type Analyzer interface {
	Type() string
	// return value of this method depends on the type of analyzer
	Analyze([]byte) interface{}
}

const (
	TokensAnalyzerType     = "token"
	HookTokensAnalyzerType = "hook_token"
	VectorAnalyzerType     = "vector"
	HookVectorAnalyzerType = "hook_vector"
)

// # Analyzer Analyze() return type
type TokensAnalyzer struct {
	Tokens TokenStream
}
type HookTokensAnalyzer struct {
	Tokens TokenStream
	Err    error
}
type VectorAnalyzer []float64
type HookVectorAnalyzer struct {
	Vector []float64
	Err    error
}

func AnalyzeForTokens(analyzer Analyzer, input []byte) (TokenStream, error) {
	analyzerType := analyzer.Type()
	if analyzerType != TokensAnalyzerType &&
		analyzerType != HookTokensAnalyzerType {
		return nil, fmt.Errorf("cannot analyze text with analyzer of type: %s",
			analyzerType)
	}

	// analyze ouput
	analyzedOp := analyzer.Analyze(input)
	err := CheckAnalyzed(analyzedOp, analyzer)
	if err != nil {
		return nil, fmt.Errorf("incompatible analysis result for analyzer "+
			"of type: %s, err:%+v", analyzerType, err)
	}

	if analyzerType == TokensAnalyzerType {
		op := analyzedOp.(TokensAnalyzer)
		return op.Tokens, nil
	}

	// analyzerType == analysis.HookTokensAnalyzerType

	op := analyzedOp.(HookTokensAnalyzer)
	if op.Err != nil {
		return nil, fmt.Errorf("analyzer hook failed, err:%+v", op.Err)
	}

	return op.Tokens, nil
}

func CheckAnalyzed(value interface{}, analyzer Analyzer) error {
	switch analyzer.Type() {
	case TokensAnalyzerType:
		_, ok := value.(TokensAnalyzer)
		if !ok {
			return fmt.Errorf("expected TokensAnalyzer, got %T", value)
		}
	case HookTokensAnalyzerType:
		_, ok := value.(HookTokensAnalyzer)
		if !ok {
			return fmt.Errorf("expected HookTokensAnalyzer, got %T", value)
		}
	case VectorAnalyzerType:
		_, ok := value.(VectorAnalyzer)
		if !ok {
			return fmt.Errorf("expected VectorAnalyzer, got %T", value)
		}
	case HookVectorAnalyzerType:
		_, ok := value.(HookVectorAnalyzer)
		if !ok {
			return fmt.Errorf("expected HookVectorAnalyzer, got %T", value)
		}
	default:
		return fmt.Errorf("unknown analyzer type %s", analyzer.Type())
	}
	return nil
}

type DefaultAnalyzer struct {
	CharFilters  []CharFilter
	Tokenizer    Tokenizer
	TokenFilters []TokenFilter
}

func (a *DefaultAnalyzer) Analyze(input []byte) interface{} {
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

func (a *DefaultAnalyzer) Type() string {
	return TokensAnalyzerType
}

var ErrInvalidDateTime = fmt.Errorf("unable to parse datetime with any of the layouts")

var ErrInvalidTimestampString = fmt.Errorf("unable to parse timestamp string")
var ErrInvalidTimestampRange = fmt.Errorf("timestamp out of range")

type DateTimeParser interface {
	ParseDateTime(string) (time.Time, string, error)
}

type ByteArrayConverter interface {
	Convert([]byte) (interface{}, error)
}
