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
	"math"
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
	Analyze([]byte) TokenStream
}

type DefaultAnalyzer struct {
	CharFilters  []CharFilter
	Tokenizer    Tokenizer
	TokenFilters []TokenFilter
}

func (a *DefaultAnalyzer) Analyze(input []byte) TokenStream {
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

var ErrInvalidDateTime = fmt.Errorf("unable to parse datetime with any of the layouts")

const UnixSecs = "unix_sec"
const UnixMilliSecs = "unix_milli"
const UnixMicroSecs = "unix_micro"
const UnixNanoSecs = "unix_nano"

type TimestampBounds struct {
	Min int64
	Max int64
}

var UnixTimestampFormats = map[string]TimestampBounds{
	UnixSecs: {
		Min: math.MinInt64 / 1000000000,
		Max: math.MaxInt64 / 1000000000,
	},
	UnixMilliSecs: {
		Min: math.MinInt64 / 1000000,
		Max: math.MaxInt64 / 1000000,
	},
	UnixMicroSecs: {
		Min: math.MinInt64 / 1000,
		Max: math.MaxInt64 / 1000,
	},
	UnixNanoSecs: {
		Min: math.MinInt64,
		Max: math.MaxInt64,
	},
}

func convertTimestamp(timestamp int64, format string) int64 {
	switch format {
	case UnixSecs:
		timestamp *= 1000000000
	case UnixMilliSecs:
		timestamp *= 1000000
	case UnixMicroSecs:
		timestamp *= 1000
	}
	return timestamp
}

// ValidateAndConvertTimestamp validates the timestamp against the bounds and
// converts it to the nanoseconds if valid.
func ValidateAndConvertTimestamp(timestamp int64, bounds TimestampBounds, format string) (int64, error) {
	if timestamp > bounds.Min && timestamp < bounds.Max {
		return convertTimestamp(timestamp, format), nil
	}
	return 0, fmt.Errorf("timestamp out of range")
}

type DateTimeParser interface {
	ParseDateTime(string) (time.Time, error)
}

type ByteArrayConverter interface {
	Convert([]byte) (interface{}, error)
}
