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

package unicode

import (
	"bytes"
	"io/ioutil"
	"os"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/segment"
)

func TestUnicode(t *testing.T) {

	tests := []struct {
		input  []byte
		output analysis.TokenStream
	}{
		{
			[]byte("Hello World"),
			analysis.TokenStream{
				{
					Start:    0,
					End:      5,
					Term:     []byte("Hello"),
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    6,
					End:      11,
					Term:     []byte("World"),
					Position: 2,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
		{
			[]byte("steven's"),
			analysis.TokenStream{
				{
					Start:    0,
					End:      8,
					Term:     []byte("steven's"),
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
			},
		},
		{
			[]byte("こんにちは世界"),
			analysis.TokenStream{
				{
					Start:    0,
					End:      3,
					Term:     []byte("こ"),
					Position: 1,
					Type:     analysis.Ideographic,
				},
				{
					Start:    3,
					End:      6,
					Term:     []byte("ん"),
					Position: 2,
					Type:     analysis.Ideographic,
				},
				{
					Start:    6,
					End:      9,
					Term:     []byte("に"),
					Position: 3,
					Type:     analysis.Ideographic,
				},
				{
					Start:    9,
					End:      12,
					Term:     []byte("ち"),
					Position: 4,
					Type:     analysis.Ideographic,
				},
				{
					Start:    12,
					End:      15,
					Term:     []byte("は"),
					Position: 5,
					Type:     analysis.Ideographic,
				},
				{
					Start:    15,
					End:      18,
					Term:     []byte("世"),
					Position: 6,
					Type:     analysis.Ideographic,
				},
				{
					Start:    18,
					End:      21,
					Term:     []byte("界"),
					Position: 7,
					Type:     analysis.Ideographic,
				},
			},
		},
		{
			[]byte("age 25"),
			analysis.TokenStream{
				{
					Start:    0,
					End:      3,
					Term:     []byte("age"),
					Position: 1,
					Type:     analysis.AlphaNumeric,
				},
				{
					Start:    4,
					End:      6,
					Term:     []byte("25"),
					Position: 2,
					Type:     analysis.Numeric,
				},
			},
		},
		{
			[]byte("カ"),
			analysis.TokenStream{
				{
					Start:    0,
					End:      3,
					Term:     []byte("カ"),
					Position: 1,
					Type:     analysis.Ideographic,
				},
			},
		},
	}

	for _, test := range tests {
		tokenizer := NewUnicodeTokenizer()
		actual := tokenizer.Tokenize(test.input)

		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("Expected\n%v\n, got\n%v\n for %q", test.output, actual, string(test.input))
		}
	}
}

func TestPreviousIdentical(t *testing.T) {
	file, err := ioutil.ReadFile("testdata/sample.txt")
	if err != nil {
		t.Fatal(err)
	}

	previous := TokenizePrevious(file)
	current := NewUnicodeTokenizer().Tokenize(file)

	if len(previous) != len(current) {
		t.Errorf("len(previous): %d, len(current): %d\n", len(previous), len(current))
	}

	for i := range previous {
		prev := previous[i]
		curr := current[i]
		if !bytes.Equal(prev.Term, curr.Term) {
			t.Fatalf("previous term: %q, current term: %q", prev.Term, curr.Term)
		}
		if prev.Start != curr.Start {
			t.Fatalf("prev.Start: %d, curr.Start: %d", prev.Start, curr.Start)
		}
		if prev.End != curr.End {
			t.Fatalf("prev.End: %d, curr.End: %d", prev.End, curr.End)
		}
		if prev.Position != curr.Position {
			t.Fatalf("prev.Position: %d, curr.Position: %d", prev.Position, curr.Position)
		}
		if prev.Type != curr.Type {
			t.Errorf("prev.Type: %v, curr.Type: %v\n\n", prev.Type, curr.Type)
		}
	}
}

func BenchmarkTokenizeMultilingual(b *testing.B) {
	file, err := os.ReadFile("testdata/sample.txt")
	if err != nil {
		b.Fatal(err)
	}

	tokenizer := NewUnicodeTokenizer()

	b.SetBytes(int64(len(file)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tokens := tokenizer.Tokenize(file)
		b.ReportMetric(float64(len(tokens)), "tokens")
	}
}

func BenchmarkTokenizeMultilingualPrevious(b *testing.B) {
	file, err := os.ReadFile("testdata/sample.txt")
	if err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(len(file)))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tokens := TokenizePrevious(file)
		b.ReportMetric(float64(len(tokens)), "tokens")
	}
}

// Previous implementation for testing ↓

func TokenizePrevious(input []byte) analysis.TokenStream {
	rvx := make([]analysis.TokenStream, 0, 10) // When rv gets full, append to rvx.
	rv := make(analysis.TokenStream, 0, 1)

	ta := []analysis.Token(nil)
	taNext := 0

	segmenter := segment.NewWordSegmenterDirect(input)
	start := 0
	pos := 1

	guessRemaining := func(end int) int {
		avgSegmentLen := end / (len(rv) + 1)
		if avgSegmentLen < 1 {
			avgSegmentLen = 1
		}

		remainingLen := len(input) - end

		return remainingLen / avgSegmentLen
	}

	for segmenter.Segment() {
		segmentBytes := segmenter.Bytes()
		end := start + len(segmentBytes)
		if segmenter.Type() != segment.None {
			if taNext >= len(ta) {
				remainingSegments := guessRemaining(end)
				if remainingSegments > 1000 {
					remainingSegments = 1000
				}
				if remainingSegments < 1 {
					remainingSegments = 1
				}

				ta = make([]analysis.Token, remainingSegments)
				taNext = 0
			}

			token := &ta[taNext]
			taNext++

			token.Term = segmentBytes
			token.Start = start
			token.End = end
			token.Position = pos
			token.Type = convertType(segmenter.Type())

			if len(rv) >= cap(rv) { // When rv is full, save it into rvx.
				rvx = append(rvx, rv)

				rvCap := cap(rv) * 2
				if rvCap > 256 {
					rvCap = 256
				}

				rv = make(analysis.TokenStream, 0, rvCap) // Next rv cap is bigger.
			}

			rv = append(rv, token)
			pos++
		}
		start = end
	}

	if len(rvx) > 0 {
		n := len(rv)
		for _, r := range rvx {
			n += len(r)
		}
		rall := make(analysis.TokenStream, 0, n)
		for _, r := range rvx {
			rall = append(rall, r...)
		}
		return append(rall, rv...)
	}

	return rv
}

func convertType(segmentWordType int) analysis.TokenType {
	switch segmentWordType {
	case segment.Ideo:
		return analysis.Ideographic
	case segment.Kana:
		return analysis.Ideographic
	case segment.Number:
		return analysis.Numeric
	}
	return analysis.AlphaNumeric
}
