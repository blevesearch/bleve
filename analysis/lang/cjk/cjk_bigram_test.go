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

package cjk

import (
	"container/ring"
	"reflect"
	"testing"

	"github.com/blevesearch/bleve/v2/analysis"
)

// Helper function to create a token
func makeToken(term string, start, end, pos int) *analysis.Token {
	return &analysis.Token{
		Term:     []byte(term),
		Start:    start,
		End:      end,
		Position: pos, // Note: buildUnigram uses the 'pos' argument, not the token's original pos
		Type:     analysis.Ideographic,
	}
}

func TestCJKBigramFilter_buildUnigram(t *testing.T) {
	filter := NewCJKBigramFilter(false)

	tests := []struct {
		name        string
		ringSetup   func() (*ring.Ring, int) // Function to set up the ring and itemsInRing
		inputPos    int                      // Position to pass to buildUnigram
		expectToken *analysis.Token
	}{
		{
			name: "itemsInRing == 2",
			ringSetup: func() (*ring.Ring, int) {
				r := ring.New(2)
				token1 := makeToken("一", 0, 3, 1) // Original pos 1
				token2 := makeToken("二", 3, 6, 2) // Original pos 2
				r.Value = token1
				r = r.Next()
				r.Value = token2
				// r currently points to token2, r.Move(-1) points to token1
				return r, 2
			},
			inputPos: 10, // Expected output position
			expectToken: &analysis.Token{
				Type:     analysis.Single,
				Term:     []byte("一"),
				Position: 10, // Should use inputPos
				Start:    0,
				End:      3,
			},
		},
		{
			name: "itemsInRing == 1 (ring points to the single item)",
			ringSetup: func() (*ring.Ring, int) {
				r := ring.New(2)
				token1 := makeToken("三", 6, 9, 3)
				r.Value = token1
				// r points to token1
				return r, 1
			},
			inputPos: 11,
			expectToken: &analysis.Token{
				Type:     analysis.Single,
				Term:     []byte("三"),
				Position: 11, // Should use inputPos
				Start:    6,
				End:      9,
			},
		},
		{
			name: "itemsInRing == 1 (ring points to nil, next is the single item)",
			ringSetup: func() (*ring.Ring, int) {
				r := ring.New(2)
				token1 := makeToken("四", 9, 12, 4)
				r = r.Next() // r points to nil initially
				r.Value = token1
				// r points to token1
				return r, 1
			},
			inputPos: 12,
			expectToken: &analysis.Token{
				Type:     analysis.Single,
				Term:     []byte("四"),
				Position: 12, // Should use inputPos
				Start:    9,
				End:      12,
			},
		},
		{
			name: "itemsInRing == 0",
			ringSetup: func() (*ring.Ring, int) {
				r := ring.New(2)
				// Ring is empty
				return r, 0
			},
			inputPos:    13,
			expectToken: nil, // Expect nil when itemsInRing is not 1 or 2
		},
		{
			name: "itemsInRing > 2 (should behave like 0)",
			ringSetup: func() (*ring.Ring, int) {
				r := ring.New(2)
				token1 := makeToken("五", 12, 15, 5)
				token2 := makeToken("六", 15, 18, 6)
				r.Value = token1
				r = r.Next()
				r.Value = token2
				// Simulate incorrect itemsInRing count
				return r, 3
			},
			inputPos:    14,
			expectToken: nil, // Expect nil when itemsInRing is not 1 or 2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ringPtr, itemsInRing := tt.ringSetup()
			itemsInRingCopy := itemsInRing // Pass a pointer to a copy

			gotToken := filter.buildUnigram(ringPtr, &itemsInRingCopy, tt.inputPos)

			if !reflect.DeepEqual(gotToken, tt.expectToken) {
				t.Errorf("buildUnigram() got = %v, want %v", gotToken, tt.expectToken)
			}

			// Check if itemsInRing was modified (it shouldn't be by buildUnigram)
			if itemsInRingCopy != itemsInRing {
				t.Errorf("buildUnigram() modified itemsInRing, got = %d, want %d", itemsInRingCopy, itemsInRing)
			}
		})
	}
}

func TestCJKBigramFilter_outputBigram(t *testing.T) {
	// Create a filter instance (outputUnigram value doesn't matter for outputBigram)
	filter := NewCJKBigramFilter(false)

	tests := []struct {
		name        string
		ringSetup   func() (*ring.Ring, int) // Function to set up the ring and itemsInRing
		inputPos    int                      // Position to pass to outputBigram
		expectToken *analysis.Token
	}{
		{
			name: "itemsInRing == 2",
			ringSetup: func() (*ring.Ring, int) {
				r := ring.New(2)
				token1 := makeToken("一", 0, 3, 1) // Original pos 1
				token2 := makeToken("二", 3, 6, 2) // Original pos 2
				r.Value = token1
				r = r.Next()
				r.Value = token2
				// r currently points to token2, r.Move(-1) points to token1
				return r, 2
			},
			inputPos: 10, // Expected output position
			expectToken: &analysis.Token{
				Type:     analysis.Double,
				Term:     []byte("一二"), // Combined term
				Position: 10,           // Should use inputPos
				Start:    0,            // Start of first token
				End:      6,            // End of second token
			},
		},
		{
			name: "itemsInRing == 2 with different terms",
			ringSetup: func() (*ring.Ring, int) {
				r := ring.New(2)
				token1 := makeToken("你好", 0, 6, 1)
				token2 := makeToken("世界", 6, 12, 2)
				r.Value = token1
				r = r.Next()
				r.Value = token2
				return r, 2
			},
			inputPos: 5,
			expectToken: &analysis.Token{
				Type:     analysis.Double,
				Term:     []byte("你好世界"),
				Position: 5,
				Start:    0,
				End:      12,
			},
		},
		{
			name: "itemsInRing == 1",
			ringSetup: func() (*ring.Ring, int) {
				r := ring.New(2)
				token1 := makeToken("三", 6, 9, 3)
				r.Value = token1
				return r, 1
			},
			inputPos:    11,
			expectToken: nil, // Expect nil when itemsInRing is not 2
		},
		{
			name: "itemsInRing == 0",
			ringSetup: func() (*ring.Ring, int) {
				r := ring.New(2)
				// Ring is empty
				return r, 0
			},
			inputPos:    13,
			expectToken: nil, // Expect nil when itemsInRing is not 2
		},
		{
			name: "itemsInRing > 2 (should behave like 0)",
			ringSetup: func() (*ring.Ring, int) {
				r := ring.New(2)
				token1 := makeToken("五", 12, 15, 5)
				token2 := makeToken("六", 15, 18, 6)
				r.Value = token1
				r = r.Next()
				r.Value = token2
				// Simulate incorrect itemsInRing count
				return r, 3
			},
			inputPos:    14,
			expectToken: nil, // Expect nil when itemsInRing is not 2
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ringPtr, itemsInRing := tt.ringSetup()
			itemsInRingCopy := itemsInRing // Pass a pointer to a copy

			gotToken := filter.outputBigram(ringPtr, &itemsInRingCopy, tt.inputPos)

			if !reflect.DeepEqual(gotToken, tt.expectToken) {
				t.Errorf("outputBigram() got = %v, want %v", gotToken, tt.expectToken)
			}

			// Check if itemsInRing was modified (it shouldn't be by outputBigram)
			if itemsInRingCopy != itemsInRing {
				t.Errorf("outputBigram() modified itemsInRing, got = %d, want %d", itemsInRingCopy, itemsInRing)
			}
		})
	}
}

func TestCJKBigramFilter(t *testing.T) {
	tests := []struct {
		outputUnigram bool
		input         analysis.TokenStream
		output        analysis.TokenStream
	}{
		// first test that non-adjacent terms are not combined
		{
			outputUnigram: false,
			input: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("こ"),
					Type:     analysis.Ideographic,
					Position: 1,
					Start:    0,
					End:      3,
				},
				&analysis.Token{
					Term:     []byte("ん"),
					Type:     analysis.Ideographic,
					Position: 2,
					Start:    5,
					End:      8,
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("こ"),
					Type:     analysis.Single,
					Position: 1,
					Start:    0,
					End:      3,
				},
				&analysis.Token{
					Term:     []byte("ん"),
					Type:     analysis.Single,
					Position: 2,
					Start:    5,
					End:      8,
				},
			},
		},
		{
			outputUnigram: false,
			input: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("こ"),
					Type:     analysis.Ideographic,
					Position: 1,
					Start:    0,
					End:      3,
				},
				&analysis.Token{
					Term:     []byte("ん"),
					Type:     analysis.Ideographic,
					Position: 2,
					Start:    3,
					End:      6,
				},
				&analysis.Token{
					Term:     []byte("に"),
					Type:     analysis.Ideographic,
					Position: 3,
					Start:    6,
					End:      9,
				},
				&analysis.Token{
					Term:     []byte("ち"),
					Type:     analysis.Ideographic,
					Position: 4,
					Start:    9,
					End:      12,
				},
				&analysis.Token{
					Term:     []byte("は"),
					Type:     analysis.Ideographic,
					Position: 5,
					Start:    12,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("世"),
					Type:     analysis.Ideographic,
					Position: 6,
					Start:    15,
					End:      18,
				},
				&analysis.Token{
					Term:     []byte("界"),
					Type:     analysis.Ideographic,
					Position: 7,
					Start:    18,
					End:      21,
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("こん"),
					Type:     analysis.Double,
					Position: 1,
					Start:    0,
					End:      6,
				},
				&analysis.Token{
					Term:     []byte("んに"),
					Type:     analysis.Double,
					Position: 2,
					Start:    3,
					End:      9,
				},
				&analysis.Token{
					Term:     []byte("にち"),
					Type:     analysis.Double,
					Position: 3,
					Start:    6,
					End:      12,
				},
				&analysis.Token{
					Term:     []byte("ちは"),
					Type:     analysis.Double,
					Position: 4,
					Start:    9,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("は世"),
					Type:     analysis.Double,
					Position: 5,
					Start:    12,
					End:      18,
				},
				&analysis.Token{
					Term:     []byte("世界"),
					Type:     analysis.Double,
					Position: 6,
					Start:    15,
					End:      21,
				},
			},
		},
		{
			outputUnigram: true,
			input: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("こ"),
					Type:     analysis.Ideographic,
					Position: 1,
					Start:    0,
					End:      3,
				},
				&analysis.Token{
					Term:     []byte("ん"),
					Type:     analysis.Ideographic,
					Position: 2,
					Start:    3,
					End:      6,
				},
				&analysis.Token{
					Term:     []byte("に"),
					Type:     analysis.Ideographic,
					Position: 3,
					Start:    6,
					End:      9,
				},
				&analysis.Token{
					Term:     []byte("ち"),
					Type:     analysis.Ideographic,
					Position: 4,
					Start:    9,
					End:      12,
				},
				&analysis.Token{
					Term:     []byte("は"),
					Type:     analysis.Ideographic,
					Position: 5,
					Start:    12,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("世"),
					Type:     analysis.Ideographic,
					Position: 6,
					Start:    15,
					End:      18,
				},
				&analysis.Token{
					Term:     []byte("界"),
					Type:     analysis.Ideographic,
					Position: 7,
					Start:    18,
					End:      21,
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("こ"),
					Type:     analysis.Single,
					Position: 1,
					Start:    0,
					End:      3,
				},
				&analysis.Token{
					Term:     []byte("こん"),
					Type:     analysis.Double,
					Position: 1,
					Start:    0,
					End:      6,
				},
				&analysis.Token{
					Term:     []byte("ん"),
					Type:     analysis.Single,
					Position: 2,
					Start:    3,
					End:      6,
				},
				&analysis.Token{
					Term:     []byte("んに"),
					Type:     analysis.Double,
					Position: 2,
					Start:    3,
					End:      9,
				},
				&analysis.Token{
					Term:     []byte("に"),
					Type:     analysis.Single,
					Position: 3,
					Start:    6,
					End:      9,
				},
				&analysis.Token{
					Term:     []byte("にち"),
					Type:     analysis.Double,
					Position: 3,
					Start:    6,
					End:      12,
				},
				&analysis.Token{
					Term:     []byte("ち"),
					Type:     analysis.Single,
					Position: 4,
					Start:    9,
					End:      12,
				},
				&analysis.Token{
					Term:     []byte("ちは"),
					Type:     analysis.Double,
					Position: 4,
					Start:    9,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("は"),
					Type:     analysis.Single,
					Position: 5,
					Start:    12,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("は世"),
					Type:     analysis.Double,
					Position: 5,
					Start:    12,
					End:      18,
				},
				&analysis.Token{
					Term:     []byte("世"),
					Type:     analysis.Single,
					Position: 6,
					Start:    15,
					End:      18,
				},
				&analysis.Token{
					Term:     []byte("世界"),
					Type:     analysis.Double,
					Position: 6,
					Start:    15,
					End:      21,
				},
				&analysis.Token{
					Term:     []byte("界"),
					Type:     analysis.Single,
					Position: 7,
					Start:    18,
					End:      21,
				},
			},
		},
		{
			// Assuming that `、` is removed by unicode tokenizer from `こんにちは、世界`
			outputUnigram: true,
			input: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("こ"),
					Type:     analysis.Ideographic,
					Position: 1,
					Start:    0,
					End:      3,
				},
				&analysis.Token{
					Term:     []byte("ん"),
					Type:     analysis.Ideographic,
					Position: 2,
					Start:    3,
					End:      6,
				},
				&analysis.Token{
					Term:     []byte("に"),
					Type:     analysis.Ideographic,
					Position: 3,
					Start:    6,
					End:      9,
				},
				&analysis.Token{
					Term:     []byte("ち"),
					Type:     analysis.Ideographic,
					Position: 4,
					Start:    9,
					End:      12,
				},
				&analysis.Token{
					Term:     []byte("は"),
					Type:     analysis.Ideographic,
					Position: 5,
					Start:    12,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("世"),
					Type:     analysis.Ideographic,
					Position: 7,
					Start:    18,
					End:      21,
				},
				&analysis.Token{
					Term:     []byte("界"),
					Type:     analysis.Ideographic,
					Position: 8,
					Start:    21,
					End:      24,
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("こ"),
					Type:     analysis.Single,
					Position: 1,
					Start:    0,
					End:      3,
				},
				&analysis.Token{
					Term:     []byte("こん"),
					Type:     analysis.Double,
					Position: 1,
					Start:    0,
					End:      6,
				},
				&analysis.Token{
					Term:     []byte("ん"),
					Type:     analysis.Single,
					Position: 2,
					Start:    3,
					End:      6,
				},
				&analysis.Token{
					Term:     []byte("んに"),
					Type:     analysis.Double,
					Position: 2,
					Start:    3,
					End:      9,
				},
				&analysis.Token{
					Term:     []byte("に"),
					Type:     analysis.Single,
					Position: 3,
					Start:    6,
					End:      9,
				},
				&analysis.Token{
					Term:     []byte("にち"),
					Type:     analysis.Double,
					Position: 3,
					Start:    6,
					End:      12,
				},
				&analysis.Token{
					Term:     []byte("ち"),
					Type:     analysis.Single,
					Position: 4,
					Start:    9,
					End:      12,
				},
				&analysis.Token{
					Term:     []byte("ちは"),
					Type:     analysis.Double,
					Position: 4,
					Start:    9,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("は"),
					Type:     analysis.Single,
					Position: 5,
					Start:    12,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("世"),
					Type:     analysis.Single,
					Position: 6,
					Start:    18,
					End:      21,
				},
				&analysis.Token{
					Term:     []byte("世界"),
					Type:     analysis.Double,
					Position: 6,
					Start:    18,
					End:      24,
				},
				&analysis.Token{
					Term:     []byte("界"),
					Type:     analysis.Single,
					Position: 7,
					Start:    21,
					End:      24,
				},
			},
		},
		{
			outputUnigram: false,
			input: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("こ"),
					Type:     analysis.Ideographic,
					Position: 1,
					Start:    0,
					End:      3,
				},
				&analysis.Token{
					Term:     []byte("ん"),
					Type:     analysis.Ideographic,
					Position: 2,
					Start:    3,
					End:      6,
				},
				&analysis.Token{
					Term:     []byte("に"),
					Type:     analysis.Ideographic,
					Position: 3,
					Start:    6,
					End:      9,
				},
				&analysis.Token{
					Term:     []byte("ち"),
					Type:     analysis.Ideographic,
					Position: 4,
					Start:    9,
					End:      12,
				},
				&analysis.Token{
					Term:     []byte("は"),
					Type:     analysis.Ideographic,
					Position: 5,
					Start:    12,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("cat"),
					Type:     analysis.AlphaNumeric,
					Position: 6,
					Start:    12,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("世"),
					Type:     analysis.Ideographic,
					Position: 7,
					Start:    18,
					End:      21,
				},
				&analysis.Token{
					Term:     []byte("界"),
					Type:     analysis.Ideographic,
					Position: 8,
					Start:    21,
					End:      24,
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("こん"),
					Type:     analysis.Double,
					Position: 1,
					Start:    0,
					End:      6,
				},
				&analysis.Token{
					Term:     []byte("んに"),
					Type:     analysis.Double,
					Position: 2,
					Start:    3,
					End:      9,
				},
				&analysis.Token{
					Term:     []byte("にち"),
					Type:     analysis.Double,
					Position: 3,
					Start:    6,
					End:      12,
				},
				&analysis.Token{
					Term:     []byte("ちは"),
					Type:     analysis.Double,
					Position: 4,
					Start:    9,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("cat"),
					Type:     analysis.AlphaNumeric,
					Position: 5,
					Start:    12,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("世界"),
					Type:     analysis.Double,
					Position: 6,
					Start:    18,
					End:      24,
				},
			},
		},
		{
			outputUnigram: false,
			input: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("パイプライン"),
					Type:     analysis.Ideographic,
					Position: 1,
					Start:    0,
					End:      18,
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("パイ"),
					Type:     analysis.Double,
					Position: 1,
					Start:    0,
					End:      6,
				},
				&analysis.Token{
					Term:     []byte("イプ"),
					Type:     analysis.Double,
					Position: 2,
					Start:    3,
					End:      9,
				},
				&analysis.Token{
					Term:     []byte("プラ"),
					Type:     analysis.Double,
					Position: 3,
					Start:    6,
					End:      12,
				},
				&analysis.Token{
					Term:     []byte("ライ"),
					Type:     analysis.Double,
					Position: 4,
					Start:    9,
					End:      15,
				},
				&analysis.Token{
					Term:     []byte("イン"),
					Type:     analysis.Double,
					Position: 5,
					Start:    12,
					End:      18,
				},
			},
		},
	}

	for _, test := range tests {
		cjkBigramFilter := NewCJKBigramFilter(test.outputUnigram)
		actual := cjkBigramFilter.Filter(test.input)
		if !reflect.DeepEqual(actual, test.output) {
			t.Errorf("expected %s, got %s", test.output, actual)
		}
	}
}
