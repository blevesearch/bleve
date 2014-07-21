package cld2

import (
	"reflect"
	"testing"

	"github.com/couchbaselabs/bleve/analysis"
)

func TestCld2Tokenizer(t *testing.T) {
	tests := []struct {
		input  []byte
		output analysis.TokenStream
	}{
		{
			input: []byte("the quick brown fox"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("en"),
					Start:    0,
					End:      2,
					Position: 1,
				},
			},
		},
		{
			input: []byte("こんにちは世界"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("ja"),
					Start:    0,
					End:      2,
					Position: 1,
				},
			},
		},
		{
			input: []byte("แยกคำภาษาไทยก็ทำได้นะจ้ะ"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("th"),
					Start:    0,
					End:      2,
					Position: 1,
				},
			},
		},
		{
			input: []byte("مرحبا، العالم!"),
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("ar"),
					Start:    0,
					End:      2,
					Position: 1,
				},
			},
		},
	}

	tokenizer := NewCld2Tokenizer()
	for _, test := range tests {
		res := tokenizer.Tokenize(test.input)
		if !reflect.DeepEqual(res, test.output) {
			t.Errorf("expected:")
			for _, token := range test.output {
				t.Errorf("%#v - %s", token, token.Term)
			}
			t.Errorf("got:")
			for _, token := range res {
				t.Errorf("%#v - %s", token, token.Term)
			}
		}
	}

}
