package hierarchy

import (
	"reflect"
	"testing"

	"bleve/v2/analysis"
)

func TestHierarchyFilter(t *testing.T) {

	tests := []struct {
		name       string
		delimiter  string
		max        int
		splitInput bool

		input  analysis.TokenStream
		output analysis.TokenStream
	}{
		{
			name: "single token a/b/c, delimiter /",
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("a/b/c"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("a"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      1,
					Position: 1,
				},
				&analysis.Token{
					Term:     []byte("a/b"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      3,
					Position: 1,
				},
				&analysis.Token{
					Term:     []byte("a/b/c"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      5,
					Position: 1,
				},
			},
			delimiter:  "/",
			max:        10,
			splitInput: true,
		},
		{
			name: "multiple tokens already split a b c, delimiter /",
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("a"),
				},
				&analysis.Token{
					Term: []byte("b"),
				},
				&analysis.Token{
					Term: []byte("c"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("a"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      1,
					Position: 1,
				},
				&analysis.Token{
					Term:     []byte("a/b"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      3,
					Position: 1,
				},
				&analysis.Token{
					Term:     []byte("a/b/c"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      5,
					Position: 1,
				},
			},
			delimiter:  "/",
			max:        10,
			splitInput: true,
		},
		{
			name: "single token a/b/c, delimiter /, limit 2",
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("a/b/c"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("a"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      1,
					Position: 1,
				},
				&analysis.Token{
					Term:     []byte("a/b"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      3,
					Position: 1,
				},
			},
			delimiter:  "/",
			max:        2,
			splitInput: true,
		},
		{
			name: "multiple tokens already split a b c, delimiter /, limit 2",
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("a"),
				},
				&analysis.Token{
					Term: []byte("b"),
				},
				&analysis.Token{
					Term: []byte("c"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("a"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      1,
					Position: 1,
				},
				&analysis.Token{
					Term:     []byte("a/b"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      3,
					Position: 1,
				},
			},
			delimiter:  "/",
			max:        2,
			splitInput: true,
		},

		{
			name: "single token a/b/c, delimiter /, no split",
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("a/b/c"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("a/b/c"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      5,
					Position: 1,
				},
			},
			delimiter:  "/",
			max:        10,
			splitInput: false,
		},
		{
			name: "multiple tokens already split a b c, delimiter /, no split",
			input: analysis.TokenStream{
				&analysis.Token{
					Term: []byte("a"),
				},
				&analysis.Token{
					Term: []byte("b"),
				},
				&analysis.Token{
					Term: []byte("c"),
				},
			},
			output: analysis.TokenStream{
				&analysis.Token{
					Term:     []byte("a"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      1,
					Position: 1,
				},
				&analysis.Token{
					Term:     []byte("a/b"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      3,
					Position: 1,
				},
				&analysis.Token{
					Term:     []byte("a/b/c"),
					Type:     analysis.Shingle,
					Start:    0,
					End:      5,
					Position: 1,
				},
			},
			delimiter:  "/",
			max:        10,
			splitInput: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			filter := NewHierarchyFilter([]byte(test.delimiter), test.max, test.splitInput)
			actual := filter.Filter(test.input)
			if !reflect.DeepEqual(actual, test.output) {
				t.Errorf("expected %s, got %s", test.output, actual)
			}
		})
	}

}
