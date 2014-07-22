package search

import (
	"testing"
)

func TestHTMLFragmentFormatterDefault(t *testing.T) {
	tests := []struct {
		fragment *Fragment
		tlm      TermLocationMap
		output   string
	}{
		{
			fragment: &Fragment{
				orig:  []byte("the quick brown fox"),
				start: 0,
				end:   19,
			},
			tlm: TermLocationMap{
				"quick": Locations{
					&Location{
						Pos:   2,
						Start: 4,
						End:   9,
					},
				},
			},
			output: "the <b>quick</b> brown fox",
		},
	}

	emHtmlFormatter := NewHTMLFragmentFormatter()
	for _, test := range tests {
		result := emHtmlFormatter.Format(test.fragment, test.tlm)
		if result != test.output {
			t.Errorf("expected `%s`, got `%s`", test.output, result)
		}
	}
}

func TestHTMLFragmentFormatterCustom(t *testing.T) {
	tests := []struct {
		fragment *Fragment
		tlm      TermLocationMap
		output   string
	}{
		{
			fragment: &Fragment{
				orig:  []byte("the quick brown fox"),
				start: 0,
				end:   19,
			},
			tlm: TermLocationMap{
				"quick": Locations{
					&Location{
						Pos:   2,
						Start: 4,
						End:   9,
					},
				},
			},
			output: "the <em>quick</em> brown fox",
		},
	}

	emHtmlFormatter := NewHTMLFragmentFormatterCustom("<em>", "</em>")
	for _, test := range tests {
		result := emHtmlFormatter.Format(test.fragment, test.tlm)
		if result != test.output {
			t.Errorf("expected `%s`, got `%s`", test.output, result)
		}
	}
}
