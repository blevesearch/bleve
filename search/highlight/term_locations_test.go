package highlight

import (
	"reflect"
	"testing"
)

func TestTermLocationOverlaps(t *testing.T) {

	tests := []struct {
		left     *TermLocation
		right    *TermLocation
		expected bool
	}{
		{
			left: &TermLocation{
				Start: 0,
				End:   5,
			},
			right: &TermLocation{
				Start: 3,
				End:   7,
			},
			expected: true,
		},
		{
			left: &TermLocation{
				Start: 0,
				End:   5,
			},
			right: &TermLocation{
				Start: 5,
				End:   7,
			},
			expected: false,
		},
		{
			left: &TermLocation{
				Start: 0,
				End:   5,
			},
			right: &TermLocation{
				Start: 7,
				End:   11,
			},
			expected: false,
		},
	}

	for _, test := range tests {
		actual := test.left.Overlaps(test.right)
		if actual != test.expected {
			t.Errorf("expected %t got %t for %#v", test.expected, actual, test)
		}
	}
}

func TestTermLocationsMergeOverlapping(t *testing.T) {

	tests := []struct {
		input  TermLocations
		output TermLocations
	}{
		{
			input:  TermLocations{},
			output: TermLocations{},
		},
		{
			input: TermLocations{
				&TermLocation{
					Start: 0,
					End:   5,
				},
				&TermLocation{
					Start: 7,
					End:   11,
				},
			},
			output: TermLocations{
				&TermLocation{
					Start: 0,
					End:   5,
				},
				&TermLocation{
					Start: 7,
					End:   11,
				},
			},
		},
		{
			input: TermLocations{
				&TermLocation{
					Start: 0,
					End:   5,
				},
				&TermLocation{
					Start: 4,
					End:   11,
				},
			},
			output: TermLocations{
				&TermLocation{
					Start: 0,
					End:   11,
				},
				nil,
			},
		},
		{
			input: TermLocations{
				&TermLocation{
					Start: 0,
					End:   5,
				},
				&TermLocation{
					Start: 4,
					End:   11,
				},
				&TermLocation{
					Start: 9,
					End:   13,
				},
			},
			output: TermLocations{
				&TermLocation{
					Start: 0,
					End:   13,
				},
				nil,
				nil,
			},
		},
		{
			input: TermLocations{
				&TermLocation{
					Start: 0,
					End:   5,
				},
				&TermLocation{
					Start: 4,
					End:   11,
				},
				&TermLocation{
					Start: 9,
					End:   13,
				},
				&TermLocation{
					Start: 15,
					End:   21,
				},
			},
			output: TermLocations{
				&TermLocation{
					Start: 0,
					End:   13,
				},
				nil,
				nil,
				&TermLocation{
					Start: 15,
					End:   21,
				},
			},
		},
	}

	for _, test := range tests {
		test.input.MergeOverlapping()
		if !reflect.DeepEqual(test.input, test.output) {
			t.Errorf("expected: %#v got %#v", test.output, test.input)
		}
	}
}
