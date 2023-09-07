package sanitized

import (
	"reflect"
	"testing"
)

func TestLayoutValidatorRegex(t *testing.T) {
	splitRegexTests := []struct {
		input  string
		output []string
	}{
		{
			input:  "2014-08-03",
			output: []string{"2014", "08", "03"},
		},
		{
			input:  "2014-08-03T15:59:30",
			output: []string{"2014", "08", "03", "15", "59", "30"},
		},
		{
			input:  "2014.08-03 15/59`30",
			output: []string{"2014", "08", "03", "15", "59", "30"},
		},
		{
			input:  "2014/08/03T15:59:30Z08:00",
			output: []string{"2014", "08", "03", "15", "59", "30", "08", "00"},
		},
		{
			input:  "2014\\08|03T15=59.30.999999999+08*00",
			output: []string{"2014", "08", "03", "15", "59", "30", "999999999", "08", "00"},
		},
		{
			input:  "2006-01-02T15:04:05.999999999Z07:00",
			output: []string{"2006", "01", "02", "15", "04", "05", "999999999", "07", "00"},
		},
		{
			input: "A-B C:DTE,FZG.H<I>J;K?L!M`N~O@P#Q$R%S^U&V*W|X'Y\"A(B)C{D}E[F]G/H\\I+J=L",
			output: []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P",
				"Q", "R", "S", "U", "V", "W", "X", "Y", "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "L"},
		},
	}
	regex := layoutSplitRegex
	for _, test := range splitRegexTests {
		t.Run(test.input, func(t *testing.T) {
			actualOutput := regex.Split(test.input, -1)
			if !reflect.DeepEqual(actualOutput, test.output) {
				t.Fatalf("expected output %v, got %v", test.output, actualOutput)
			}
		})
	}

	stripRegexTests := []struct {
		input  string
		output string
	}{
		{
			input:  "3PM",
			output: "3",
		},
		{
			input:  "3.0PM",
			output: "3",
		},
		{
			input:  "3.9AM",
			output: "3AM",
		},
		{
			input:  "3.999999999pm",
			output: "3",
		},
		{
			input:  "2006-01-02T15:04:05.999999999Z07:00MST",
			output: "2006-01-02T15:04:05Z07:00",
		},
		{
			input:  "Jan _2 15:04:05.0000000+07:00MST",
			output: "Jan _2 15:04:05+07:00",
		},
		{
			input:  "15:04:05.99PM+07:00MST",
			output: "15:04:05+07:00",
		},
	}
	regex = layoutStripRegex
	for _, test := range stripRegexTests {
		t.Run(test.input, func(t *testing.T) {
			actualOutput := layoutStripRegex.ReplaceAllString(test.input, "")
			if !reflect.DeepEqual(actualOutput, test.output) {
				t.Fatalf("expected output %v, got %v", test.output, actualOutput)
			}
		})
	}
}
