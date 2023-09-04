package percent

import (
	"fmt"
	"testing"
)

func TestConversionFromPercentStyle(t *testing.T) {
	tests := []struct {
		input  string
		output string
		err    error
	}{
		{
			input:  "%Y-%m-%d",
			output: "2006-1-2",
			err:    nil,
		},
		{
			input:  "%Y/%M%%%%%DT%H%i:%S",
			output: "2006/01%%02T034:05",
			err:    nil,
		},
		{
			input:  "%Y-%M-%DT%O:%I:%S%ZM",
			output: "2006-01-02T15:04:05Z0700",
			err:    nil,
		},
		{
			input:  "%B %D, %Y %H:%I %P %Z:M",
			output: "January 02, 2006 03:04 pm Z07:00",
			err:    nil,
		},
		{
			input:  "Hour %O Minute %iseconds %S%N Timezone:%Z:S, Weekday %a; Day %D Month %b, Year %y",
			output: "Hour 15 Minute 4seconds 05.999999999 Timezone:Z07:00:00, Weekday Mon; Day 02 Month Jan, Year 06",
			err:    nil,
		},
		{
			input:  "%Y-%M-%D%T%O:%I:%S%ZM",
			output: "",
			err:    fmt.Errorf("invalid format string, unknown format specifier: T"),
		},
		{
			input:  "%Y-%M-%DT%O:%I%S%ZM%",
			output: "",
			err:    fmt.Errorf("invalid format string, invalid format string, expected character after %%"),
		},
		{
			input:  "%Y-%M-%DT%O:%I:%S%Z",
			output: "",
			err:    fmt.Errorf("invalid format string, expected character after Z"),
		},
		{
			input:  "%Y-%M-%DT%O:%I:%S%Z:",
			output: "",
			err:    fmt.Errorf("invalid format string, expected character after colon"),
		},
		{
			input:  "%O:%I:%S%Z%H:%M:%S",
			output: "",
			err:    fmt.Errorf("invalid format string, unknown timezone specifier: Z%%"),
		},
	}
	for _, test := range tests {
		out, err := parseFormatString(test.input)
		if err != nil && test.err == nil || err == nil && test.err != nil {
			t.Fatalf("expected error %v, got error %v", test.err, err)
		}
		if out != test.output {
			t.Fatalf("expected output %v, got %v", test.output, out)
		}
	}

}
