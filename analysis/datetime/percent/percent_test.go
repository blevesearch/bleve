//  Copyright (c) 2023 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package percent

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/analysis"
)

func TestConversionFromPercentStyle(t *testing.T) {
	tests := []struct {
		name   string // Added name field
		input  string
		output string
		err    error
	}{
		{
			name:   "basic YMD",
			input:  "%Y-%m-%d",
			output: "2006-01-02",
			err:    nil,
		},
		{
			name:   "YMD with double percent and literal T",
			input:  "%Y/%m%%%%%dT%H%M:%S",
			output: "2006/01%%02T1504:05",
			err:    nil,
		},
		{
			name:   "YMD T HMS Z z",
			input:  "%Y-%m-%dT%H:%M:%S %Z%z",
			output: "2006-01-02T15:04:05 MSTZ0700",
			err:    nil,
		},
		{
			name:   "Full month, padded day/hour, am/pm, z:M",
			input:  "%B %e, %Y %l:%i %P %z:M",
			output: "January 2, 2006 3:4 pm Z07:00",
			err:    nil,
		},
		{
			name:   "Long format with literals and timezone literal :S",
			input:  "Hour %H Minute %Mseconds %S.%N Timezone:%Z:S, Weekday %a; Day %d Month %b, Year %y",
			output: "Hour 15 Minute 04seconds 05.999999999 Timezone:MST:S, Weekday Mon; Day 02 Month Jan, Year 06",
			err:    nil,
		},
		{
			name:   "YMD T HMS with nanoseconds",
			input:  "%Y-%m-%dT%H:%M:%S.%N",
			output: "2006-01-02T15:04:05.999999999",
			err:    nil,
		},
		{
			name:   "HMS Z z",
			input:  "%H:%M:%S %Z %z",
			output: "15:04:05 MST Z0700",
			err:    nil,
		},
		{
			name:   "HMS Z z literal colon",
			input:  "%H:%M:%S %Z %z:",
			output: "15:04:05 MST Z0700:",
			err:    nil,
		},
		{
			name:   "HMS Z z:M",
			input:  "%H:%M:%S %Z %z:M",
			output: "15:04:05 MST Z07:00",
			err:    nil,
		},
		{
			name:   "HMS Z z:S",
			input:  "%H:%M:%S %Z %z:S",
			output: "15:04:05 MST Z07:00:00",
			err:    nil,
		},
		{
			name:   "HMS Z z: literal A",
			input:  "%H:%M:%S %Z %z:A",
			output: "15:04:05 MST Z0700:A",
			err:    nil,
		},
		{
			name:   "HMS Z z literal M",
			input:  "%H:%M:%S %Z %zM",
			output: "15:04:05 MST Z0700M",
			err:    nil,
		},
		{
			name:   "HMS Z zH",
			input:  "%H:%M:%S %Z %zH",
			output: "15:04:05 MST Z07",
			err:    nil,
		},
		{
			name:   "HMS Z zS",
			input:  "%H:%M:%S %Z %zS",
			output: "15:04:05 MST Z070000",
			err:    nil,
		},
		{
			name:   "Complex combination z zS z: zH",
			input:  "%H:%M:%S %Z %z%Z %zS%z:%zH",
			output: "15:04:05 MST Z0700MST Z070000Z0700:Z07",
			err:    nil,
		},
		{
			name:   "z at end",
			input:  "%Y-%m-%d %z",
			output: "2006-01-02 Z0700",
			err:    nil,
		},
		{
			name:   "z: at end",
			input:  "%Y-%m-%d %z:",
			output: "2006-01-02 Z0700:",
			err:    nil,
		},
		{
			name:   "zH at end",
			input:  "%Y-%m-%d %zH",
			output: "2006-01-02 Z07",
			err:    nil,
		},
		{
			name:   "zS at end",
			input:  "%Y-%m-%d %zS",
			output: "2006-01-02 Z070000",
			err:    nil,
		},
		{
			name:   "z:M at end",
			input:  "%Y-%m-%d %z:M",
			output: "2006-01-02 Z07:00",
			err:    nil,
		},
		{
			name:   "z:S at end",
			input:  "%Y-%m-%d %z:S",
			output: "2006-01-02 Z07:00:00",
			err:    nil,
		},
		{
			name:   "z followed by literal X",
			input:  "%Y-%m-%d %zX",
			output: "2006-01-02 Z0700X",
			err:    nil,
		},
		{
			name:   "z: followed by literal X",
			input:  "%Y-%m-%d %z:X",
			output: "2006-01-02 Z0700:X",
			err:    nil,
		},
		{
			name:   "Invalid specifier T",
			input:  "%Y-%m-%d%T%H:%M:%S %ZM",
			output: "",
			err:    fmt.Errorf("invalid format string, unknown format specifier: T"),
		},
		{
			name:   "Ends with %",
			input:  "%Y-%m-%dT%H:%M:%S %ZM%",
			output: "",
			err:    fmt.Errorf("invalid format string, expected character after %%"),
		},
		{
			name:   "Just %",
			input:  "%",
			output: "",
			err:    fmt.Errorf("invalid format string, expected character after %%"),
		},
		{
			name:   "Just %%",
			input:  "%%",
			output: "%",
			err:    nil,
		},
		{
			name:   "Unknown specifier x",
			input:  "%x",
			output: "",
			err:    fmt.Errorf("invalid format string, unknown format specifier: x"),
		},
		{
			name:   "Literal prefix",
			input:  "literal %Y",
			output: "literal 2006",
			err:    nil,
		},
		{
			name:   "Literal suffix",
			input:  "%Y literal",
			output: "2006 literal",
			err:    nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out, err := parseFormatString(test.input)

			// Enhanced Error Check:
			expectedErrStr := ""
			if test.err != nil {
				expectedErrStr = test.err.Error()
			}
			actualErrStr := ""
			if err != nil {
				actualErrStr = err.Error()
			}

			if expectedErrStr != actualErrStr {
				// Provide more detailed output if errors don't match as strings
				t.Fatalf("error mismatch:\nExpected error: %q\nGot error     : %q", expectedErrStr, actualErrStr)
			}

			// Original error presence check (redundant if string check passes, but safe to keep)
			if (err != nil && test.err == nil) || (err == nil && test.err != nil) {
				t.Fatalf("presence mismatch: expected error %v, got error %v", test.err, err)
			}

			// Check output matching only if no error was expected/occurred
			if err == nil && test.err == nil && out != test.output {
				t.Fatalf("output mismatch: expected '%v', got '%v'", test.output, out)
			}
		})
	}
}

func TestDateTimeParser_ParseDateTime(t *testing.T) {
	// Pre-create some parsers with known Go layouts
	parser1 := New([]string{"2006-01-02", "01/02/2006"}) // YYYY-MM-DD, MM/DD/YYYY
	parser2 := New([]string{"15:04:05"})                 // HH:MM:SS
	parserEmpty := New([]string{})                       // No layouts

	// Define expected time values
	time1, _ := time.Parse("2006-01-02", "2023-10-27")
	time2, _ := time.Parse("01/02/2006", "10/27/2023")
	time3, _ := time.Parse("15:04:05", "14:30:00")

	tests := []struct {
		name         string
		parser       *DateTimeParser
		input        string
		expectTime   time.Time
		expectLayout string
		expectErr    error
	}{
		{
			name:         "match first layout",
			parser:       parser1,
			input:        "2023-10-27",
			expectTime:   time1,
			expectLayout: "2006-01-02",
			expectErr:    nil,
		},
		{
			name:         "match second layout",
			parser:       parser1,
			input:        "10/27/2023",
			expectTime:   time2,
			expectLayout: "01/02/2006",
			expectErr:    nil,
		},
		{
			name:         "no matching layout",
			parser:       parser1,
			input:        "14:30:00", // Matches parser2's layout, not parser1's
			expectTime:   time.Time{},
			expectLayout: "",
			expectErr:    analysis.ErrInvalidDateTime,
		},
		{
			name:         "match only layout",
			parser:       parser2,
			input:        "14:30:00",
			expectTime:   time3,
			expectLayout: "15:04:05",
			expectErr:    nil,
		},
		{
			name:         "invalid date format for layout",
			parser:       parser1,
			input:        "27-10-2023", // Wrong separators
			expectTime:   time.Time{},
			expectLayout: "",
			expectErr:    analysis.ErrInvalidDateTime, // time.Parse fails on all, returns ErrInvalidDateTime
		},
		{
			name:         "empty input",
			parser:       parser1,
			input:        "",
			expectTime:   time.Time{},
			expectLayout: "",
			expectErr:    analysis.ErrInvalidDateTime,
		},
		{
			name:         "parser with no layouts",
			parser:       parserEmpty,
			input:        "2023-10-27",
			expectTime:   time.Time{},
			expectLayout: "",
			expectErr:    analysis.ErrInvalidDateTime,
		},
		{
			name:         "not a date string",
			parser:       parser1,
			input:        "hello world",
			expectTime:   time.Time{},
			expectLayout: "",
			expectErr:    analysis.ErrInvalidDateTime,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gotTime, gotLayout, gotErr := test.parser.ParseDateTime(test.input)

			// Check error
			if !reflect.DeepEqual(gotErr, test.expectErr) {
				t.Fatalf("error mismatch:\nExpected: %v\nGot:      %v", test.expectErr, gotErr)
			}

			// Check time only if no error expected
			if test.expectErr == nil {
				if !gotTime.Equal(test.expectTime) {
					t.Errorf("time mismatch:\nExpected: %v\nGot:      %v", test.expectTime, gotTime)
				}
				if gotLayout != test.expectLayout {
					t.Errorf("layout mismatch:\nExpected: %q\nGot:      %q", test.expectLayout, gotLayout)
				}
			}
		})
	}
}

func TestDateTimeParserConstructor(t *testing.T) {
	tests := []struct {
		name          string
		config        map[string]interface{}
		expectLayouts []string // Expected Go layouts after parsing
		expectErr     error
	}{
		{
			name: "valid config with multiple layouts",
			config: map[string]interface{}{
				"layouts": []interface{}{"%Y-%m-%d", "%H:%M:%S %Z"},
			},
			expectLayouts: []string{"2006-01-02", "15:04:05 MST"},
			expectErr:     nil,
		},
		{
			name: "valid config with single layout",
			config: map[string]interface{}{
				"layouts": []interface{}{"%Y/%m/%d %z:M"},
			},
			expectLayouts: []string{"2006/01/02 Z07:00"},
			expectErr:     nil,
		},
		{
			name: "valid config with complex layout",
			config: map[string]interface{}{
				"layouts": []interface{}{"%a, %d %b %Y %H:%M:%S %zH"},
			},
			expectLayouts: []string{"Mon, 02 Jan 2006 15:04:05 Z07"},
			expectErr:     nil,
		},
		{
			name: "config missing layouts key",
			config: map[string]interface{}{
				"other_key": "value",
			},
			expectLayouts: nil,
			expectErr:     fmt.Errorf("must specify layouts"),
		},
		{
			name: "config layouts not a slice",
			config: map[string]interface{}{
				"layouts": "not-a-slice", // Value is a string
			},
			expectLayouts: nil,
			// Update the expected error message
			expectErr: fmt.Errorf("must specify layouts"),
		},
		{
			name: "config layouts contains non-string",
			config: map[string]interface{}{
				"layouts": []interface{}{"%Y-%m-%d", 123},
			},
			// Should process the valid string, ignore the int
			expectLayouts: []string{"2006-01-02"},
			expectErr:     nil,
		},
		{
			name: "config layouts contains invalid percent format",
			config: map[string]interface{}{
				"layouts": []interface{}{"%Y-%m-%d", "%x"}, // %x is invalid
			},
			expectLayouts: nil,
			expectErr:     fmt.Errorf("invalid format string, unknown format specifier: x"),
		},
		{
			name: "config layouts contains format ending in %",
			config: map[string]interface{}{
				"layouts": []interface{}{"%Y-%m-%d", "%H:%M:%"},
			},
			expectLayouts: nil,
			expectErr:     fmt.Errorf("invalid format string, expected character after %%"),
		},
		{
			name: "config with empty layouts slice",
			config: map[string]interface{}{
				"layouts": []interface{}{},
			},
			expectLayouts: []string{}, // Expect an empty slice, not nil
			expectErr:     nil,
		},
		{
			name:          "nil config",
			config:        nil,
			expectLayouts: nil,
			expectErr:     fmt.Errorf("must specify layouts"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Cache is not used by this constructor, so nil is fine
			parserIntf, err := DateTimeParserConstructor(test.config, nil)

			// Check error
			// Use string comparison for errors as they might be created differently
			expectedErrStr := ""
			if test.expectErr != nil {
				expectedErrStr = test.expectErr.Error()
			}
			actualErrStr := ""
			if err != nil {
				actualErrStr = err.Error()
			}
			if expectedErrStr != actualErrStr {
				t.Fatalf("error mismatch:\nExpected: %q\nGot:      %q", expectedErrStr, actualErrStr)
			}

			// Check layouts only if no error expected
			if test.expectErr == nil {
				// Type assert to access the layouts field
				parser, ok := parserIntf.(*DateTimeParser)
				if !ok {
					t.Fatalf("constructor did not return a *DateTimeParser")
				}
				if !reflect.DeepEqual(parser.layouts, test.expectLayouts) {
					t.Errorf("layouts mismatch:\nExpected: %v\nGot:      %v", test.expectLayouts, parser.layouts)
				}
			}
		})
	}
}
