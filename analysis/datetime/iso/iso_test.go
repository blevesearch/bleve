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

package iso

import (
	"fmt"
	"testing"
)

func TestConversionFromISOStyle(t *testing.T) {
	tests := []struct {
		input  string
		output string
		err    error
	}{
		{
			input:  "yyyy-MM-dd",
			output: "2006-01-02",
			err:    nil,
		},
		{
			input:  "uuu/M''''dd'T'HH:m:ss.SSS",
			output: "2006/1''02T15:4:05.000",
			err:    nil,
		},
		{
			input:  "YYYY-MM-dd'T'H:mm:ss zzz",
			output: "2006-01-02T15:04:05 MST",
			err:    nil,
		},
		{
			input:  "MMMM dd yyyy', 'HH:mm:ss.SSS",
			output: "January 02 2006, 15:04:05.000",
			err:    nil,
		},
		{
			input:  "h 'o'''' clock' a, XXX",
			output: "3 o' clock PM, Z07:00",
			err:    nil,
		},
		{
			input:  "YYYY-MM-dd'T'HH:mm:ss'Z'",
			output: "2006-01-02T15:04:05Z",
			err:    nil,
		},
		{
			input:  "E MMM d H:mm:ss z Y",
			output: "Mon Jan 2 15:04:05 MST 2006",
			err:    nil,
		},
		{
			input:  "E MMM DD H:m:s z Y",
			output: "",
			err:    fmt.Errorf("invalid format string, unknown format specifier: DD"),
		},
		{
			input:  "E MMM''''' H:m:s z Y",
			output: "",
			err:    fmt.Errorf("invalid format string, expected text literal delimiter: '"),
		},
		{
			input:  "MMMMM dd yyyy', 'HH:mm:ss.SSS",
			output: "",
			err:    fmt.Errorf("invalid format string, unknown format specifier: MMMMM"),
		},
		{
			input:  "yy", // year (2 digits)
			output: "06",
			err:    nil,
		},
		{
			input:  "yyyyy", // year (5 digits, padded)
			output: "02006",
			err:    nil,
		},
		{
			input:  "h", // hour 1-12 (1 digit)
			output: "3",
			err:    nil,
		},
		{
			input:  "hh", // hour 1-12 (2 digits)
			output: "03",
			err:    nil,
		},
		{
			input:  "KK", // hour 1-12 (2 digits, alt)
			output: "03",
			err:    nil,
		},
		{
			input:  "hhh", // invalid hour count
			output: "",
			err:    fmt.Errorf("invalid format string, unknown format specifier: hhh"),
		},
		{
			input:  "E", // Day of week (short)
			output: "Mon",
			err:    nil,
		},
		{
			input:  "EEE", // Day of week (short)
			output: "Mon",
			err:    nil,
		},
		{
			input:  "EEEE", // Day of week (long)
			output: "Monday",
			err:    nil,
		},
		{
			input:  "EEEEE", // Day of week (long)
			output: "",
			err:    fmt.Errorf("invalid format string, unknown format specifier: EEEEE"),
		},
		{
			input:  "S", // Fraction of second (1 digit)
			output: "0",
			err:    nil,
		},
		{
			input:  "SSSSSSSSS", // Fraction of second (9 digits)
			output: "000000000",
			err:    nil,
		},
		{
			input:  "SSSSSSSSSS", // Invalid fraction of second count
			output: "",
			err:    fmt.Errorf("invalid format string, unknown format specifier: SSSSSSSSSS"),
		},
		{
			input:  "z", // Timezone name (short)
			output: "MST",
			err:    nil,
		},
		{
			input:  "zzz", // Timezone name (short) - Corrected expectation
			output: "MST", // Should output MST
			err:    nil,   // Should not produce an error
		},
		{
			input:  "zzzz", // Timezone name (long) - Corrected expectation
			output: "MST",  // Should output MST
			err:    nil,    // Should not produce an error
		},
		{
			input:  "G", // Era designator (unsupported)
			output: "",
			err:    fmt.Errorf("invalid format string, unknown format specifier: G"),
		},
		{
			input:  "W", // Week of month (unsupported)
			output: "",
			err:    fmt.Errorf("invalid format string, unknown format specifier: W"),
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("test %d: %s", i, test.input), func(t *testing.T) {
			out, err := parseISOString(test.input)
			// Check error matching
			if (err != nil && test.err == nil) || (err == nil && test.err != nil) || (err != nil && test.err != nil && err.Error() != test.err.Error()) {
				t.Fatalf("expected error %v, got error %v", test.err, err)
			}
			// Check output matching only if no error was expected/occurred
			if err == nil && test.err == nil && out != test.output {
				t.Fatalf("expected output '%v', got '%v'", test.output, out)
			}
		})
	}
}
