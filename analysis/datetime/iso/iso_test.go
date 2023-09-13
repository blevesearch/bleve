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
	}
	for _, test := range tests {
		out, err := parseISOString(test.input)
		if err != nil && test.err == nil || err == nil && test.err != nil {
			t.Fatalf("expected error %v, got error %v", test.err, err)
		}
		if out != test.output {
			t.Fatalf("expected output %v, got %v", test.output, out)
		}
	}

}
