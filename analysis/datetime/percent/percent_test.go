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
			output: "2006-01-02",
			err:    nil,
		},
		{
			input:  "%Y/%m%%%%%dT%H%M:%S",
			output: "2006/01%%02T1504:05",
			err:    nil,
		},
		{
			input:  "%Y-%m-%dT%H:%M:%S %Z%z",
			output: "2006-01-02T15:04:05 MSTZ0700",
			err:    nil,
		},
		{
			input:  "%B %e, %Y %l:%i %P %z:M",
			output: "January 2, 2006 3:4 pm Z07:00",
			err:    nil,
		},
		{
			input:  "Hour %H Minute %Mseconds %S.%N Timezone:%Z:S, Weekday %a; Day %d Month %b, Year %y",
			output: "Hour 15 Minute 04seconds 05.999999999 Timezone:MST:S, Weekday Mon; Day 02 Month Jan, Year 06",
			err:    nil,
		},
		{
			input:  "%Y-%m-%dT%H:%M:%S.%N",
			output: "2006-01-02T15:04:05.999999999",
			err:    nil,
		},
		{
			input:  "%H:%M:%S %Z %z",
			output: "15:04:05 MST Z0700",
			err:    nil,
		},
		{
			input:  "%H:%M:%S %Z %z:",
			output: "15:04:05 MST Z0700:",
			err:    nil,
		},
		{
			input:  "%H:%M:%S %Z %z:M",
			output: "15:04:05 MST Z07:00",
			err:    nil,
		},
		{
			input:  "%H:%M:%S %Z %z:A",
			output: "15:04:05 MST Z0700:A",
			err:    nil,
		},
		{
			input:  "%H:%M:%S %Z %zM",
			output: "15:04:05 MST Z0700M",
			err:    nil,
		},
		{
			input:  "%H:%M:%S %Z %zS",
			output: "15:04:05 MST Z070000",
			err:    nil,
		},
		{
			input:  "%H:%M:%S %Z %z%Z %zS%z:%zH",
			output: "15:04:05 MST Z0700MST Z070000Z0700:Z07",
			err:    nil,
		},
		{
			input:  "%Y-%m-%d%T%H:%M:%S %ZM",
			output: "",
			err:    fmt.Errorf("invalid format string, unknown format specifier: T"),
		},
		{
			input:  "%Y-%m-%dT%H:%M:%S %ZM%",
			output: "",
			err:    fmt.Errorf("invalid format string, invalid format string, expected character after %%"),
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
