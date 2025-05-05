//  Copyright (c) 2023 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package percent

import (
	"fmt"
	"strings"
	"time"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const Name = "percentstyle"

var formatDelimiter byte = '%'

// format specifiers as per strftime in the C standard library
// https://man7.org/linux/man-pages/man3/strftime.3.html
var formatSpecifierToLayout = map[byte]string{
	formatDelimiter: string(formatDelimiter), // %% = % (literal %)
	'a':             "Mon",                   // %a = short weekday name
	'A':             "Monday",                // %A = full weekday name
	'd':             "02",                    // %d = day of month (2 digits) (01-31)
	'e':             "2",                     // %e = day of month (1 digit) (1-31)
	'b':             "Jan",                   // %b = short month name
	'B':             "January",               // %B = full month name
	'm':             "01",                    // %m = month of year (2 digits) (01-12)
	'y':             "06",                    // %y = year without century
	'Y':             "2006",                  // %Y = year with century
	'H':             "15",                    // %H = hour (24 hour clock) (2 digits)
	'I':             "03",                    // %I = hour (12 hour clock) (2 digits)
	'l':             "3",                     // %l = hour (12 hour clock) (1 digit)
	'p':             "PM",                    // %p = PM/AM
	'P':             "pm",                    // %P = pm/am (lowercase)
	'M':             "04",                    // %M = minute (2 digits)
	'S':             "05",                    // %S = seconds (2 digits)
	'f':             "999999",                // .%f = fraction of seconds - up to microseconds (6 digits) - deci/milli/micro
	'Z':             "MST",                   // %Z = timezone name (GMT, JST, UTC etc)
	// %z is present in timezone options

	// some additional options not in strftime to support additional options such as
	// disallow 0 padding in minute and seconds, nanosecond precision, etc
	'o': "1",         // %o = month of year (1 digit) (1-12)
	'i': "4",         // %i = minute (1 digit)
	's': "5",         // %s = seconds (1 digit)
	'N': "999999999", // .%N = fraction of seconds - up to microseconds (9 digits) - milli/micro/nano
}

// some additional options for timezone
// such as allowing colon in timezone offset and specifying the seconds
// timezone offsets are from UTC
var timezoneOptions = map[string]string{
	"z":   "Z0700",     // %z = timezone offset in +-hhmm / +-(2 digit hour)(2 digit minute) +0500, -0600 etc
	"z:M": "Z07:00",    // %z:M = timezone offset(+-hh:mm) / +-(2 digit hour):(2 digit minute) +05:00, -06:00 etc
	"z:S": "Z07:00:00", // %z:M = timezone offset(+-hh:mm:ss) / +-(2 digit hour):(2 digit minute):(2 digit second) +05:20:00, -06:30:00 etc
	"zH":  "Z07",       // %zH = timezone offset(+-hh) / +-(2 digit hour) +05, -06 etc
	"zS":  "Z070000",   // %zS = timezone offset(+-hhmmss) / +-(2 digit hour)(2 digit minute)(2 digit second) +052000, -063000 etc
}

type DateTimeParser struct {
	layouts []string
}

func New(layouts []string) *DateTimeParser {
	return &DateTimeParser{
		layouts: layouts,
	}
}

func checkTZOptions(formatString string, idx int) (string, int) {
	// idx points to '%'
	// We know formatString[idx+1] == 'z'
	nextIdx := idx + 2 // Index of the character immediately after 'z'

	// Default values assume only '%z' is present
	layout := timezoneOptions["z"]
	finalIdx := nextIdx // Index after '%z'

	if nextIdx < len(formatString) {
		switch formatString[nextIdx] {
		case ':':
			// Check for modifier after the colon ':'
			colonModifierIdx := nextIdx + 1
			if colonModifierIdx < len(formatString) {
				switch formatString[colonModifierIdx] {
				case 'M':
					// Found %z:M
					layout = timezoneOptions["z:M"]
					finalIdx = colonModifierIdx + 1 // Index after %z:M
				case 'S':
					// Found %z:S
					layout = timezoneOptions["z:S"]
					finalIdx = colonModifierIdx + 1 // Index after %z:S
					// default: If %z: is followed by something else, or just %z: at the end.
					// Keep the default layout ("z") and finalIdx (idx + 2).
					// The ':' will be treated as a literal by the main loop.
				}
			}
			// else: %z: is at the very end of the string.
			// Keep the default layout ("z") and finalIdx (idx + 2).
			// The ':' will be treated as a literal by the main loop.

		case 'H':
			// Found %zH
			layout = timezoneOptions["zH"]
			finalIdx = nextIdx + 1 // Index after %zH
		case 'S':
			// Found %zS
			layout = timezoneOptions["zS"]
			finalIdx = nextIdx + 1 // Index after %zS

			// default: If %z is followed by something other than ':', 'H', or 'S'.
			// Keep the default layout ("z") and finalIdx (idx + 2).
			// The character formatString[nextIdx] will be handled by the main loop.
		}
	}
	// else: %z is at the very end of the string.
	// Keep the default layout ("z") and finalIdx (idx + 2).

	return layout, finalIdx
}

func parseFormatString(formatString string) (string, error) {
	var dateTimeLayout strings.Builder
	// iterate over the format string and replace the format specifiers with
	// the corresponding golang constants
	for idx := 0; idx < len(formatString); {
		// check if the character is a format delimiter (%)
		if formatString[idx] == formatDelimiter {
			// check if there is a character after the format delimiter (%)
			if idx+1 >= len(formatString) {
				return "", fmt.Errorf("invalid format string, expected character after %s", string(formatDelimiter))
			}
			formatSpecifier := formatString[idx+1]
			if layout, ok := formatSpecifierToLayout[formatSpecifier]; ok {
				dateTimeLayout.WriteString(layout)
				idx += 2
			} else if formatSpecifier == 'z' {
				// did not find a valid specifier
				// check if it is for timezone
				var tzLayout string
				tzLayout, idx = checkTZOptions(formatString, idx)
				dateTimeLayout.WriteString(tzLayout)
			} else {
				return "", fmt.Errorf("invalid format string, unknown format specifier: %s", string(formatSpecifier))
			}
			continue
		}
		// copy the character as is
		dateTimeLayout.WriteByte(formatString[idx])
		idx++
	}
	return dateTimeLayout.String(), nil
}

func (p *DateTimeParser) ParseDateTime(input string) (time.Time, string, error) {
	for _, layout := range p.layouts {
		rv, err := time.Parse(layout, input)
		if err == nil {
			return rv, layout, nil
		}
	}
	return time.Time{}, "", analysis.ErrInvalidDateTime
}

func DateTimeParserConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.DateTimeParser, error) {
	layouts, ok := config["layouts"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("must specify layouts")
	}

	layoutStrs := make([]string, 0, len(layouts))
	for _, layout := range layouts {
		layoutStr, ok := layout.(string)
		if ok {
			layout, err := parseFormatString(layoutStr)
			if err != nil {
				return nil, err
			}
			layoutStrs = append(layoutStrs, layout)
		}
	}

	return New(layoutStrs), nil
}

func init() {
	err := registry.RegisterDateTimeParser(Name, DateTimeParserConstructor)
	if err != nil {
		panic(err)
	}
}
