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

package iso

import (
	"fmt"
	"strings"
	"time"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const Name = "isostyle"

var textLiteralDelimiter byte = '\'' // single quote

// ISO style date strings are represented in
// https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html
//
// Some format specifiers are not specified in go time package, such as:
// - 'V' for timezone name, like 'Europe/Berlin' or 'America/New_York'.
// - 'Q' for quarter of year, like Q3 or 3rd Quarter.
// - 'zzzz' for full name of timezone like "Japan Standard Time" or "Eastern Standard Time".
// - 'O' for localized zone-offset, like GMT+8 or GMT+08:00.
// - '[]' for optional section of the format.
// - 'G' for era, like AD or BC.
// - 'W' for week of month.
// - 'D' for day of year.
// So date strings with these date elements cannot be parsed.
var timeElementToLayout = map[byte]map[int]string{
	'M': {
		4: "January", // MMMM = full month name
		3: "Jan",     // MMM = short month name
		2: "01",      // MM = month of year (2 digits) (01-12)
		1: "1",       // M = month of year (1 digit) (1-12)
	},
	'd': {
		2: "02", // dd = day of month (2 digits) (01-31)
		1: "2",  // d = day of month (1 digit) (1-31)
	},
	'a': {
		2: "pm", // aa = pm/am
		1: "PM", // a = PM/AM
	},
	'H': {
		2: "15", // HH = hour (24 hour clock) (2 digits)
		1: "15", // H = hour (24 hour clock) (1 digit)
	},
	'm': {
		2: "04", // mm = minute (2 digits)
		1: "4",  // m = minute (1 digit)
	},
	's': {
		2: "05", // ss = seconds (2 digits)
		1: "5",  // s = seconds (1 digit)
	},

	// timezone offsets from UTC below
	'X': {
		5: "Z07:00:00", // XXXXX = timezone offset (+-hh:mm:ss)
		4: "Z070000",   // XXXX = timezone offset (+-hhmmss)
		3: "Z07:00",    // XXX = timezone offset (+-hh:mm)
		2: "Z0700",     // XX = timezone offset (+-hhmm)
		1: "Z07",       // X = timezone offset (+-hh)
	},
	'x': {
		5: "-07:00:00", // xxxxx = timezone offset (+-hh:mm:ss)
		4: "-070000",   // xxxx = timezone offset (+-hhmmss)
		3: "-07:00",    // xxx = timezone offset (+-hh:mm)
		2: "-0700",     // xx = timezone offset (+-hhmm)
		1: "-07",       // x = timezone offset (+-hh)
	},
}

type DateTimeParser struct {
	layouts []string
}

func New(layouts []string) *DateTimeParser {
	return &DateTimeParser{
		layouts: layouts,
	}
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

func letterCounter(layout string, idx int) int {
	count := 1
	for idx+count < len(layout) {
		if layout[idx+count] == layout[idx] {
			count++
		} else {
			break
		}
	}
	return count
}

func invalidFormatError(character byte, count int) error {
	return fmt.Errorf("invalid format string, unknown format specifier: %s", strings.Repeat(string(character), count))
}

func parseISOString(layout string) (string, error) {
	var dateTimeLayout strings.Builder

	for idx := 0; idx < len(layout); {
		// check if the character is a text literal delimiter (')
		if layout[idx] == textLiteralDelimiter {
			if idx+1 < len(layout) && layout[idx+1] == textLiteralDelimiter {
				// if the next character is also a text literal delimiter, then
				// copy the character as is
				dateTimeLayout.WriteByte(textLiteralDelimiter)
				idx += 2
				continue
			}
			// find the next text literal delimiter
			for idx++; idx < len(layout); idx++ {
				if layout[idx] == textLiteralDelimiter {
					break
				}
				dateTimeLayout.WriteByte(layout[idx])
			}
			// idx can either be equal to len(layout) if the text literal delimiter is not found
			// after the first text literal delimiter or it will be equal to the index of the
			// second text literal delimiter
			if idx == len(layout) {
				// text literal delimiter not found error
				return "", fmt.Errorf("invalid format string, expected text literal delimiter: %s", string(textLiteralDelimiter))
			}
			// increment idx to skip the second text literal delimiter
			idx++
			continue
		}
		// check if character is a letter in english alphabet - a-zA-Z which are reserved
		// for format specifiers
		if (layout[idx] >= 'a' && layout[idx] <= 'z') || (layout[idx] >= 'A' && layout[idx] <= 'Z') {
			// find the number of times the character occurs consecutively
			count := letterCounter(layout, idx)
			character := layout[idx]
			// first check the table
			if layout, ok := timeElementToLayout[character][count]; ok {
				dateTimeLayout.WriteString(layout)
			} else {
				switch character {
				case 'y', 'u', 'Y':
					// year
					if count == 2 {
						dateTimeLayout.WriteString("06")
					} else {
						format := fmt.Sprintf("%%0%ds", count)
						dateTimeLayout.WriteString(fmt.Sprintf(format, "2006"))
					}
				case 'h', 'K':
					// hour (1-12)
					switch count {
					case 2:
						// hh, KK -> 03
						dateTimeLayout.WriteString("03")
					case 1:
						// h, K -> 3
						dateTimeLayout.WriteString("3")
					default:
						// e.g., hhh
						return "", invalidFormatError(character, count)
					}
				case 'E':
					// day of week
					if count == 4 {
						dateTimeLayout.WriteString("Monday") // EEEE -> Monday
					} else if count <= 3 {
						dateTimeLayout.WriteString("Mon") // E, EE, EEE -> Mon
					} else {
						return "", invalidFormatError(character, count) // e.g., EEEEE
					}
				case 'S':
					// fraction of second
					// .SSS = millisecond
					// .SSSSSS = microsecond
					// .SSSSSSSSS = nanosecond
					if count > 9 {
						return "", invalidFormatError(character, count)
					}
					dateTimeLayout.WriteString(strings.Repeat(string('0'), count))
				case 'z':
					// timezone id
					if count < 5 {
						dateTimeLayout.WriteString("MST")
					} else {
						return "", invalidFormatError(character, count)
					}
				default:
					return "", invalidFormatError(character, count)
				}
			}
			idx += count
		} else {
			// copy the character as is
			dateTimeLayout.WriteByte(layout[idx])
			idx++
		}
	}
	return dateTimeLayout.String(), nil
}

func DateTimeParserConstructor(config map[string]interface{}, cache *registry.Cache) (analysis.DateTimeParser, error) {
	layouts, ok := config["layouts"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("must specify layouts")
	}
	var layoutStrs []string
	for _, layout := range layouts {
		layoutStr, ok := layout.(string)
		if ok {
			layout, err := parseISOString(layoutStr)
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
