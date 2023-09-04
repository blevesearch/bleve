//  Copyright (c) 2014 Couchbase, Inc.
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

const Name = "percentgo"

var formatDelimiter byte = '%'

var timezoneSpecifier byte = 'Z'

var formatSpecifierToLayout = map[byte]string{
	formatDelimiter: string(formatDelimiter),
	'd':             "2",
	'D':             "02",
	'm':             "1",
	'M':             "01",
	'y':             "06",
	'Y':             "2006",
	'b':             "Jan",
	'B':             "January",
	'a':             "Mon",
	'A':             "Monday",
	'h':             "3",
	'H':             "03",
	'O':             "15",
	'i':             "4",
	'I':             "04",
	's':             "5",
	'S':             "05",
	'p':             "PM",
	'P':             "pm",
	'N':             ".999999999",
}

var timezoneOptions = map[string]string{
	"Z:M": "Z07:00",
	"Z:S": "Z07:00:00",
	"ZH":  "Z07",
	"ZM":  "Z0700",
	"ZS":  "Z070000",
}

type DateTimeParser struct {
	layouts []string
}

func New(layouts []string) *DateTimeParser {
	return &DateTimeParser{
		layouts: layouts,
	}
}

func checkTZOptions(formatString string, idx int) (string, int, error) {
	key := "Z"
	if idx+1 >= len(formatString) {
		return "", 0, fmt.Errorf("invalid format string, expected character after " + string(timezoneSpecifier))
	}
	if formatString[idx+1] == ':' {
		// check if there is a character after the colon
		if idx+2 >= len(formatString) {
			return "", 0, fmt.Errorf("invalid format string, expected character after colon")
		}
		key += ":"
		idx++
	}
	key += string(formatString[idx+1])
	if layout, ok := timezoneOptions[key]; ok {
		return layout, idx + 2, nil
	}
	return "", 0, fmt.Errorf("invalid format string, unknown timezone specifier: " + key)
}

func parseFormatString(formatString string) (string, error) {
	var dateTimeLayout strings.Builder
	// iterate over the format string and replace the format specifiers with
	// the corresponding golang constants
	for idx := 0; idx < len(formatString); {
		// check if the character is a format specifier
		if formatString[idx] == formatDelimiter {
			// check if there is a character after the format specifier
			if idx+1 >= len(formatString) {
				return "", fmt.Errorf("invalid format string, expected character after " + string(formatDelimiter))
			}
			formatSpecifier := formatString[idx+1]
			if layout, ok := formatSpecifierToLayout[formatSpecifier]; ok {
				dateTimeLayout.WriteString(layout)
				idx += 2
			} else if formatSpecifier == timezoneSpecifier {
				// did not find a valid specifier
				// check if it is for timezone
				var tzLayout string
				var err error
				tzLayout, idx, err = checkTZOptions(formatString, idx+1)
				if err != nil {
					return "", err
				}
				dateTimeLayout.WriteString(tzLayout)
			} else {
				return "", fmt.Errorf("invalid format string, unknown format specifier: " + string(formatSpecifier))
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
	var layoutStrs []string
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
	registry.RegisterDateTimeParser(Name, DateTimeParserConstructor)
}
