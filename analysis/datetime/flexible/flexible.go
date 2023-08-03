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

package flexible

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const Name = "flexiblego"

var formatDelimiter byte = '%'

var formatSpecifierToLayout = map[byte]string{
	formatDelimiter: "%",
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
	'p':             "pm",
	'P':             "PM",
	'z':             "-0700",
	'Z':             "-070000",
	'x':             "-07",
	'v':             "-07:00",
	'V':             "-07:00:00",
	'N':             ".000000000",
	'F':             ".000000",
	'U':             ".000",
}

var validMagicNumbers = map[string]struct{}{
	"2006":    {},
	"06":      {}, // Year
	"01":      {},
	"1":       {},
	"_1":      {},
	"January": {},
	"Jan":     {}, // Month
	"02":      {},
	"2":       {},
	"_2":      {},
	"Monday":  {},
	"Mon":     {}, // Day
	"15":      {},
	"3":       {},
	"03":      {}, // Hour
	"4":       {},
	"04":      {}, // Minute
	"5":       {},
	"05":      {}, // Second
	"0700":    {},
	"070000":  {},
	"07":      {},
	"00":      {},
	"":        {},
}

var layoutSplitRegex = regexp.MustCompile("[- :T,Z\\.<>;\\?!`~@#$%\\^&\\*|\\(\\){}\\[\\]/\\\\\\'\\\"]")

var layoutConstants = []string{"PM", "pm", "MST", ".999999999", ".000000000", ".000000", ".000"}

type DateTimeParser struct {
	layouts []string
}

func New(layouts []string) *DateTimeParser {
	return &DateTimeParser{
		layouts: layouts,
	}
}

func (p *DateTimeParser) ParseDateTime(input string) (time.Time, error) {
	for _, layout := range p.layouts {
		rv, err := time.Parse(layout, input)
		if err == nil {
			if rv.Year() == 0 && !rv.IsZero() {
				// year is zero, so this time.Time has unspecified date
				// but is Not Zero so must have time only
				rv = rv.AddDate(1700, 0, 0)
			}
			return rv, nil
		}
	}
	return time.Time{}, analysis.ErrInvalidDateTime
}

func parseFormatString(formatString string) (string, error) {
	dateTimeLayout := ""
	usingNewFormat := false
	for idx := 0; idx < len(formatString); {
		if formatString[idx] == formatDelimiter {
			if idx+1 < len(formatString) {
				if layout, ok := formatSpecifierToLayout[formatString[idx+1]]; ok {
					dateTimeLayout += layout
					idx += 2
					usingNewFormat = true
				} else {
					return "", fmt.Errorf("invalid format string, unknown format specifier: " + string(formatString[idx+1]))
				}
			} else {
				return "", fmt.Errorf("invalid format string, expected character after " + string(formatDelimiter))
			}
		} else {
			dateTimeLayout += string(formatString[idx])
			idx++
		}
	}
	if !usingNewFormat {
		return "", nil
	}
	return dateTimeLayout, nil
}

func stripLayout(layout string) string {
	for _, k := range layoutConstants {
		layout = strings.ReplaceAll(layout, k, "")
	}
	return layout
}

func validateLayout(layout string) bool {
	layout = stripLayout(layout)
	split := layoutSplitRegex.Split(layout, -1)
	for i := range split {
		_, found := validMagicNumbers[split[i]]
		if !found {
			return false
		}
	}
	return true
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
			if layout == "" {
				// if layout is empty, and there is no error then it means that the layoutStr
				// is not using the new format and is in the old format
				if !validateLayout(layoutStr) {
					return nil, fmt.Errorf("invalid datetime parser layout: %s,"+
						" please refer to https://pkg.go.dev/time#pkg-constants for supported"+
						" layouts", layoutStr)
				}
				layout = layoutStr
			}
			layoutStrs = append(layoutStrs, layout)
		}
	}
	return New(layoutStrs), nil
}

func init() {
	registry.RegisterDateTimeParser(Name, DateTimeParserConstructor)
}
