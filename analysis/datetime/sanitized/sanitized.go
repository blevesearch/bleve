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

package sanitized

import (
	"fmt"
	"regexp"
	"time"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const Name = "sanitizedgo"

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
	"__2":     {},
	"002":     {},
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

var layoutSplitRegex = regexp.MustCompile("[\\+\\-= :T,Z\\.<>;\\?!`~@#$%\\^&\\*|'\"\\(\\){}\\[\\]/\\\\]")

var layoutStripRegex = regexp.MustCompile(`PM|pm|\.9+|\.0+|MST`)

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

// date time layouts must be a combination of constants specified in golang time package
// https://pkg.go.dev/time#pkg-constants
// this validation verifies that only these constants are used in the custom layout
// for compatibility with the golang time package
func validateLayout(layout string) bool {
	// first we strip out commonly used constants
	// such as "PM" which can be present in the layout
	// right after a time component, e.g. 03:04PM;
	// because regex split cannot separate "03:04PM" into
	// "03:04" and "PM". We also strip out ".9+" and ".0+"
	// which represent fractional seconds.
	layout = layoutStripRegex.ReplaceAllString(layout, "")
	// then we split the layout by non-constant characters
	// which is a regex and verify that each split is a valid magic number
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
			if !validateLayout(layoutStr) {
				return nil, fmt.Errorf("invalid datetime parser layout: %s,"+
					" please refer to https://pkg.go.dev/time#pkg-constants for supported"+
					" layouts", layoutStr)
			}
			layoutStrs = append(layoutStrs, layoutStr)
		}
	}
	return New(layoutStrs), nil
}

func init() {
	registry.RegisterDateTimeParser(Name, DateTimeParserConstructor)
}
