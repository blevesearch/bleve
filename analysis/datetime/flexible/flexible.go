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
	"time"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/registry"
)

const Name = "flexiblego"

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
			return rv, nil
		}
	}
	return time.Time{}, analysis.ErrInvalidDateTime
}

func validateLayout(layout string) bool {
	validMagicNumbers := map[string]bool{
		"2006":    true,
		"06":      true, // Year
		"01":      true,
		"1":       true,
		"_1":      true,
		"January": true,
		"Jan":     true, // Month
		"02":      true,
		"2":       true,
		"_2":      true,
		"Monday":  true,
		"Mon":     true, // Day
		"15":      true,
		"3":       true,
		"03":      true, // Hour
		"4":       true,
		"04":      true, // Minute
		"5":       true,
		"05":      true, // Second
		"PM":      true,
		"pm":      true,
		"MST":     true,
		"Z0700":   true, // prints Z for UTC
		"Z070000": true,
		"Z07":     true,
		"0700":    true,
		"070000":  true,
		"07":      true,
		"":        true,
	}
	re := regexp.MustCompile("[- :T,\\.<>;\\?!`~@#$%\\^&\\*|\\(\\){}\\[\\]/\\\\]")
	split := re.Split(layout, -1)
	for _, v := range split {
		fmt.Println(v)
	}

	for i := range split {
		if !validMagicNumbers[split[i]] {
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
					" please refer to https://pkg.go.dev/time#pkg-constants for valid"+
					" constants to use", layoutStr)
			}
			layoutStrs = append(layoutStrs, layoutStr)
		}
	}
	return New(layoutStrs), nil
}

func init() {
	registry.RegisterDateTimeParser(Name, DateTimeParserConstructor)
}
