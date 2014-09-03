//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package ansi

import (
	"github.com/blevesearch/bleve/registry"
	"github.com/blevesearch/bleve/search"
	"github.com/blevesearch/bleve/search/highlight"
)

const Name = "ansi"

const DefaultAnsiHighlight = bgYellow

type ANSIFragmentFormatter struct {
	color string
}

func NewANSIFragmentFormatter() *ANSIFragmentFormatter {
	return &ANSIFragmentFormatter{
		color: DefaultAnsiHighlight,
	}
}

func (a *ANSIFragmentFormatter) Format(f *highlight.Fragment, tlm search.TermLocationMap) string {
	orderedTermLocations := highlight.OrderTermLocations(tlm)
	rv := ""
	curr := f.Start
	for _, termLocation := range orderedTermLocations {
		if termLocation.Start < curr {
			continue
		}
		if termLocation.End > f.End {
			break
		}
		// add the stuff before this location
		rv += string(f.Orig[curr:termLocation.Start])
		// add the color
		rv += a.color
		// add the term itself
		rv += string(f.Orig[termLocation.Start:termLocation.End])
		// reset the color
		rv += reset
		// update current
		curr = termLocation.End
	}
	// add any remaining text after the last token
	rv += string(f.Orig[curr:f.End])

	return rv
}

// ANSI color control escape sequences.
// Shamelessly copied from https://github.com/sqp/godock/blob/master/libs/log/colors.go
const (
	reset      = "\x1b[0m"
	bright     = "\x1b[1m"
	dim        = "\x1b[2m"
	underscore = "\x1b[4m"
	blink      = "\x1b[5m"
	reverse    = "\x1b[7m"
	hidden     = "\x1b[8m"
	fgBlack    = "\x1b[30m"
	fgRed      = "\x1b[31m"
	fgGreen    = "\x1b[32m"
	fgYellow   = "\x1b[33m"
	fgBlue     = "\x1b[34m"
	fgMagenta  = "\x1b[35m"
	fgCyan     = "\x1b[36m"
	fgWhite    = "\x1b[37m"
	bgBlack    = "\x1b[40m"
	bgRed      = "\x1b[41m"
	bgGreen    = "\x1b[42m"
	bgYellow   = "\x1b[43m"
	bgBlue     = "\x1b[44m"
	bgMagenta  = "\x1b[45m"
	bgCyan     = "\x1b[46m"
	bgWhite    = "\x1b[47m"
)

func Constructor(config map[string]interface{}, cache *registry.Cache) (highlight.FragmentFormatter, error) {
	return NewANSIFragmentFormatter(), nil
}

func init() {
	registry.RegisterFragmentFormatter(Name, Constructor)
}
