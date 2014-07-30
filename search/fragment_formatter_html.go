//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import ()

const DEFAULT_HTML_HIGHLIGHT_BEFORE = "<b>"
const DEFAULT_HTML_HIGHLIGHT_AFTER = "</b>"

type HTMLFragmentFormatter struct {
	before string
	after  string
}

func NewHTMLFragmentFormatter() *HTMLFragmentFormatter {
	return &HTMLFragmentFormatter{
		before: DEFAULT_HTML_HIGHLIGHT_BEFORE,
		after:  DEFAULT_HTML_HIGHLIGHT_AFTER,
	}
}

func NewHTMLFragmentFormatterCustom(before, after string) *HTMLFragmentFormatter {
	return &HTMLFragmentFormatter{
		before: before,
		after:  after,
	}
}

func (a *HTMLFragmentFormatter) Format(f *Fragment, tlm TermLocationMap) string {
	orderedTermLocations := OrderTermLocations(tlm)
	rv := ""
	curr := f.start
	for _, termLocation := range orderedTermLocations {
		if termLocation.Start < curr {
			continue
		}
		if termLocation.End > f.end {
			break
		}
		// add the stuff before this location
		rv += string(f.orig[curr:termLocation.Start])
		// add the color
		rv += a.before
		// add the term itself
		rv += string(f.orig[termLocation.Start:termLocation.End])
		// reset the color
		rv += a.after
		// update current
		curr = termLocation.End
	}
	// add any remaining text after the last token
	rv += string(f.orig[curr:f.end])

	return rv
}
