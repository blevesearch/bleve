//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package html_char_filter

import (
	"regexp"

	"github.com/couchbaselabs/bleve/analysis/char_filters/regexp_char_filter"
)

// the origin of this regex is here:
// http://haacked.com/archive/2004/10/25/usingregularexpressionstomatchhtml.aspx/
// slightly modified by me to also match the DOCTYPE
const htmlTagPattern = `</?[!\w]+((\s+\w+(\s*=\s*(?:".*?"|'.*?'|[^'">\s]+))?)+\s*|\s*)/?>`

var htmlRegex = regexp.MustCompile(htmlTagPattern)

type HtmlCharFilter struct {
	*regexp_char_filter.RegexpCharFilter
}

func NewHtmlCharFilter() *HtmlCharFilter {
	return &HtmlCharFilter{
		regexp_char_filter.NewRegexpCharFilter(htmlRegex, []byte{' '}),
	}
}
