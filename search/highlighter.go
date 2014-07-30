//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

import (
	"github.com/couchbaselabs/bleve/document"
)

type Fragment struct {
	orig  []byte
	start int
	end   int
	score float64
	index int // used by heap
}

func (f *Fragment) Overlaps(other *Fragment) bool {
	if other.start >= f.start && other.start < f.end {
		return true
	} else if f.start >= other.start && f.start < other.end {
		return true
	}
	return false
}

type Fragmenter interface {
	Fragment([]byte, termLocations) []*Fragment
}

type FragmentFormatter interface {
	Format(f *Fragment, tlm TermLocationMap) string
}

type FragmentScorer interface {
	Score(f *Fragment) float64
}

type Highlighter interface {
	Fragmenter() Fragmenter
	SetFragmenter(Fragmenter)

	FragmentFormatter() FragmentFormatter
	SetFragmentFormatter(FragmentFormatter)

	Separator() string
	SetSeparator(string)

	BestFragmentInField(*DocumentMatch, *document.Document, string) string
	BestFragmentsInField(*DocumentMatch, *document.Document, string, int) []string
}
