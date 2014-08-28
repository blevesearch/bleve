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
	"container/heap"

	"github.com/blevesearch/bleve/document"
)

const DEFAULT_SEPARATOR = "…"

type SimpleHighlighter struct {
	fragmenter Fragmenter
	formatter  FragmentFormatter
	sep        string
}

func NewSimpleHighlighter() *SimpleHighlighter {
	return &SimpleHighlighter{
		fragmenter: NewSimpleFragmenter(),
		formatter:  NewANSIFragmentFormatter(),
		sep:        DEFAULT_SEPARATOR,
	}
}

func (s *SimpleHighlighter) Fragmenter() Fragmenter {
	return s.fragmenter
}

func (s *SimpleHighlighter) SetFragmenter(f Fragmenter) {
	s.fragmenter = f
}

func (s *SimpleHighlighter) FragmentFormatter() FragmentFormatter {
	return s.formatter
}

func (s *SimpleHighlighter) SetFragmentFormatter(f FragmentFormatter) {
	s.formatter = f
}

func (s *SimpleHighlighter) Separator() string {
	return s.sep
}

func (s *SimpleHighlighter) SetSeparator(sep string) {
	s.sep = sep
}

func (s *SimpleHighlighter) BestFragmentInField(dm *DocumentMatch, doc *document.Document, field string) string {
	fragments := s.BestFragmentsInField(dm, doc, field, 1)
	if len(fragments) > 0 {
		return fragments[0]
	}
	return ""
}

func (s *SimpleHighlighter) BestFragmentsInField(dm *DocumentMatch, doc *document.Document, field string, num int) []string {
	tlm := dm.Locations[field]
	orderedTermLocations := OrderTermLocations(tlm)
	scorer := NewSimpleFragmentScorer(dm.Locations[field])

	// score the fragments and put them into a priority queue ordered by score
	fq := make(FragmentQueue, 0)
	heap.Init(&fq)
	for _, f := range doc.Fields {
		if f.Name() == field {
			_, ok := f.(*document.TextField)
			if ok {
				fieldData := f.Value()
				fragments := s.fragmenter.Fragment(fieldData, orderedTermLocations)
				for _, fragment := range fragments {
					scorer.Score(fragment)
					heap.Push(&fq, fragment)
				}
			}
		}
	}

	// now find the N best non-overlapping fragments
	bestFragments := make([]*Fragment, 0)
	if len(fq) > 0 {
		candidate := heap.Pop(&fq)
	OUTER:
		for candidate != nil && len(bestFragments) < num {
			// see if this overlaps with any of the best already identified
			if len(bestFragments) > 0 {
				for _, frag := range bestFragments {
					if candidate.(*Fragment).Overlaps(frag) {
						if len(fq) < 1 {
							break OUTER
						}
						candidate = heap.Pop(&fq)
						continue OUTER
					}
				}
				bestFragments = append(bestFragments, candidate.(*Fragment))
			} else {
				bestFragments = append(bestFragments, candidate.(*Fragment))
			}

			if len(fq) < 1 {
				break
			}
			candidate = heap.Pop(&fq)
		}
	}

	// now that we have the best fragments, we can format them
	formattedFragments := make([]string, len(bestFragments))
	for i, fragment := range bestFragments {
		formattedFragments[i] = ""
		if fragment.start != 0 {
			formattedFragments[i] += s.sep
		}
		formattedFragments[i] += s.formatter.Format(fragment, dm.Locations[field])
		if fragment.end != len(fragment.orig) {
			formattedFragments[i] += s.sep
		}
	}

	if dm.Fragments == nil {
		dm.Fragments = make(FieldFragmentMap, 0)
	}
	dm.Fragments[field] = formattedFragments

	return formattedFragments
}

// A PriorityQueue implements heap.Interface and holds Items.
type FragmentQueue []*Fragment

func (fq FragmentQueue) Len() int { return len(fq) }

func (fq FragmentQueue) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return fq[i].score > fq[j].score
}

func (fq FragmentQueue) Swap(i, j int) {
	fq[i], fq[j] = fq[j], fq[i]
	fq[i].index = i
	fq[j].index = j
}

func (fq *FragmentQueue) Push(x interface{}) {
	n := len(*fq)
	item := x.(*Fragment)
	item.index = n
	*fq = append(*fq, item)
}

func (fq *FragmentQueue) Pop() interface{} {
	old := *fq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*fq = old[0 : n-1]
	return item
}
