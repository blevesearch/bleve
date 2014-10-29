//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package facets

import (
	"container/list"

	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
)

type TermsFacetBuilder struct {
	size       int
	field      string
	termsCount map[string]int
	total      int
	missing    int
}

func NewTermsFacetBuilder(field string, size int) *TermsFacetBuilder {
	return &TermsFacetBuilder{
		size:       size,
		field:      field,
		termsCount: make(map[string]int),
	}
}

func (fb *TermsFacetBuilder) Update(ft index.FieldTerms) {
	terms, ok := ft[fb.field]
	if ok {
		for _, term := range terms {
			existingCount, existed := fb.termsCount[term]
			if existed {
				fb.termsCount[term] = existingCount + 1
			} else {
				fb.termsCount[term] = 1
			}
			fb.total++
		}
	} else {
		fb.missing++
	}
}

func (fb *TermsFacetBuilder) Result() *search.FacetResult {
	rv := search.FacetResult{
		Field:   fb.field,
		Total:   fb.total,
		Missing: fb.missing,
	}

	// FIXME better implementation needed here this is quick and dirty
	topN := list.New()

	// walk entries and find top N
OUTER:
	for term, count := range fb.termsCount {
		tf := &search.TermFacet{
			Term:  term,
			Count: count,
		}

		for e := topN.Front(); e != nil; e = e.Next() {
			curr := e.Value.(*search.TermFacet)
			if tf.Count < curr.Count {

				topN.InsertBefore(tf, e)
				// if we just made the list too long
				if topN.Len() > fb.size {
					// remove the head
					topN.Remove(topN.Front())
				}
				continue OUTER
			}
		}
		// if we got to the end, we still have to add it
		topN.PushBack(tf)
		if topN.Len() > fb.size {
			// remove the head
			topN.Remove(topN.Front())
		}

	}

	// we now have the list of the top N facets
	rv.Terms = make([]*search.TermFacet, topN.Len())
	i := 0
	notOther := 0
	for e := topN.Back(); e != nil; e = e.Prev() {
		rv.Terms[i] = e.Value.(*search.TermFacet)
		i++
		notOther += e.Value.(*search.TermFacet).Count
	}
	rv.Other = fb.total - notOther

	return &rv
}
