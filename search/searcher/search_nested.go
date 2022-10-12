//  Copyright (c) 2019 Couchbase, Inc.
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

package searcher

import (
	"github.com/blevesearch/bleve/index"
	"github.com/blevesearch/bleve/search"
)

type depthFilterContext struct {
	termAPMap  map[string][]search.ArrayPositions
	apLists    [][]search.ArrayPositions
	arrayDepth int
}

func NewNestedArraySearcher(indexReader index.IndexReader, depth int,
	qsearchers []search.Searcher, options search.SearcherOptions) (
	search.Searcher, error) {
	conjunctionSearcher, err := NewConjunctionSearcher(indexReader, qsearchers,
		options)
	if err != nil {
		return nil, err
	}

	rv := depthFilterContext{arrayDepth: depth}

	return NewFilteringSearcher(conjunctionSearcher,
		rv.buildArrayDepthFilter()), nil
}

// buildArrayDepthFilter returns a filter that checks whether
// all the search terms are sharing any common array position in
// the document match.
func (c *depthFilterContext) buildArrayDepthFilter() FilterFunc {
	return c.checkArrayDepth
}

func (c *depthFilterContext) checkArrayDepth(d *search.DocumentMatch) bool {
	if d == nil || len(d.FieldTermLocations) == 0 {
		return false
	}

	// reset and resuse
	if len(c.termAPMap) == 0 {
		c.termAPMap = make(map[string][]search.ArrayPositions, len(d.FieldTermLocations))
	} else {
		for k := range c.termAPMap {
			list := c.termAPMap[k]
			c.termAPMap[k] = list[:0]
		}
	}

	termList := make(map[string]struct{}, 1)
	for _, ftl := range d.FieldTermLocations {
		if len(ftl.Location.ArrayPositions) == c.arrayDepth {
			c.termAPMap[ftl.Term] = append(c.termAPMap[ftl.Term],
				ftl.Location.ArrayPositions)
		}
		termList[ftl.Term] = struct{}{}
	}

	// exit early if any of the search terms are not found
	// at the given array depth.
	for term := range termList {
		if _, ok := c.termAPMap[term]; !ok {
			return false
		}
	}

	if cap(c.apLists) < len(c.termAPMap) {
		c.apLists = make([][]search.ArrayPositions, len(c.termAPMap))
	} else {
		for i := range c.apLists {
			c.apLists[i] = c.apLists[i][:0]
		}
	}

	var index int
	for _, ap := range c.termAPMap {
		c.apLists[index] = ap
		index++
	}

	return checkForIntersection(c.apLists)
}

// checkForIntersection check the existence of common
// elements in a list of array position lists.
func checkForIntersection(apLists [][]search.ArrayPositions) bool {
	if len(apLists) == 1 {
		return true
	}

	if len(apLists) == 2 && len(apLists[0]) == 1 && len(apLists[1]) == 1 {
		if equalAPSlices(apLists[0][0], apLists[1][0]) == 0 {
			return true
		}
	}

	var baseIndex int
	indices := make([]int, len(apLists))
	var smallestListOver bool

	for baseIndex < len(apLists[0]) && !smallestListOver {
		var totalMatchFound int
		for i := 1; i < len(apLists); i++ {

			curIndex := indices[i-1]
			for curIndex < len(apLists[i]) &&
				equalAPSlices(apLists[i][curIndex], apLists[0][baseIndex]) == -1 {
				curIndex++
			}
			if curIndex < len(apLists[i]) {
				if equalAPSlices(apLists[i][curIndex], apLists[0][baseIndex]) == 0 {
					totalMatchFound++
				}
			} else {
				smallestListOver = true
			}

			indices[i-1] = curIndex
		}

		if totalMatchFound == len(apLists)-1 {
			return true
		}
		baseIndex++
	}

	return false
}

func equalAPSlices(a, b search.ArrayPositions) int {
	for i := range a {
		if a[i] < b[i] {
			return -1
		} else if a[i] > b[i] {
			return 1
		}
	}
	return 0
}
