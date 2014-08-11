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
	"github.com/couchbaselabs/bleve/index"
)

type FacetBuilder interface {
	Update(index.FieldTerms)
	Result() FacetResult
}

type FacetsBuilder struct {
	index  index.Index
	facets map[string]FacetBuilder
}

func NewFacetsBuilder(index index.Index) *FacetsBuilder {
	return &FacetsBuilder{
		index:  index,
		facets: make(map[string]FacetBuilder, 0),
	}
}

func (fb *FacetsBuilder) Add(name string, facetBuilder FacetBuilder) {
	fb.facets[name] = facetBuilder
}

func (fb *FacetsBuilder) Update(docMatch *DocumentMatch) error {
	fieldTerms, err := fb.index.DocumentFieldTerms(docMatch.ID)
	if err != nil {
		return err
	}
	for _, facetBuilder := range fb.facets {
		facetBuilder.Update(fieldTerms)
	}
	return nil
}

type TermFacet struct {
	Term  string `json:"term"`
	Count int    `json:"count"`
}

type NumericRangeFacet struct {
	Name  string   `json:"name"`
	Min   *float64 `json:"min,omitempty"`
	Max   *float64 `json:"max,omitempty"`
	Count int      `json:"count"`
}

type DateRangeFacet struct {
	Name  string  `json:"name"`
	Start *string `json:"start,omitempty"`
	End   *string `json:"end,omitempty"`
	Count int     `json:"count"`
}

type FacetResult struct {
	Field         string               `json:"field"`
	Total         int                  `json:"total"`
	Missing       int                  `json:"missing"`
	Other         int                  `json:"other"`
	Terms         []*TermFacet         `json:"terms,omitempty"`
	NumericRanges []*NumericRangeFacet `json:"numeric_ranges,omitempty"`
	DateRanges    []*DateRangeFacet    `json:"date_ranges,omitempty"`
}

type FacetResults map[string]FacetResult

func (fb *FacetsBuilder) Results() FacetResults {
	fr := make(FacetResults)
	for facetName, facetBuilder := range fb.facets {
		facetResult := facetBuilder.Result()
		fr[facetName] = facetResult
	}
	return fr
}
