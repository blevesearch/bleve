//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package bleve

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/search"
)

type NumericRange struct {
	Name string   `json:"name,omitempty"`
	Min  *float64 `json:"min,omitempty"`
	Max  *float64 `json:"max,omitempty"`
}

type DateTimeRange struct {
	Name        string    `json:"name,omitempty"`
	Start       time.Time `json:"start,omitempty"`
	End         time.Time `json:"end,omitempty"`
	startString *string
	endString   *string
}

func (dr *DateTimeRange) ParseDates(dateTimeParser analysis.DateTimeParser) {
	if dr.Start.IsZero() && dr.startString != nil {
		start, err := dateTimeParser.ParseDateTime(*dr.startString)
		if err == nil {
			dr.Start = start
		}
	}
	if dr.End.IsZero() && dr.endString != nil {
		end, err := dateTimeParser.ParseDateTime(*dr.endString)
		if err == nil {
			dr.End = end
		}
	}
}

func (dr *DateTimeRange) UnmarshalJSON(input []byte) error {
	var temp struct {
		Name  string  `json:"name,omitempty"`
		Start *string `json:"start,omitempty"`
		End   *string `json:"end,omitempty"`
	}

	err := json.Unmarshal(input, &temp)
	if err != nil {
		return err
	}

	dr.Name = temp.Name
	if temp.Start != nil {
		dr.startString = temp.Start
	}
	if temp.End != nil {
		dr.endString = temp.End
	}

	return nil
}

type FacetRequest struct {
	Size           int
	Field          string
	NumericRanges  []*NumericRange  `json:"numeric_ranges,omitempty"`
	DateTimeRanges []*DateTimeRange `json:"date_ranges,omitempty"`
}

func NewFacetRequest(field string, size int) *FacetRequest {
	return &FacetRequest{
		Field: field,
		Size:  size,
	}
}

func (fr *FacetRequest) AddDateTimeRange(name string, start, end time.Time) {
	if fr.DateTimeRanges == nil {
		fr.DateTimeRanges = make([]*DateTimeRange, 0, 1)
	}
	fr.DateTimeRanges = append(fr.DateTimeRanges, &DateTimeRange{Name: name, Start: start, End: end})
}

func (fr *FacetRequest) AddNumericRange(name string, min, max *float64) {
	if fr.NumericRanges == nil {
		fr.NumericRanges = make([]*NumericRange, 0, 1)
	}
	fr.NumericRanges = append(fr.NumericRanges, &NumericRange{Name: name, Min: min, Max: max})
}

type FacetsRequest map[string]*FacetRequest

type HighlightRequest struct {
	Style  *string  `json:"style"`
	Fields []string `json:"fields"`
}

func NewHighlight() *HighlightRequest {
	return &HighlightRequest{}
}

func NewHighlightWithStyle(style string) *HighlightRequest {
	return &HighlightRequest{
		Style: &style,
	}
}

type SearchRequest struct {
	Query     Query             `json:"query"`
	Size      int               `json:"size"`
	From      int               `json:"from"`
	Highlight *HighlightRequest `json:"highlight"`
	Fields    []string          `json:"fields"`
	Facets    FacetsRequest     `json:"facets"`
	Explain   bool              `json:"explain"`
}

func (r *SearchRequest) AddFacet(facetName string, f *FacetRequest) {
	if r.Facets == nil {
		r.Facets = make(FacetsRequest, 1)
	}
	r.Facets[facetName] = f
}

func (r *SearchRequest) UnmarshalJSON(input []byte) error {
	var temp struct {
		Q         json.RawMessage   `json:"query"`
		Size      int               `json:"size"`
		From      int               `json:"from"`
		Highlight *HighlightRequest `json:"highlight"`
		Fields    []string          `json:"fields"`
		Facets    FacetsRequest     `json:"facets"`
		Explain   bool              `json:"explain"`
	}

	err := json.Unmarshal(input, &temp)
	if err != nil {
		return err
	}

	r.Size = temp.Size
	r.From = temp.From
	r.Explain = temp.Explain
	r.Highlight = temp.Highlight
	r.Fields = temp.Fields
	r.Facets = temp.Facets
	r.Query, err = ParseQuery(temp.Q)
	if err != nil {
		return err
	}

	if r.Size <= 0 {
		r.Size = 10
	}
	if r.From <= 0 {
		r.From = 0
	}

	return nil

}

func NewSearchRequest(q Query) *SearchRequest {
	return NewSearchRequestOptions(q, 10, 0, false)
}

func NewSearchRequestOptions(q Query, size, from int, explain bool) *SearchRequest {
	return &SearchRequest{
		Query:   q,
		Size:    size,
		From:    from,
		Explain: explain,
	}
}

type SearchResult struct {
	Request  *SearchRequest                 `json:"request"`
	Hits     search.DocumentMatchCollection `json:"hits"`
	Total    uint64                         `json:"total_hits"`
	MaxScore float64                        `json:"max_score"`
	Took     time.Duration                  `json:"took"`
	Facets   search.FacetResults            `json:"facets"`
}

func (sr *SearchResult) String() string {
	rv := ""
	if len(sr.Hits) > 0 {
		rv = fmt.Sprintf("%d matches, showing %d through %d, took %s\n", sr.Total, sr.Request.From+1, sr.Request.From+len(sr.Hits), sr.Took)
		for i, hit := range sr.Hits {
			rv += fmt.Sprintf("%5d. %s (%f)\n", i+sr.Request.From+1, hit.ID, hit.Score)
			for fragmentField, fragments := range hit.Fragments {
				rv += fmt.Sprintf("\t%s\n", fragmentField)
				for _, fragment := range fragments {
					rv += fmt.Sprintf("\t\t%s\n", fragment)
				}
			}
		}
	} else {
		rv = "No matches"
	}
	return rv
}
