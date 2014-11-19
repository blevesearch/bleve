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

type numericRange struct {
	Name string   `json:"name,omitempty"`
	Min  *float64 `json:"min,omitempty"`
	Max  *float64 `json:"max,omitempty"`
}

type dateTimeRange struct {
	Name        string    `json:"name,omitempty"`
	Start       time.Time `json:"start,omitempty"`
	End         time.Time `json:"end,omitempty"`
	startString *string
	endString   *string
}

func (dr *dateTimeRange) ParseDates(dateTimeParser analysis.DateTimeParser) {
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

func (dr *dateTimeRange) UnmarshalJSON(input []byte) error {
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

// A FacetRequest describes an facet or aggregation
// of the result document set you would like to be
// built.
type FacetRequest struct {
	Size           int
	Field          string
	NumericRanges  []*numericRange  `json:"numeric_ranges,omitempty"`
	DateTimeRanges []*dateTimeRange `json:"date_ranges,omitempty"`
}

// NewFacetRequest creates a facet on the specified
// field that limits the number of entries to the
// specified size.
func NewFacetRequest(field string, size int) *FacetRequest {
	return &FacetRequest{
		Field: field,
		Size:  size,
	}
}

// AddDateTimeRange adds a bucket to a field
// containing date values.  Documents with a
// date value falling into this range are tabulated
// as part of this bucket/range.
func (fr *FacetRequest) AddDateTimeRange(name string, start, end time.Time) {
	if fr.DateTimeRanges == nil {
		fr.DateTimeRanges = make([]*dateTimeRange, 0, 1)
	}
	fr.DateTimeRanges = append(fr.DateTimeRanges, &dateTimeRange{Name: name, Start: start, End: end})
}

// AddNumericRange adds a bucket to a field
// containing numeric values.  Documents with a
// numeric value falling into this range are
// tabulated as part of this bucket/range.
func (fr *FacetRequest) AddNumericRange(name string, min, max *float64) {
	if fr.NumericRanges == nil {
		fr.NumericRanges = make([]*numericRange, 0, 1)
	}
	fr.NumericRanges = append(fr.NumericRanges, &numericRange{Name: name, Min: min, Max: max})
}

// FacetsRequest groups together all the
// FacetRequest objects for a single query.
type FacetsRequest map[string]*FacetRequest

// HighlightRequest describes how field matches
// should be highlighted.
type HighlightRequest struct {
	Style  *string  `json:"style"`
	Fields []string `json:"fields"`
}

// NewHighlight creates a default
// HighlightRequest.
func NewHighlight() *HighlightRequest {
	return &HighlightRequest{}
}

// NewHighlightWithStyle creates a HighlightRequest
// with an alternate style.
func NewHighlightWithStyle(style string) *HighlightRequest {
	return &HighlightRequest{
		Style: &style,
	}
}

func (h *HighlightRequest) AddField(field string) {
	if h.Fields == nil {
		h.Fields = make([]string, 0, 1)
	}
	h.Fields = append(h.Fields, field)
}

// A SearchRequest describes all the parameters
// needed to search the index.
// Query is required.
// Size/From describe how much and which part of the
// result set to return.
// Highlight describes optional search result
// highlighting.
// Fields desribed a list of field values whcih
// should be retrieved for result documents.
// Facets describe the set of facets to be computed.
// Explain triggers inclusion of additional search
// result score explanations.
type SearchRequest struct {
	Query     Query             `json:"query"`
	Size      int               `json:"size"`
	From      int               `json:"from"`
	Highlight *HighlightRequest `json:"highlight"`
	Fields    []string          `json:"fields"`
	Facets    FacetsRequest     `json:"facets"`
	Explain   bool              `json:"explain"`
}

// AddFacet adds a FacetRequest to this SearchRequest
func (r *SearchRequest) AddFacet(facetName string, f *FacetRequest) {
	if r.Facets == nil {
		r.Facets = make(FacetsRequest, 1)
	}
	r.Facets[facetName] = f
}

// UnmarshalJSON deserializes a JSON representation of
// a SearchRequest
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

	if r.Size < 0 {
		r.Size = 10
	}
	if r.From < 0 {
		r.From = 0
	}

	return nil

}

// NewSearchRequest creates a new SearchRequest
// for the Query, using default values for all
// other search parameters.
func NewSearchRequest(q Query) *SearchRequest {
	return NewSearchRequestOptions(q, 10, 0, false)
}

// NewSearchRequestOptions creates a new SearchRequest
// for the Query, with the requested size, from
// and explanation search parameters.
func NewSearchRequestOptions(q Query, size, from int, explain bool) *SearchRequest {
	return &SearchRequest{
		Query:   q,
		Size:    size,
		From:    from,
		Explain: explain,
	}
}

// A SearchResult describes the results of executing
// a SearchRequest.
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
	if sr.Total > 0 {
		if sr.Request.Size > 0 {
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
			rv = fmt.Sprintf("%d matches, took %s\n", sr.Total, sr.Took)
		}
	} else {
		rv = "No matches"
	}
	return rv
}

func (sr *SearchResult) Merge(other *SearchResult) {
	sr.Hits = append(sr.Hits, other.Hits...)
	sr.Total += other.Total
	sr.Took += other.Took
	if other.MaxScore > sr.MaxScore {
		sr.MaxScore = other.MaxScore
	}
	sr.Facets.Merge(other.Facets)
}
