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
	"fmt"

	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
)

type Location struct {
	Pos            float64   `json:"pos"`
	Start          float64   `json:"start"`
	End            float64   `json:"end"`
	ArrayPositions []float64 `json:"array_positions"`
}

// SameArrayElement returns true if two locations are point to
// the same array element
func (l *Location) SameArrayElement(other *Location) bool {
	if len(l.ArrayPositions) != len(other.ArrayPositions) {
		return false
	}
	for i, elem := range l.ArrayPositions {
		if other.ArrayPositions[i] != elem {
			return false
		}
	}
	return true
}

type Locations []*Location

type TermLocationMap map[string]Locations

func (t TermLocationMap) AddLocation(term string, location *Location) {
	existingLocations, exists := t[term]
	if exists {
		existingLocations = append(existingLocations, location)
		t[term] = existingLocations
	} else {
		locations := make(Locations, 1)
		locations[0] = location
		t[term] = locations
	}
}

type FieldTermLocationMap map[string]TermLocationMap

type FieldFragmentMap map[string][]string

type DocumentMatch struct {
	Index           string                `json:"index,omitempty"`
	ID              string                `json:"id"`
	IndexInternalID index.IndexInternalID `json:"-"`
	Score           float64               `json:"score"`
	Expl            *Explanation          `json:"explanation,omitempty"`
	Locations       FieldTermLocationMap  `json:"locations,omitempty"`
	Fragments       FieldFragmentMap      `json:"fragments,omitempty"`
	Sort            []string              `json:"sort,omitempty"`

	// Fields contains the values for document fields listed in
	// SearchRequest.Fields. Text fields are returned as strings, numeric
	// fields as float64s and date fields as time.RFC3339 formatted strings.
	Fields map[string]interface{} `json:"fields,omitempty"`

	// as we learn field terms, we can cache important ones for later use
	// for example, sorting and building facets need these values
	CachedFieldTerms index.FieldTerms `json:"-"`

	// if we load the document for this hit, remember it so we dont load again
	Document *document.Document `json:"-"`

	// used to maintain natural index order
	HitNumber uint64 `json:"-"`
}

func (dm *DocumentMatch) AddFieldValue(name string, value interface{}) {
	if dm.Fields == nil {
		dm.Fields = make(map[string]interface{})
	}
	existingVal, ok := dm.Fields[name]
	if !ok {
		dm.Fields[name] = value
		return
	}

	valSlice, ok := existingVal.([]interface{})
	if ok {
		// already a slice, append to it
		valSlice = append(valSlice, value)
	} else {
		// create a slice
		valSlice = []interface{}{existingVal, value}
	}
	dm.Fields[name] = valSlice
}

// Reset allows an already allocated DocumentMatch to be reused
func (dm *DocumentMatch) Reset() *DocumentMatch {
	// remember the []byte used for the IndexInternalID
	indexInternalID := dm.IndexInternalID
	// remember the []interface{} used for sort
	sort := dm.Sort
	// idiom to copy over from empty DocumentMatch (0 allocations)
	*dm = DocumentMatch{}
	// reuse the []byte already allocated (and reset len to 0)
	dm.IndexInternalID = indexInternalID[:0]
	// reuse the []interface{} already allocated (and reset len to 0)
	dm.Sort = sort[:0]
	return dm
}

func (dm *DocumentMatch) String() string {
	return fmt.Sprintf("[%s-%f]", string(dm.IndexInternalID), dm.Score)
}

type DocumentMatchCollection []*DocumentMatch

func (c DocumentMatchCollection) Len() int           { return len(c) }
func (c DocumentMatchCollection) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }
func (c DocumentMatchCollection) Less(i, j int) bool { return c[i].Score > c[j].Score }

type Searcher interface {
	Next(ctx *SearchContext) (*DocumentMatch, error)
	Advance(ctx *SearchContext, ID index.IndexInternalID) (*DocumentMatch, error)
	Close() error
	Weight() float64
	SetQueryNorm(float64)
	Count() uint64
	Min() int

	DocumentMatchPoolSize() int
}

// SearchContext represents the context around a single search
type SearchContext struct {
	DocumentMatchPool *DocumentMatchPool

	// A LowScoreFilter is an optional score provided by the
	// collector, allowing searchers to potentially optimize by
	// performing early filtering of doc matches with low score.
	LowScoreFilter float64

	// A count of the matches which were filtered out due to a low
	// score w.r.t. the LowScoreFilter.  These need to be counted
	// into the search result's total hits.
	LowScoreNumMatches uint64
}
