//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.
package search

type Location struct {
	Pos   float64 `json:"pos"`
	Start float64 `json:"start"`
	End   float64 `json:"end"`
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
	ID        string               `json:"id"`
	Score     float64              `json:"score"`
	Expl      *Explanation         `json:"explanation,omitempty"`
	Locations FieldTermLocationMap `json:"locations,omitempty"`
	Fragments FieldFragmentMap     `json:"fragments,omitempty"`
}

type DocumentMatchCollection []*DocumentMatch

type Searcher interface {
	Next() (*DocumentMatch, error)
	Advance(ID string) (*DocumentMatch, error)
	Close()
	Weight() float64
	SetQueryNorm(float64)
	Count() uint64
}
