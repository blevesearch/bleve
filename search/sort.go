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
	"encoding/json"
	"sort"
	"strings"

	"github.com/blevesearch/bleve/numeric_util"
)

var HighTerm = strings.Repeat(string([]byte{0xff}), 10)
var LowTerm = string([]byte{0x00})

type SearchSort interface {
	Compare(a, b *DocumentMatch) int

	RequiresDocID() bool
	RequiresScoring() bool
	RequiresFields() []string
}

func ParseSearchSort(input json.RawMessage) (SearchSort, error) {
	var tmp string
	err := json.Unmarshal(input, &tmp)
	if err != nil {
		return nil, err
	}
	descending := false
	if strings.HasPrefix(tmp, "-") {
		descending = true
		tmp = tmp[1:]
	}
	if tmp == "_id" {
		return &SortDocID{
			Descending: descending,
		}, nil
	} else if tmp == "_score" {
		return &SortScore{
			Descending: descending,
		}, nil
	}
	return &SortField{
		Field:      tmp,
		Descending: descending,
	}, nil
}

func ParseSortOrder(in []json.RawMessage) (SortOrder, error) {
	rv := make(SortOrder, 0, len(in))
	for _, i := range in {
		ss, err := ParseSearchSort(i)
		if err != nil {
			return nil, err
		}
		rv = append(rv, ss)
	}
	return rv, nil
}

type SortOrder []SearchSort

func (so SortOrder) Compare(i, j *DocumentMatch) int {
	// compare the documents on all search sorts until a differences is found
	for _, soi := range so {
		c := soi.Compare(i, j)
		if c == 0 {
			continue
		}
		return c
	}
	// if they are the same at this point, impose order based on index natural sort order
	if i.HitNumber == j.HitNumber {
		return 0
	} else if i.HitNumber > j.HitNumber {
		return 1
	}
	return -1
}

func (so SortOrder) RequiresScore() bool {
	rv := false
	for _, soi := range so {
		if soi.RequiresScoring() {
			rv = true
		}
	}
	return rv
}

func (so SortOrder) RequiresDocID() bool {
	rv := false
	for _, soi := range so {
		if soi.RequiresDocID() {
			rv = true
		}
	}
	return rv
}

func (so SortOrder) RequiredFields() []string {
	var rv []string
	for _, soi := range so {
		rv = append(rv, soi.RequiresFields()...)
	}
	return rv
}

// SortFieldType lets you control some internal sort behavior
// normally leaving this to the zero-value of SortFieldAuto is fine
type SortFieldType int

const (
	// SortFieldAuto applies heuristics attempt to automatically sort correctly
	SortFieldAuto SortFieldType = iota
	// SortFieldAsString forces sort as string (no prefix coded terms removed)
	SortFieldAsString
	// SortFieldAsNumber forces sort as string (prefix coded terms with shift > 0 removed)
	SortFieldAsNumber
	// SortFieldAsDate forces sort as string (prefix coded terms with shift > 0 removed)
	SortFieldAsDate
)

// SortFieldMode describes the behavior if the field has multiple values
type SortFieldMode int

const (
	// SortFieldFirst uses the first (or only) value, this is the default zero-value
	SortFieldFirst SortFieldMode = iota // FIXME name is confusing
	// SortFieldMin uses the minimum value
	SortFieldMin
	// SortFieldMax uses the maximum value
	SortFieldMax
)

const SortFieldMissingLast = "_last"
const SortFieldMissingFirst = "_first"

// SortField will sort results by the value of a stored field
//   Field is the name of the field
//   Descending reverse the sort order (default false)
//   Type allows forcing of string/number/date behavior (default auto)
//   Mode controls behavior for multi-values fields (default first)
//   Missing controls behavior of missing values (default last)
type SortField struct {
	Field      string
	Descending bool
	Type       SortFieldType
	Mode       SortFieldMode
	Missing    string
}

// Compare orders DocumentMatch instances by stored field values
func (s *SortField) Compare(i, j *DocumentMatch) int {
	iTerms := i.CachedFieldTerms[s.Field]
	iTerms = s.filterTermsByType(iTerms)
	iTerm := s.filterTermsByMode(iTerms)
	jTerms := j.CachedFieldTerms[s.Field]
	jTerms = s.filterTermsByType(jTerms)
	jTerm := s.filterTermsByMode(jTerms)
	rv := strings.Compare(iTerm, jTerm)
	if s.Descending {
		rv = -rv
	}
	return rv
}

func (s *SortField) filterTermsByMode(terms []string) string {
	if len(terms) == 1 || (len(terms) > 1 && s.Mode == SortFieldFirst) {
		return terms[0]
	} else if len(terms) > 1 {
		switch s.Mode {
		case SortFieldMin:
			sort.Strings(terms)
			return terms[0]
		case SortFieldMax:
			sort.Strings(terms)
			return terms[len(terms)-1]
		}
	}

	// handle missing terms
	if s.Missing == "" || s.Missing == SortFieldMissingLast {
		if s.Descending {
			return LowTerm
		}
		return HighTerm
	} else if s.Missing == SortFieldMissingFirst {
		if s.Descending {
			return HighTerm
		}
		return LowTerm
	}
	return s.Missing
}

// filterTermsByType attempts to make one pass on the terms
// if we are in auto-mode AND all the terms look like prefix-coded numbers
// return only the terms which had shift of 0
// if we are in explicit number or date mode, return only valid
// prefix coded numbers with shift of 0
func (s *SortField) filterTermsByType(terms []string) []string {
	stype := s.Type
	if stype == SortFieldAuto {
		allTermsPrefixCoded := true
		var termsWithShiftZero []string
		for _, term := range terms {
			valid, shift := numeric_util.ValidPrefixCodedTerm(term)
			if valid && shift == 0 {
				termsWithShiftZero = append(termsWithShiftZero, term)
			} else if !valid {
				allTermsPrefixCoded = false
			}
		}
		if allTermsPrefixCoded {
			terms = termsWithShiftZero
		}
	} else if stype == SortFieldAsNumber || stype == SortFieldAsDate {
		var termsWithShiftZero []string
		for _, term := range terms {
			valid, shift := numeric_util.ValidPrefixCodedTerm(term)
			if valid && shift == 0 {
				termsWithShiftZero = append(termsWithShiftZero)
			}
		}
		terms = termsWithShiftZero
	}
	return terms
}

// RequiresDocID says this SearchSort does not require the DocID be loaded
func (s *SortField) RequiresDocID() bool { return false }

// RequiresScoring says this SearchStore does not require scoring
func (s *SortField) RequiresScoring() bool { return false }

// RequiresFields says this SearchStore requires the specified stored field
func (s *SortField) RequiresFields() []string { return []string{s.Field} }

func (s *SortField) MarshalJSON() ([]byte, error) {
	if s.Descending {
		return json.Marshal("-" + s.Field)
	}
	return json.Marshal(s.Field)
}

// SortDocID will sort results by the document identifier
type SortDocID struct {
	Descending bool
}

// Compare orders DocumentMatch instances by document identifiers
func (s *SortDocID) Compare(i, j *DocumentMatch) int {
	if s.Descending {
		return strings.Compare(j.ID, i.ID)
	}
	return strings.Compare(i.ID, j.ID)
}

// RequiresDocID says this SearchSort does require the DocID be loaded
func (s *SortDocID) RequiresDocID() bool { return true }

// RequiresScoring says this SearchStore does not require scoring
func (s *SortDocID) RequiresScoring() bool { return false }

// RequiresFields says this SearchStore does not require any stored fields
func (s *SortDocID) RequiresFields() []string { return nil }

func (s *SortDocID) MarshalJSON() ([]byte, error) {
	if s.Descending {
		return json.Marshal("-_id")
	}
	return json.Marshal("_id")
}

// SortScore will sort results by the document match score
type SortScore struct {
	Descending bool
}

// Compare orders DocumentMatch instances by computed scores
func (s *SortScore) Compare(i, j *DocumentMatch) int {
	if i.Score == j.Score {
		return 0
	} else if (i.Score < j.Score && !s.Descending) || (j.Score < i.Score && s.Descending) {
		return -1
	}
	return 1
}

// RequiresDocID says this SearchSort does not require the DocID be loaded
func (s *SortScore) RequiresDocID() bool { return false }

// RequiresScoring says this SearchStore does require scoring
func (s *SortScore) RequiresScoring() bool { return true }

// RequiresFields says this SearchStore does not require any store fields
func (s *SortScore) RequiresFields() []string { return nil }

func (s *SortScore) MarshalJSON() ([]byte, error) {
	if s.Descending {
		return json.Marshal("-_score")
	}
	return json.Marshal("_score")
}
