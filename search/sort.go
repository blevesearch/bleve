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
	"fmt"
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

func ParseSearchSortObj(input map[string]interface{}) (SearchSort, error) {
	descending, ok := input["desc"].(bool)
	by, ok := input["by"].(string)
	if !ok {
		return nil, fmt.Errorf("search sort must specify by")
	}
	switch by {
	case "id":
		return &SortDocID{
			Descending: descending,
		}, nil
	case "score":
		return &SortScore{
			Descending: descending,
		}, nil
	case "field":
		field, ok := input["field"].(string)
		if !ok {
			return nil, fmt.Errorf("search sort mode field must specify field")
		}
		rv := &SortField{
			Field:      field,
			Descending: descending,
		}
		typ, ok := input["type"].(string)
		if ok {
			switch typ {
			case "auto":
				rv.Type = SortFieldAuto
			case "string":
				rv.Type = SortFieldAsString
			case "number":
				rv.Type = SortFieldAsNumber
			case "date":
				rv.Type = SortFieldAsDate
			default:
				return nil, fmt.Errorf("unkown sort field type: %s", typ)
			}
		}
		mode, ok := input["mode"].(string)
		if ok {
			switch mode {
			case "default":
				rv.Mode = SortFieldDefault
			case "min":
				rv.Mode = SortFieldMin
			case "max":
				rv.Mode = SortFieldMax
			default:
				return nil, fmt.Errorf("unknown sort field mode: %s", mode)
			}
		}
		missing, ok := input["missing"].(string)
		if ok {
			switch missing {
			case "first":
				rv.Missing = SortFieldMissingFirst
			case "last":
				rv.Missing = SortFieldMissingLast
			default:
				return nil, fmt.Errorf("unknown sort field missing: %s", missing)
			}
		}
		return rv, nil
	}

	return nil, fmt.Errorf("unknown search sort by: %s", by)
}

func ParseSearchSortString(input string) SearchSort {
	descending := false
	if strings.HasPrefix(input, "-") {
		descending = true
		input = input[1:]
	} else if strings.HasPrefix(input, "+") {
		input = input[1:]
	}
	if input == "_id" {
		return &SortDocID{
			Descending: descending,
		}
	} else if input == "_score" {
		return &SortScore{
			Descending: descending,
		}
	}
	return &SortField{
		Field:      input,
		Descending: descending,
	}
}

func ParseSearchSortJSON(input json.RawMessage) (SearchSort, error) {
	// first try to parse it as string
	var sortString string
	err := json.Unmarshal(input, &sortString)
	if err != nil {
		var sortObj map[string]interface{}
		err = json.Unmarshal(input, &sortObj)
		if err != nil {
			return nil, err
		}
		return ParseSearchSortObj(sortObj)
	}
	return ParseSearchSortString(sortString), nil
}

func ParseSortOrderStrings(in []string) SortOrder {
	rv := make(SortOrder, 0, len(in))
	for _, i := range in {
		ss := ParseSearchSortString(i)
		rv = append(rv, ss)
	}
	return rv
}

func ParseSortOrderJSON(in []json.RawMessage) (SortOrder, error) {
	rv := make(SortOrder, 0, len(in))
	for _, i := range in {
		ss, err := ParseSearchSortJSON(i)
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
	// SortFieldDefault uses the first (or only) value, this is the default zero-value
	SortFieldDefault SortFieldMode = iota // FIXME name is confusing
	// SortFieldMin uses the minimum value
	SortFieldMin
	// SortFieldMax uses the maximum value
	SortFieldMax
)

// SortFieldMissing controls where documents missing a field value should be sorted
type SortFieldMissing int

const (
	// SortFieldMissingLast sorts documents missing a field at the end
	SortFieldMissingLast SortFieldMissing = iota

	// SortFieldMissingFirst sorts documents missing a field at the beginning
	SortFieldMissingFirst
)

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
	Missing    SortFieldMissing
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
	if len(terms) == 1 || (len(terms) > 1 && s.Mode == SortFieldDefault) {
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
	if s.Missing == SortFieldMissingLast {
		if s.Descending {
			return LowTerm
		}
		return HighTerm
	}
	if s.Descending {
		return HighTerm
	}
	return LowTerm
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
	// see if simple format can be used
	if s.Missing == SortFieldMissingLast &&
		s.Mode == SortFieldDefault &&
		s.Type == SortFieldAuto {
		if s.Descending {
			return json.Marshal("-" + s.Field)
		}
		return json.Marshal(s.Field)
	}
	sfm := map[string]interface{}{
		"by":    "field",
		"field": s.Field,
	}
	if s.Descending {
		sfm["desc"] = true
	}
	if s.Missing > SortFieldMissingLast {
		switch s.Missing {
		case SortFieldMissingFirst:
			sfm["missing"] = "first"
		}
	}
	if s.Mode > SortFieldDefault {
		switch s.Mode {
		case SortFieldMin:
			sfm["mode"] = "min"
		case SortFieldMax:
			sfm["mode"] = "max"
		}
	}
	if s.Type > SortFieldAuto {
		switch s.Type {
		case SortFieldAsString:
			sfm["type"] = "string"
		case SortFieldAsNumber:
			sfm["type"] = "number"
		case SortFieldAsDate:
			sfm["type"] = "date"
		}
	}

	return json.Marshal(sfm)
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
