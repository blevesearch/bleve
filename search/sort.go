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
	"strings"
)

type SearchSort interface {
	Compare(a, b *DocumentMatch) int

	RequiresDocID() bool
	RequiresScoring() bool
	RequiresStoredFields() []string
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
	return &SortStoredField{
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

func (so SortOrder) RequiredStoredFields() []string {
	var rv []string
	for _, soi := range so {
		rv = append(rv, soi.RequiresStoredFields()...)
	}
	return rv
}

// SortStoredField will sort results by the value of a stored field
type SortStoredField struct {
	Field      string
	Descending bool
}

// Compare orders DocumentMatch instances by stored field values
func (s *SortStoredField) Compare(i, j *DocumentMatch) int {
	return i.Document.CompareFieldsNamed(j.Document, s.Field, s.Descending)
}

// RequiresDocID says this SearchSort does not require the DocID be loaded
func (s *SortStoredField) RequiresDocID() bool { return false }

// RequiresScoring says this SearchStore does not require scoring
func (s *SortStoredField) RequiresScoring() bool { return false }

// RequiresStoredFields says this SearchStore requires the specified stored field
func (s *SortStoredField) RequiresStoredFields() []string { return []string{s.Field} }

func (s *SortStoredField) MarshalJSON() ([]byte, error) {
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

// RequiresStoredFields says this SearchStore does not require any stored fields
func (s *SortDocID) RequiresStoredFields() []string { return nil }

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

// RequiresStoredFields says this SearchStore does not require any store fields
func (s *SortScore) RequiresStoredFields() []string { return nil }

func (s *SortScore) MarshalJSON() ([]byte, error) {
	if s.Descending {
		return json.Marshal("-_score")
	}
	return json.Marshal("_score")
}
