//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package search

import "strings"

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
