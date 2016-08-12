//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package document

import (
	"github.com/blevesearch/bleve/analysis"
)

type Field interface {
	// Name returns the path of the field from the root DocumentMapping.
	// A root field path is "field", a subdocument field is "parent.field".
	Name() string
	// ArrayPositions returns the intermediate document and field indices
	// required to resolve the field value in the document. For example, if the
	// field path is "doc1.doc2.field" where doc1 and doc2 are slices or
	// arrays, ArrayPositions returns 2 indices used to resolve "doc2" value in
	// "doc1", then "field" in "doc2".
	ArrayPositions() []uint64
	Options() IndexingOptions
	Analyze() (int, analysis.TokenFrequencies)
	Value() []byte

	// NumPlainTextBytes should return the number of plain text bytes
	// that this field represents - this is a common metric for tracking
	// the rate of indexing
	NumPlainTextBytes() uint64
}

// CompareFieldValues provides ordering amongst stored field values
// when trying compare field values of different types,
// we impose the following order:
// - nil (missing field)
// - boolean
// - number
// - text
// - date
func CompareFieldValues(i, j Field, descending bool) int {

	lower := func() int {
		if descending {
			return 1
		}
		return -1
	}

	higher := func() int {
		if descending {
			return -1
		}
		return 1
	}

	switch i := i.(type) {
	case nil:
		switch j.(type) {
		case nil:
			return 0
		default:
			return lower()
		}
	case *BooleanField:
		switch j := j.(type) {
		case nil:
			return higher()
		case *BooleanField:
			return i.Compare(j, descending)
		default:
			return lower()
		}
	case *NumericField:
		switch j := j.(type) {
		case nil:
			return higher()
		case *BooleanField:
			return higher()
		case *NumericField:
			return i.Compare(j, descending)
		default:
			return lower()
		}
	case *TextField:
		switch j := j.(type) {
		case nil:
			return higher()
		case *BooleanField:
			return higher()
		case *NumericField:
			return higher()
		case *TextField:
			return i.Compare(j, descending)
		default:
			return lower()
		}
	case *DateTimeField:
		switch j := j.(type) {
		case nil:
			return higher()
		case *BooleanField:
			return higher()
		case *NumericField:
			return higher()
		case *TextField:
			return higher()
		case *DateTimeField:
			return i.Compare(j, descending)
		}
	}

	return 0
}
