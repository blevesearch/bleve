//  Copyright (c) 2026 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package searcher

import (
	"time"

	"github.com/blevesearch/bleve/v2/numeric"
	"github.com/blevesearch/bleve/v2/search"
	index "github.com/blevesearch/bleve_index_api"
)

// loadDocValuesOnHit uses the supplied DocValueReader to visit doc values
// for the given hit and populate hit.Fields. It also resolves hit.ID if empty.
// It is a no-op when dvReader is nil.
//
// fieldTypes maps field name → mapping type (e.g. "datetime", "number").
// When provided, datetime fields are decoded as nanosecond int64s (cast to
// float64) rather than using the IEEE 754 bit reinterpretation used for
// numeric fields. When nil, all prefix-coded values use the numeric path.
func loadDocValuesOnHit(hit *search.DocumentMatch, dvReader index.DocValueReader,
	r index.IndexReader) error {
	return loadDocValuesOnHitWithTypes(hit, dvReader, r, nil)
}

func loadDocValuesOnHitWithTypes(hit *search.DocumentMatch, dvReader index.DocValueReader,
	r index.IndexReader, fieldTypes map[string]string) error {
	// Always resolve external ID so the callback can read hit.ID.
	if hit.ID == "" && r != nil {
		extID, err := r.ExternalID(hit.IndexInternalID)
		if err != nil {
			return err
		}
		hit.ID = extID
	}

	if dvReader == nil {
		return nil
	}

	err := dvReader.VisitDocValues(hit.IndexInternalID, func(field string, term []byte) {
		value := decodeDocValueTerm(term, fieldTypes[field])
		if value != nil {
			hit.AddFieldValue(field, value)
		}
	})

	return err
}

// decodeDocValueTerm converts raw doc value bytes into a typed Go value.
// Numeric fields are prefix-coded int64s (only shift-0 terms carry values).
// Boolean fields are stored as "T" or "F".
// Everything else (text/keyword) is returned as a string.
//
// fieldType is the mapping type string for the field (e.g. "datetime",
// "number"). When fieldType is "datetime", the prefix-coded int64 is
// treated as raw nanoseconds (time.UnixNano()) and cast directly to float64.
// For numeric fields the int64 is decoded via Int64ToFloat64 (IEEE 754 bit
// reinterpretation).
func decodeDocValueTerm(term []byte, fieldType string) interface{} {
	if len(term) == 0 {
		return nil
	}

	// Check if it's a prefix-coded numeric term.
	if valid, shift := numeric.ValidPrefixCodedTermBytes(term); valid {
		// Only shift-0 terms carry the actual value.
		if shift != 0 {
			return nil
		}
		i64, err := numeric.PrefixCoded(term).Int64()
		if err != nil {
			return nil
		}
		if fieldType == "datetime" {
			// Datetime doc values store time.UnixNano() directly as int64.
			// Convert back to a formatted string so callbacks (including
			// JS UDFs) receive a human-readable date like "2022-03-10T00:00:00Z".
			return time.Unix(0, i64).UTC().Format(time.RFC3339Nano)
		}
		// Numeric float64 fields use Float64ToInt64 bit manipulation encoding.
		return numeric.Int64ToFloat64(i64)
	}

	// Boolean fields are stored as "T" or "F".
	if len(term) == 1 {
		if term[0] == 'T' {
			return true
		}
		if term[0] == 'F' {
			return false
		}
	}

	// Default: text/keyword — return as string.
	return string(term)
}
