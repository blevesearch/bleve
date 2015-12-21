//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package upside_down

import (
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
)

func (udc *UpsideDownCouch) Analyze(d *document.Document) *index.AnalysisResult {
	rv := &index.AnalysisResult{
		DocID: d.ID,
		Rows:  make([]index.IndexRow, 0, 100),
	}

	// track our back index entries
	backIndexTermEntries := make([]*BackIndexTermEntry, 0)
	backIndexStoredEntries := make([]*BackIndexStoreEntry, 0)

	// information we collate as we merge fields with same name
	fieldTermFreqs := make(map[uint16]analysis.TokenFrequencies)
	fieldLengths := make(map[uint16]int)
	fieldIncludeTermVectors := make(map[uint16]bool)
	fieldNames := make(map[uint16]string)

	// walk all the fields, record stored fields now
	// place information about indexed fields into map
	// this collates information across fields with
	// same names (arrays)
	for _, field := range d.Fields {
		fieldIndex, newFieldRow := udc.fieldIndexOrNewRow(field.Name())
		if newFieldRow != nil {
			rv.Rows = append(rv.Rows, newFieldRow)
		}
		fieldNames[fieldIndex] = field.Name()

		if field.Options().IsIndexed() {
			fieldLength, tokenFreqs := field.Analyze()
			existingFreqs := fieldTermFreqs[fieldIndex]
			if existingFreqs == nil {
				fieldTermFreqs[fieldIndex] = tokenFreqs
			} else {
				existingFreqs.MergeAll(field.Name(), tokenFreqs)
				fieldTermFreqs[fieldIndex] = existingFreqs
			}
			fieldLengths[fieldIndex] += fieldLength
			fieldIncludeTermVectors[fieldIndex] = field.Options().IncludeTermVectors()
		}

		if field.Options().IsStored() {
			storeRows, indexBackIndexStoreEntries := udc.storeField(d.ID, field, fieldIndex)
			rv.Rows = append(rv.Rows, storeRows...)
			backIndexStoredEntries = append(backIndexStoredEntries, indexBackIndexStoreEntries...)
		}

	}

	// walk through the collated information and proccess
	// once for each indexed field (unique name)
	for fieldIndex, tokenFreqs := range fieldTermFreqs {
		fieldLength := fieldLengths[fieldIndex]
		includeTermVectors := fieldIncludeTermVectors[fieldIndex]

		// see if any of the composite fields need this
		for _, compositeField := range d.CompositeFields {
			compositeField.Compose(fieldNames[fieldIndex], fieldLength, tokenFreqs)
		}

		// encode this field
		indexRows, indexBackIndexTermEntries := udc.indexField(d.ID, includeTermVectors, fieldIndex, fieldLength, tokenFreqs)
		rv.Rows = append(rv.Rows, indexRows...)
		backIndexTermEntries = append(backIndexTermEntries, indexBackIndexTermEntries...)
	}

	// now index the composite fields
	for _, compositeField := range d.CompositeFields {
		fieldIndex, newFieldRow := udc.fieldIndexOrNewRow(compositeField.Name())
		if newFieldRow != nil {
			rv.Rows = append(rv.Rows, newFieldRow)
		}
		if compositeField.Options().IsIndexed() {
			fieldLength, tokenFreqs := compositeField.Analyze()
			// encode this field
			indexRows, indexBackIndexTermEntries := udc.indexField(d.ID, compositeField.Options().IncludeTermVectors(), fieldIndex, fieldLength, tokenFreqs)
			rv.Rows = append(rv.Rows, indexRows...)
			backIndexTermEntries = append(backIndexTermEntries, indexBackIndexTermEntries...)
		}
	}

	// build the back index row
	backIndexRow := NewBackIndexRow(d.ID, backIndexTermEntries, backIndexStoredEntries)
	rv.Rows = append(rv.Rows, backIndexRow)

	return rv
}
