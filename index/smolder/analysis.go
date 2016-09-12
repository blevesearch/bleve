//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package smolder

import (
	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
)

func (udc *SmolderingCouch) Analyze(d *document.Document) *index.AnalysisResult {
	rv := &index.AnalysisResult{
		DocID: d.ID,
		Rows:  make([]index.IndexRow, 0, 100),
	}

	docNumBytes := EncodeUvarintAscending(nil, d.Number)

	// track our back index entries
	backIndexStoredEntries := make([]*BackIndexStoreEntry, 0)

	// information we collate as we merge fields with same name
	fieldTermFreqs := make(map[uint16]analysis.TokenFrequencies)
	fieldLengths := make(map[uint16]int)
	fieldIncludeTermVectors := make(map[uint16]bool)
	fieldNames := make(map[uint16]string)

	// set the value for the _id field
	idBytes := []byte(d.ID)
	fieldTermFreqs[0] = analysis.TokenFrequency(analysis.TokenStream{
		&analysis.Token{
			Term:     idBytes,
			Position: 1,
			Start:    0,
			End:      len(d.ID),
		},
	}, nil, false)
	// store the _id field as well
	f := document.NewTextField("_id", nil, []byte(idBytes))
	rv.Rows, backIndexStoredEntries = udc.storeField(docNumBytes, f, 0, rv.Rows, backIndexStoredEntries)

	analyzeField := func(field document.Field, storable bool) {
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

		if storable && field.Options().IsStored() {
			rv.Rows, backIndexStoredEntries = udc.storeField(docNumBytes, field, fieldIndex, rv.Rows, backIndexStoredEntries)
		}
	}

	// walk all the fields, record stored fields now
	// place information about indexed fields into map
	// this collates information across fields with
	// same names (arrays)
	for _, field := range d.Fields {
		analyzeField(field, true)
	}

	if len(d.CompositeFields) > 0 {
		for fieldIndex, tokenFreqs := range fieldTermFreqs {
			if fieldIndex == 0 {
				// dont add id to any composite field
				continue
			}
			// see if any of the composite fields need this
			for _, compositeField := range d.CompositeFields {
				compositeField.Compose(fieldNames[fieldIndex], fieldLengths[fieldIndex], tokenFreqs)
			}
		}

		for _, compositeField := range d.CompositeFields {
			analyzeField(compositeField, false)
		}
	}

	rowsCapNeeded := len(rv.Rows) + 1
	for _, tokenFreqs := range fieldTermFreqs {
		rowsCapNeeded += len(tokenFreqs)
	}

	rv.Rows = append(make([]index.IndexRow, 0, rowsCapNeeded), rv.Rows...)

	backIndexTermsEntries := make([]*BackIndexTermsEntry, 0, len(fieldTermFreqs))

	// walk through the collated information and process
	// once for each indexed field (unique name)
	for fieldIndex, tokenFreqs := range fieldTermFreqs {
		fieldLength := fieldLengths[fieldIndex]
		includeTermVectors := fieldIncludeTermVectors[fieldIndex]

		// encode this field
		rv.Rows, backIndexTermsEntries = udc.indexField(docNumBytes, includeTermVectors, fieldIndex, fieldLength, tokenFreqs, rv.Rows, backIndexTermsEntries)
	}

	// build the back index row
	backIndexRow := NewBackIndexRow(docNumBytes, backIndexTermsEntries, backIndexStoredEntries)
	rv.Rows = append(rv.Rows, backIndexRow)

	return rv
}
