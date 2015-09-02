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

	for _, field := range d.Fields {
		fieldIndex, newFieldRow := udc.fieldIndexOrNewRow(field.Name())
		if newFieldRow != nil {
			rv.Rows = append(rv.Rows, newFieldRow)
		}

		if field.Options().IsIndexed() {

			fieldLength, tokenFreqs := field.Analyze()

			// see if any of the composite fields need this
			for _, compositeField := range d.CompositeFields {
				compositeField.Compose(field.Name(), fieldLength, tokenFreqs)
			}

			// encode this field
			indexRows, indexBackIndexTermEntries := udc.indexField(d.ID, field, fieldIndex, fieldLength, tokenFreqs)
			rv.Rows = append(rv.Rows, indexRows...)
			backIndexTermEntries = append(backIndexTermEntries, indexBackIndexTermEntries...)
		}

		if field.Options().IsStored() {
			storeRows, indexBackIndexStoreEntries := udc.storeField(d.ID, field, fieldIndex)
			rv.Rows = append(rv.Rows, storeRows...)
			backIndexStoredEntries = append(backIndexStoredEntries, indexBackIndexStoreEntries...)
		}

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
			indexRows, indexBackIndexTermEntries := udc.indexField(d.ID, compositeField, fieldIndex, fieldLength, tokenFreqs)
			rv.Rows = append(rv.Rows, indexRows...)
			backIndexTermEntries = append(backIndexTermEntries, indexBackIndexTermEntries...)
		}
	}

	// build the back index row
	backIndexRow := NewBackIndexRow(d.ID, backIndexTermEntries, backIndexStoredEntries)
	rv.Rows = append(rv.Rows, backIndexRow)

	return rv
}
