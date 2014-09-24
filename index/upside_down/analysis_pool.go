//  Copyright (c) 2014 Couchbase, Inc.
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
)

type AnalysisResult struct {
	docID string
	rows  []UpsideDownCouchRow
}

type AnalysisWork struct {
	udc *UpsideDownCouch
	d   *document.Document
	rc  chan *AnalysisResult
}

type AnalysisQueue chan AnalysisWork

func NewAnalysisQueue(numWorkers int) AnalysisQueue {
	rv := make(AnalysisQueue)
	for i := 0; i < numWorkers; i++ {
		go AnalysisWorker(rv)
	}
	return rv
}

func AnalysisWorker(q AnalysisQueue) {
	// read work off the queue
	for {
		w := <-q

		rv := &AnalysisResult{
			docID: w.d.ID,
			rows:  make([]UpsideDownCouchRow, 0, 100),
		}

		// track our back index entries
		backIndexTermEntries := make([]*BackIndexTermEntry, 0)
		backIndexStoredEntries := make([]*BackIndexStoreEntry, 0)

		for _, field := range w.d.Fields {
			fieldIndex, newFieldRow := w.udc.fieldIndexCache.FieldIndex(field.Name())
			if newFieldRow != nil {
				rv.rows = append(rv.rows, newFieldRow)
			}

			if field.Options().IsIndexed() {

				fieldLength, tokenFreqs := field.Analyze()

				// see if any of the composite fields need this
				for _, compositeField := range w.d.CompositeFields {
					compositeField.Compose(field.Name(), fieldLength, tokenFreqs)
				}

				// encode this field
				indexRows, indexBackIndexTermEntries := w.udc.indexField(w.d.ID, field, fieldIndex, fieldLength, tokenFreqs)
				rv.rows = append(rv.rows, indexRows...)
				backIndexTermEntries = append(backIndexTermEntries, indexBackIndexTermEntries...)
			}

			if field.Options().IsStored() {
				storeRows, indexBackIndexStoreEntries := w.udc.storeField(w.d.ID, field, fieldIndex)
				rv.rows = append(rv.rows, storeRows...)
				backIndexStoredEntries = append(backIndexStoredEntries, indexBackIndexStoreEntries...)
			}

		}

		// now index the composite fields
		for _, compositeField := range w.d.CompositeFields {
			fieldIndex, newFieldRow := w.udc.fieldIndexCache.FieldIndex(compositeField.Name())
			if newFieldRow != nil {
				rv.rows = append(rv.rows, newFieldRow)
			}
			if compositeField.Options().IsIndexed() {
				fieldLength, tokenFreqs := compositeField.Analyze()
				// encode this field
				indexRows, indexBackIndexTermEntries := w.udc.indexField(w.d.ID, compositeField, fieldIndex, fieldLength, tokenFreqs)
				rv.rows = append(rv.rows, indexRows...)
				backIndexTermEntries = append(backIndexTermEntries, indexBackIndexTermEntries...)
			}
		}

		// build the back index row
		backIndexRow := NewBackIndexRow(w.d.ID, backIndexTermEntries, backIndexStoredEntries)
		rv.rows = append(rv.rows, backIndexRow)

		w.rc <- rv
	}
}
