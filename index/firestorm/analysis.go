//  Copyright (c) 2015 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package firestorm

import (
	"math"

	"github.com/blevesearch/bleve/analysis"
	"github.com/blevesearch/bleve/document"
	"github.com/blevesearch/bleve/index"
)

func (f *Firestorm) Analyze(d *document.Document) *index.AnalysisResult {

	rv := &index.AnalysisResult{
		DocID: d.ID,
		Rows:  make([]index.IndexRow, 0, 100),
	}

	for _, field := range d.Fields {
		fieldIndex, newFieldRow := f.fieldIndexOrNewRow(field.Name())
		if newFieldRow != nil {
			rv.Rows = append(rv.Rows, newFieldRow)
		}

		// add the _id row
		rv.Rows = append(rv.Rows, NewTermFreqRow(0, nil, []byte(d.ID), d.Number, 0, 0, nil))

		if field.Options().IsIndexed() {

			fieldLength, tokenFreqs := field.Analyze()

			// see if any of the composite fields need this
			for _, compositeField := range d.CompositeFields {
				compositeField.Compose(field.Name(), fieldLength, tokenFreqs)
			}

			// encode this field
			indexRows := f.indexField(d.ID, d.Number, field, fieldIndex, fieldLength, tokenFreqs)
			rv.Rows = append(rv.Rows, indexRows...)
		}

		if field.Options().IsStored() {
			storeRow := f.storeField(d.ID, d.Number, field, fieldIndex)
			rv.Rows = append(rv.Rows, storeRow)
		}

	}

	// now index the composite fields
	for _, compositeField := range d.CompositeFields {
		fieldIndex, newFieldRow := f.fieldIndexOrNewRow(compositeField.Name())
		if newFieldRow != nil {
			rv.Rows = append(rv.Rows, newFieldRow)
		}
		if compositeField.Options().IsIndexed() {
			fieldLength, tokenFreqs := compositeField.Analyze()
			// encode this field
			indexRows := f.indexField(d.ID, d.Number, compositeField, fieldIndex, fieldLength, tokenFreqs)
			rv.Rows = append(rv.Rows, indexRows...)
		}
	}

	return rv
}

func (f *Firestorm) indexField(docID string, docNum uint64, field document.Field, fieldIndex uint16, fieldLength int, tokenFreqs analysis.TokenFrequencies) []index.IndexRow {

	rows := make([]index.IndexRow, 0, 100)
	fieldNorm := float32(1.0 / math.Sqrt(float64(fieldLength)))

	for _, tf := range tokenFreqs {
		var termFreqRow *TermFreqRow
		if field.Options().IncludeTermVectors() {
			tv, newFieldRows := f.termVectorsFromTokenFreq(fieldIndex, tf)
			rows = append(rows, newFieldRows...)
			termFreqRow = NewTermFreqRow(fieldIndex, tf.Term, []byte(docID), docNum, uint64(tf.Frequency()), fieldNorm, tv)
		} else {
			termFreqRow = NewTermFreqRow(fieldIndex, tf.Term, []byte(docID), docNum, uint64(tf.Frequency()), fieldNorm, nil)
		}

		rows = append(rows, termFreqRow)
	}

	return rows
}

func (f *Firestorm) termVectorsFromTokenFreq(field uint16, tf *analysis.TokenFreq) ([]*TermVector, []index.IndexRow) {
	rv := make([]*TermVector, len(tf.Locations))
	newFieldRows := make([]index.IndexRow, 0)

	for i, l := range tf.Locations {
		var newFieldRow *FieldRow
		fieldIndex := field
		if l.Field != "" {
			// lookup correct field
			fieldIndex, newFieldRow = f.fieldIndexOrNewRow(l.Field)
			if newFieldRow != nil {
				newFieldRows = append(newFieldRows, newFieldRow)
			}
		}
		tv := NewTermVector(fieldIndex, uint64(l.Position), uint64(l.Start), uint64(l.End), l.ArrayPositions)
		rv[i] = tv
	}

	return rv, newFieldRows
}

func (f *Firestorm) storeField(docID string, docNum uint64, field document.Field, fieldIndex uint16) index.IndexRow {
	fieldValue := make([]byte, 1+len(field.Value()))
	fieldValue[0] = encodeFieldType(field)
	copy(fieldValue[1:], field.Value())
	storedRow := NewStoredRow([]byte(docID), docNum, fieldIndex, field.ArrayPositions(), fieldValue)
	return storedRow
}

func encodeFieldType(f document.Field) byte {
	fieldType := byte('x')
	switch f.(type) {
	case *document.TextField:
		fieldType = 't'
	case *document.NumericField:
		fieldType = 'n'
	case *document.DateTimeField:
		fieldType = 'd'
	case *document.CompositeField:
		fieldType = 'c'
	}
	return fieldType
}
